use crate::taskset::{
    AccountState, Assignment, ReviewResult, Reward, RewardInner, SolutionHash, TaskHash,
    TaskReviewState, TaskSet,
};
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::LookupMap;
use near_sdk::json_types::{U128, U64};
use near_sdk::serde::{Deserialize, Serialize};
use near_sdk::{env, near_bindgen, AccountId, Balance, PanicOnDefault, Promise};
use std::convert::TryInto;

pub type WrappedBalance = U128;

mod datastructures;
mod taskset;

#[global_allocator]
static ALLOC: near_sdk::wee_alloc::WeeAlloc = near_sdk::wee_alloc::WeeAlloc::INIT;

#[derive(BorshDeserialize, BorshSerialize, Default)]
pub struct AccountStats {
    balance: Balance,
    successful: u32,
    failed: u32,
}

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
#[serde(crate = "near_sdk::serde")]
pub struct WrappedAccountStats {
    balance: WrappedBalance,
    successful: u32,
    failed: u32,
}

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
#[serde(crate = "near_sdk::serde")]
pub enum WrappedAccountState {
    NonExistent,
    Idle,
    WaitsForAssignment {
        time_left: U64,
        bid: WrappedBalance,
    },
    HasAssignment {
        assignment: Assignment,
        time_passed: U64,
        bid: WrappedBalance,
    },
}

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize)]
#[serde(crate = "near_sdk::serde")]
pub struct WrappedTaskSetState {
    pub next_price: WrappedBalance,
    pub wait_time: U64,
    pub num_unassigned: U64,
    pub num_reviews: U64,
}

pub struct PersistentTaskSet {
    collection: LookupMap<u32, TaskSet>,
    key: u32,
    taskset: TaskSet,
    is_view: bool,
}

impl PersistentTaskSet {
    fn new(key: u32, is_view: bool) -> Self {
        let collection = LookupMap::new(b"p".to_vec());
        let taskset = collection.get(&key).expect("No such taskset");
        Self {
            collection,
            key,
            taskset,
            is_view,
        }
    }
}

impl Drop for PersistentTaskSet {
    fn drop(&mut self) {
        if !self.is_view {
            self.collection.insert(&self.key, &self.taskset);
        }
    }
}

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize, PanicOnDefault)]
pub struct NearCrowdContract {
    admin_id: AccountId,
}

#[near_bindgen]
impl NearCrowdContract {
    #[init]
    pub fn new() -> Self {
        assert!(!env::state_exists(), "Already initialized");
        Self {
            admin_id: env::predecessor_account_id(),
        }
    }

    pub fn claim_reward() {
        let stats = Self::account_stats_map()
            .remove(&env::predecessor_account_id())
            .expect("There's no record for the account");
        assert!(
            stats.successful * 100 >= (stats.successful + stats.failed) * 90,
            "Can't claim the reward, the success rate is too low"
        );
        assert!(
            stats.successful + stats.failed >= 20,
            "Can't claim the reward, not enough completed tasks"
        );
        Promise::new(env::predecessor_account_id()).transfer(stats.balance);
    }

    pub fn add_tasks(&self, task_ordinal: u32, hashes: Vec<TaskHash>) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .add_tasks(hashes);
    }

    pub fn add_taskset(
        &mut self,
        ordinal: u32,
        max_price: WrappedBalance,
        min_price: WrappedBalance,
        mtasks_per_second: U64,
    ) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        let taskset = TaskSet::new(
            &[&[b't'], ordinal.to_le_bytes().as_ref()].concat(),
            max_price.into(),
            min_price.into(),
            mtasks_per_second.into(),
        );

        let mut collection: LookupMap<u32, TaskSet> = LookupMap::new(b"p".to_vec());
        collection.insert(&ordinal, &taskset);
    }

    pub fn update_taskset_prices(
        &mut self,
        task_ordinal: u32,
        new_min_price: WrappedBalance,
        new_max_price: WrappedBalance,
    ) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .update_prices(new_min_price.into(), new_max_price.into());
    }

    pub fn update_mtasks_per_second(&mut self, task_ordinal: u32, mtasks_per_second: U64) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .update_mtasks_per_second(mtasks_per_second.into());
    }

    pub fn whitelist_account(&mut self, account_id: &AccountId) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        Self::account_task_ordinals_map().insert(&account_id, &None);
    }

    pub fn ban_account(&mut self, account_id: &AccountId) {
        assert_eq!(self.admin_id, env::predecessor_account_id());
        let maybe_stats = Self::account_stats_map().remove(account_id);
        if let Some(stats) = maybe_stats {
            let total_tasks = stats.successful + stats.failed;
            assert!(total_tasks >= 20);

            if total_tasks < 50 {
                assert!(stats.successful * 100 <= total_tasks * 70);
            } else {
                assert!(stats.successful * 100 <= total_tasks * 85);
            }
        }

        let current_task = Self::account_task_ordinals_map()
            .remove(account_id)
            .unwrap_or_default();
        if let Some(task_ord) = current_task {
            PersistentTaskSet::new(task_ord, false)
                .taskset
                .unregister_account(account_id, true);
        }

        Self::approved_solution_hashes_map().remove(account_id);
    }

    pub fn change_taskset(new_task_ord: u32) {
        if let Some(old_task_ord) = Self::account_task_ordinals_map()
            .insert(&env::predecessor_account_id(), &Some(new_task_ord))
            .expect("Unknown account")
        {
            PersistentTaskSet::new(old_task_ord, false)
                .taskset
                .unregister_account(&env::predecessor_account_id(), false);
        }
        PersistentTaskSet::new(new_task_ord, false)
            .taskset
            .register_account(&env::predecessor_account_id());
        Self::approved_solution_hashes_map().remove(&env::predecessor_account_id());
    }

    pub fn is_account_whitelisted(&self, account_id: &AccountId) -> bool {
        Self::account_task_ordinals_map().contains_key(account_id)
    }

    pub fn get_current_taskset(&self, account_id: &AccountId) -> Option<u32> {
        Self::account_task_ordinals_map()
            .get(account_id)
            .unwrap_or_default()
    }

    pub fn get_current_assignment(
        &self,
        task_ordinal: u32,
        account_id: &AccountId,
    ) -> Option<Assignment> {
        PersistentTaskSet::new(task_ordinal, true)
            .taskset
            .get_current_assignment(account_id)
    }

    pub fn get_account_stats(&self, account_id: &AccountId) -> WrappedAccountStats {
        let AccountStats {
            successful,
            failed,
            balance,
        } = Self::account_stats_map()
            .get(account_id)
            .unwrap_or_default();
        WrappedAccountStats {
            successful,
            failed,
            balance: balance.into(),
        }
    }

    pub fn get_account_state(
        &self,
        task_ordinal: u32,
        account_id: &AccountId,
    ) -> WrappedAccountState {
        match PersistentTaskSet::new(task_ordinal, true)
            .taskset
            .get_account_state(account_id)
        {
            AccountState::NonExistent => WrappedAccountState::NonExistent,
            AccountState::Idle => WrappedAccountState::Idle,
            AccountState::WaitsForAssignment { until, bid } => {
                WrappedAccountState::WaitsForAssignment {
                    time_left: until.saturating_sub(env::block_timestamp()).into(),
                    bid: bid.into(),
                }
            }
            AccountState::HasAssignment {
                assignment,
                since,
                bid,
            } => WrappedAccountState::HasAssignment {
                assignment,
                time_passed: env::block_timestamp().saturating_sub(since).into(),
                bid: bid.into(),
            },
        }
    }

    pub fn get_taskset_state(&self, task_ordinal: u32) -> WrappedTaskSetState {
        let state = PersistentTaskSet::new(task_ordinal, true)
            .taskset
            .get_state(env::block_timestamp());
        WrappedTaskSetState {
            next_price: state.next_price.into(),
            wait_time: state.wait_time.into(),
            num_unassigned: state.num_unassigned.into(),
            num_reviews: state.num_reviews.into(),
        }
    }

    pub fn get_task_review_state(
        &self,
        task_ordinal: u32,
        task_hash: &TaskHash,
    ) -> TaskReviewState {
        PersistentTaskSet::new(task_ordinal, true)
            .taskset
            .get_task_review_state(task_hash)
    }

    pub fn return_assignment(task_ordinal: u32) {
        PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .return_assignment(&env::predecessor_account_id(), env::block_timestamp());
    }

    pub fn apply_for_assignment(task_ordinal: u32) {
        let mut ptaskset = PersistentTaskSet::new(task_ordinal, false);
        ptaskset.taskset.apply_for_assignment(
            &env::predecessor_account_id(),
            env::block_timestamp(),
            ptaskset.taskset.get_bid(env::block_timestamp()),
        );
    }

    pub fn claim_assignment(task_ordinal: u32, bid: WrappedBalance) -> bool {
        if PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .claim_assignment(
                &env::predecessor_account_id(),
                env::block_timestamp(),
                [env::random_seed()[0], env::random_seed()[1]],
                bid.into(),
            )
        {
            Self::account_task_ordinals_map().insert(&env::predecessor_account_id(), &None);
            true
        } else {
            false
        }
    }

    pub fn approve_solution(&mut self, account_id: &AccountId, solution_hash: &SolutionHash) {
        assert_eq!(env::predecessor_account_id(), self.admin_id);
        Self::approved_solution_hashes_map().insert(account_id, solution_hash);
    }

    pub fn get_approved_solution(&self, account_id: &AccountId) -> Option<SolutionHash> {
        Self::approved_solution_hashes_map().get(account_id)
    }

    pub fn submit_approved_solution(task_ordinal: u32, solution_hash: SolutionHash) {
        assert_eq!(
            solution_hash,
            Self::approved_solution_hashes_map()
                .remove(&env::predecessor_account_id())
                .expect("There's no approved solution for this account"),
        );
        let mut ptaskset = PersistentTaskSet::new(task_ordinal, false);
        ptaskset.taskset.submit_solution(
            env::predecessor_account_id(),
            solution_hash,
            true,
            env::block_timestamp(),
            ptaskset.taskset.get_bid(env::block_timestamp()),
        )
    }

    pub fn submit_solution(task_ordinal: u32, solution_data: Vec<u8>) {
        let solution_hash = SolutionHash(env::sha256(solution_data.as_ref()).try_into().unwrap());
        let mut ptaskset = PersistentTaskSet::new(task_ordinal, false);
        ptaskset.taskset.submit_solution(
            env::predecessor_account_id(),
            solution_hash,
            false,
            env::block_timestamp(),
            ptaskset.taskset.get_bid(env::block_timestamp()),
        );
        Self::approved_solution_hashes_map().remove(&env::predecessor_account_id());
    }

    #[allow(unused)]
    pub fn submit_review(task_ordinal: u32, approve: bool, rejection_reason: String) {
        let mut ptaskset = PersistentTaskSet::new(task_ordinal, false);
        ptaskset.taskset.submit_review(
            env::predecessor_account_id(),
            if approve {
                ReviewResult::Accept
            } else {
                ReviewResult::Reject
            },
            true,
            env::block_timestamp(),
            ptaskset.taskset.get_bid(env::block_timestamp()),
        );
    }

    pub fn honeypot_partial_credit(task_ordinal: u32, task_hash: &TaskHash) {
        let (r1, r2) = PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .honeypot_partial_credit(env::predecessor_account_id(), task_hash);
        Self::process_reward(r1);
        Self::process_reward(r2);
    }

    pub fn finalize_task(
        task_ordinal: u32,
        is_honeypot: bool,
        honeypot_preimage: Vec<u8>,
        task_preimage: Vec<u8>,
    ) {
        let (r1, r2) = PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .finalize_task(is_honeypot, honeypot_preimage, task_preimage);
        Self::process_reward(r1);
        Self::process_reward(r2);
    }

    pub fn finalize_challenged_task(task_ordinal: u32, task_hash: &TaskHash) {
        for r in PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .finalize_challenged_task(task_hash)
        {
            Self::process_reward(r);
        }
    }

    pub fn challenge(
        task_ordinal: u32,
        is_honeypot: bool,
        honeypot_preimage: Vec<u8>,
        task_preimage: Vec<u8>,
    ) {
        PersistentTaskSet::new(task_ordinal, false)
            .taskset
            .challenge(
                is_honeypot,
                honeypot_preimage,
                task_preimage,
                env::block_timestamp(),
            );
    }
}

impl NearCrowdContract {
    fn approved_solution_hashes_map() -> LookupMap<AccountId, SolutionHash> {
        LookupMap::new(b"a".to_vec())
    }

    fn account_stats_map() -> LookupMap<AccountId, AccountStats> {
        LookupMap::new(b"s".to_vec())
    }

    fn account_task_ordinals_map() -> LookupMap<AccountId, Option<u32>> {
        LookupMap::new(b"o".to_vec())
    }

    fn process_reward(reward: Reward) {
        let mut account_stats = Self::account_stats_map();
        let mut stats = account_stats.get(&reward.account_id).unwrap_or_default();
        match reward.inner {
            RewardInner::Successful { reward: amount } => {
                stats.successful += 1;
                stats.balance += amount;
            }
            RewardInner::PartialHoneyPot { reward_base } => {
                stats.successful += 2;
                stats.failed += 3;
                stats.balance += reward_base / 2;
            }
            RewardInner::Failed { is_honeypot } => {
                stats.failed += if is_honeypot { 5 } else { 1 };
            }
        };
        account_stats.insert(&reward.account_id, &stats);
    }
}
