use crate::datastructures::{Queue, TasksFaucet};
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::{LookupMap, Vector};
use near_sdk::serde::{Deserialize, Serialize};
use near_sdk::{env, AccountId, Balance};
use std::convert::TryInto;

// After the task is assigned, it can't be returnes for this long
const TIMEOUT_TO_ALLOW_RETURN: u64 = 5 * 60 * 1_000_000_000;

// Time for the server to submit the solution before the client can submit it on its own
const TIMEOUT_TO_REVEAL_SOLUTION: u64 = 2 * 60 * 1_000_000_000;

// Time between a review is submitted and a challenge can be initiated. Is needed to give
// the reviewer time to change the verdict for partial credit
const TIMEOUT_BEFORE_CHALLENGE: u64 = 5 * 60 * 1_000_000_000;

// Minimum number of tasks pending review for one to be assigned
const MIN_PENDING_REVIEW_TO_ASSIGN: u64 = 12;

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(crate = "near_sdk::serde")]
pub struct TaskHash(pub [u8; 32]);

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(crate = "near_sdk::serde")]
pub struct SolutionHash(pub [u8; 32]);

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(crate = "near_sdk::serde")]
pub struct Assignment {
    task_hash: TaskHash,
    // Ordinal 0 is the initial solution. Positive integers are reviews.
    ordinal: u8,
}

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(crate = "near_sdk::serde")]
pub enum AccountState {
    NonExistent,
    Idle,
    WaitsForAssignment {
        until: u64,
        bid: Balance,
    },
    HasAssignment {
        assignment: Assignment,
        since: u64,
        bid: Balance,
    },
}

#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug)]
pub struct TaskSetState {
    pub next_price: Balance,
    pub wait_time: u64,
    pub num_unassigned: u64,
    pub num_reviews: u64,
}

#[derive(BorshDeserialize, BorshSerialize, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(crate = "near_sdk::serde")]
pub enum TaskReviewState {
    None,
    NotReviewed,
    PendingFinalization(bool),
    Challenged,
    ChallengeFinalization,
}

#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug, Clone)]
pub enum ReviewResult {
    Accept,
    Reject,
}

#[derive(BorshDeserialize, BorshSerialize, Clone)]
pub struct Review {
    reviewed_by: AccountId,
    review_result: ReviewResult,
    when: u64,
    bid: Balance,
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct Solution {
    performed_by: AccountId,
    solution_hash: SolutionHash,
    bid: Balance,
}

#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug)]
pub enum RewardInner {
    Successful { reward: Balance },
    PartialHoneyPot { reward_base: Balance },
    Failed { is_honeypot: bool },
}

#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug)]
#[must_use]
pub struct Reward {
    pub account_id: AccountId,
    pub inner: RewardInner,
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct TaskSet {
    // The datastructure to track timing of assigning tasks
    faucet: TasksFaucet,
    // Hashes of unassigned tasks
    unassigned_tasks: Queue<TaskHash>,
    // Hashes of tasks that are pending review
    pending_review: Vector<Assignment>,
    // A mapping from task hash and ordinal to who performed the review and their review result.
    // Ordinal 0 is present if the task was challenged, in which case it is Accept if the task was
    // a regular task, and Reject if it was a honeypot
    reviews: LookupMap<TaskHash, Vec<Option<Review>>>,
    solutions: LookupMap<TaskHash, Solution>,

    account_states: LookupMap<AccountId, AccountState>,

    upgradability: Option<()>,
}

impl TaskSet {
    pub fn new(id: &[u8], max_price: Balance, min_price: Balance, mtasks_per_second: u64) -> Self {
        Self {
            faucet: TasksFaucet::new(max_price, min_price, mtasks_per_second),
            unassigned_tasks: Queue::new([id, &[b'b']].concat()),
            pending_review: Vector::new([id, &[b'c']].concat()),
            reviews: LookupMap::new([id, &[b'e']].concat()),
            solutions: LookupMap::new([id, &[b'f']].concat()),
            account_states: LookupMap::new([id, &[b'g']].concat()),
            upgradability: None,
        }
    }

    pub fn get_state(&self, now: u64) -> TaskSetState {
        let next_price_and_wait_time = self.faucet.get_next_price_and_wait_time(now);
        TaskSetState {
            next_price: next_price_and_wait_time.0,
            wait_time: next_price_and_wait_time.1,
            num_unassigned: self.unassigned_tasks.len(),
            num_reviews: self.pending_review.len(),
        }
    }

    pub fn update_prices(&mut self, new_min_price: Balance, new_max_price: Balance) {
        self.faucet.update_prices(new_min_price, new_max_price);
    }

    pub fn update_mtasks_per_second(&mut self, mtasks_per_second: u64) {
        self.faucet.update_mtasks_per_second(mtasks_per_second);
    }

    pub fn add_tasks(&mut self, hashes: Vec<TaskHash>) {
        for hash in hashes {
            self.unassigned_tasks.enqueue(&hash);
        }
    }

    pub fn register_account(&mut self, account_id: &AccountId) {
        assert!(self
            .account_states
            .insert(account_id, &AccountState::Idle)
            .is_none());
    }

    pub fn unregister_account(&mut self, account_id: &AccountId, allow_return_task: bool) {
        match self
            .account_states
            .remove(account_id)
            .expect("Cannot unregister an account from a taskset: account is not registered")
        {
            AccountState::Idle
            | AccountState::NonExistent
            | AccountState::WaitsForAssignment { .. } => {}
            AccountState::HasAssignment { assignment, .. } => {
                assert!(
                    allow_return_task,
                    "Cannot change the taskset when there's an active task"
                );
                if assignment.ordinal > 0 {
                    self.pending_review.push(&assignment);
                } else {
                    self.unassigned_tasks.enqueue(&assignment.task_hash);
                }
            }
        }
    }

    pub fn get_bid(&self, now: u64) -> Balance {
        self.faucet.get_next_price_and_wait_time(now).0
    }

    pub fn get_current_assignment(&self, account_id: &AccountId) -> Option<Assignment> {
        if let Some(AccountState::HasAssignment { assignment, .. }) =
            self.account_states.get(account_id)
        {
            Some(assignment)
        } else {
            None
        }
    }

    pub fn get_account_state(&self, account_id: &AccountId) -> AccountState {
        self.account_states
            .get(account_id)
            .unwrap_or(AccountState::NonExistent)
    }

    pub fn get_task_review_state(&self, task_hash: &TaskHash) -> TaskReviewState {
        if !self.solutions.contains_key(task_hash) {
            TaskReviewState::None
        } else {
            let review = self.reviews.get(task_hash);
            if review.is_none() || !review.as_ref().unwrap()[1].is_some() {
                TaskReviewState::NotReviewed
            } else if !review.as_ref().unwrap()[0].is_some() {
                TaskReviewState::PendingFinalization(
                    review.as_ref().unwrap()[1].as_ref().unwrap().review_result
                        == ReviewResult::Accept,
                )
            } else if !(2..5).all(|ord| review.as_ref().unwrap()[ord].is_some()) {
                TaskReviewState::Challenged
            } else {
                TaskReviewState::ChallengeFinalization
            }
        }
    }

    pub fn return_assignment(&mut self, account_id: &AccountId, now: u64) {
        if let Some(AccountState::HasAssignment {
            assignment, since, ..
        }) = self.account_states.get(account_id)
        {
            if since + TIMEOUT_TO_ALLOW_RETURN <= now {
                self.account_states.insert(account_id, &AccountState::Idle);
                if assignment.ordinal > 0 {
                    self.pending_review.push(&assignment);
                } else {
                    self.unassigned_tasks.enqueue(&assignment.task_hash);
                }
            }
        }
    }

    pub fn apply_for_assignment(&mut self, account_id: &AccountId, now: u64, bid: Balance) {
        let old_state = self.apply_for_assignment_internal(account_id, now, bid);
        assert!(old_state == AccountState::Idle);
    }

    pub fn claim_assignment(
        &mut self,
        account_id: &AccountId,
        now: u64,
        rand: [u8; 2],
        bid: Balance,
    ) -> bool {
        if let Some(assignment) = self.mint_assignment(account_id, rand) {
            if let Some(AccountState::WaitsForAssignment {
                until,
                bid: old_bid,
            }) = self.account_states.insert(
                account_id,
                &AccountState::HasAssignment {
                    assignment,
                    since: now,
                    bid,
                },
            ) {
                assert_eq!(
                    old_bid, bid,
                    "The new bid doesn't match the bid used to apply for the task"
                );
                assert!(until <= now, "Attempt to claim the task too early");
            } else {
                panic!("You need to apply for an assignment first");
            }
            false
        } else {
            // No task in this taskset can be assigned to this account, remove it from the taskset
            self.account_states.remove(account_id);
            true
        }
    }

    pub fn submit_solution(
        &mut self,
        account_id: AccountId,
        solution_hash: SolutionHash,
        allow_instant: bool,
        now: u64,
        new_bid: Balance,
    ) {
        // TODO: who checks if enough time has passed?
        if let AccountState::HasAssignment {
            assignment,
            since,
            bid: old_bid,
        } = self.apply_for_assignment_internal(&account_id, now, new_bid)
        {
            let Assignment { task_hash, .. } = assignment;

            assert_eq!(assignment.ordinal, 0);
            assert!(allow_instant || since + TIMEOUT_TO_REVEAL_SOLUTION <= now);

            self.solutions.insert(
                &task_hash,
                &Solution {
                    performed_by: account_id,
                    solution_hash,
                    bid: old_bid,
                },
            );

            self.pending_review.push(&Assignment {
                task_hash,
                ordinal: 1,
            });
        }
    }

    pub fn submit_review(
        &mut self,
        account_id: AccountId,
        review_result: ReviewResult,
        allow_instant: bool,
        now: u64,
        new_bid: Balance,
    ) {
        if let AccountState::HasAssignment {
            assignment,
            since,
            bid: old_bid,
        } = self.apply_for_assignment_internal(&account_id, now, new_bid)
        {
            let Assignment { task_hash, ordinal } = assignment;

            assert!(assignment.ordinal >= 1);
            assert!(allow_instant || since + TIMEOUT_TO_REVEAL_SOLUTION <= now);

            let mut task_reviews = self
                .reviews
                .get(&task_hash)
                .unwrap_or_else(|| vec![None; 5]);
            task_reviews[ordinal as usize] = Some(Review {
                reviewed_by: account_id,
                review_result,
                when: now,
                bid: old_bid,
            });

            self.reviews.insert(&task_hash, &task_reviews);
        } else {
            panic!("Attempt to submit a review without having as assignment");
        }
    }

    // The task is immediately finalized, and the corresponding rewards must be awarded
    pub fn honeypot_partial_credit(
        &mut self,
        account_id: AccountId,
        task_hash: &TaskHash,
    ) -> (Reward, Reward) {
        let Solution {
            performed_by,
            bid: solution_bid,
            ..
        } = self.solutions.remove(task_hash).unwrap();

        let reviews = self
            .reviews
            .remove(task_hash)
            .expect("There's no review for this task");

        assert!(
            reviews[0].is_none(),
            "This task has already been challenged"
        );

        if let Some(Review {
            review_result: ReviewResult::Accept,
            reviewed_by,
            bid: review_bid,
            ..
        }) = reviews[1].clone()
        {
            assert_eq!(account_id, reviewed_by);
            (
                Reward {
                    account_id: performed_by,
                    inner: RewardInner::Successful {
                        reward: solution_bid,
                    },
                },
                Reward {
                    account_id: reviewed_by,
                    inner: RewardInner::PartialHoneyPot {
                        reward_base: review_bid,
                    },
                },
            )
        } else {
            panic!("Attempt to claim partial credit for a honeypot not previously accepted by the participant");
        }
    }

    // Panics if the task cannot be finalized
    // Returns the rewards for the performer and the reviewer
    pub fn finalize_task(
        &mut self,
        is_honeypot: bool,
        honeypot_preimage: Vec<u8>,
        task_preimage: Vec<u8>,
    ) -> (Reward, Reward) {
        let task_hash = Self::get_task_hash(is_honeypot, honeypot_preimage, task_preimage);

        let Solution {
            performed_by,
            bid: solution_bid,
            ..
        } = self
            .solutions
            .remove(&task_hash)
            .expect(format!("No solution for task hash {:X?}", task_hash.0).as_str());

        let Review {
            review_result,
            reviewed_by,
            bid: reviewer_bid,
            ..
        } = self
            .reviews
            .remove(&task_hash)
            .expect("The task is not reviewed yet")[1]
            .clone()
            .expect("The task is not reviewed yet");
        assert_eq!(is_honeypot, review_result != ReviewResult::Accept);
        assert_ne!(performed_by, reviewed_by);

        (
            Reward {
                account_id: performed_by,
                inner: RewardInner::Successful {
                    reward: solution_bid,
                },
            },
            Reward {
                account_id: reviewed_by,
                inner: RewardInner::Successful {
                    reward: reviewer_bid,
                },
            },
        )
    }

    // Returns the rewards for the performer and the reviewer
    // Panics if the task cannot be finalized
    pub fn finalize_challenged_task(&mut self, task_hash: &TaskHash) -> Vec<Reward> {
        let mut accepted: Vec<(AccountId, Balance)> = vec![];
        let mut rejected: Vec<(AccountId, Balance)> = vec![];

        let mut is_honeypot = false;

        let solution_bid = self.solutions.remove(task_hash).unwrap().bid;
        let reviews = self
            .reviews
            .remove(task_hash)
            .expect("The task isn't reviewed yet");

        for (ordinal, review) in reviews.into_iter().enumerate() {
            let Review {
                reviewed_by,
                review_result,
                bid: review_bid,
                ..
            } = review.expect("The challenge is still in progress");

            let bid = if ordinal == 0 {
                solution_bid
            } else {
                review_bid
            };

            if review_result == ReviewResult::Accept {
                accepted.push((reviewed_by, bid));
                if ordinal == 1 {
                    is_honeypot = true;
                }
            } else {
                rejected.push((reviewed_by, bid));
                // The solution was ultimately rejected, reintroduce the task
                if ordinal == 0 {
                    self.unassigned_tasks.enqueue(task_hash);
                }
            }
        }

        let (successful, failed) = if accepted.len() > rejected.len() {
            (accepted, rejected)
        } else {
            (rejected, accepted)
        };

        successful
            .into_iter()
            .map(|(account_id, bid)| Reward {
                account_id,
                inner: RewardInner::Successful { reward: bid },
            })
            .chain(failed.into_iter().map(|(account_id, _)| Reward {
                account_id,
                inner: RewardInner::Failed { is_honeypot },
            }))
            .collect()
    }

    // Panics if the task cannot be challenged
    pub fn challenge(
        &mut self,
        is_honeypot: bool,
        honeypot_preimage: Vec<u8>,
        task_preimage: Vec<u8>,
        now: u64,
    ) {
        let task_hash = Self::get_task_hash(is_honeypot, honeypot_preimage, task_preimage);

        let mut reviews = self
            .reviews
            .get(&task_hash)
            .expect("The task isn't reviewed yet");

        let Review {
            review_result,
            when,
            ..
        } = reviews[1].clone().expect("The task isn't reviewed yet");
        assert_eq!(is_honeypot, review_result == ReviewResult::Accept);
        assert!(when + TIMEOUT_BEFORE_CHALLENGE <= now);
        assert!(reviews[0].is_none(), "The task is already challenged");

        let Solution {
            performed_by: account_id,
            ..
        } = self
            .solutions
            .get(&task_hash)
            .expect("Can't find the solution that is being challenged");

        reviews[0] = Some(Review {
            reviewed_by: account_id,
            review_result: if is_honeypot {
                ReviewResult::Reject
            } else {
                ReviewResult::Accept
            },
            when: now,
            bid: 0, // for the 0th ordinal the bid from the solution is used
        });

        self.reviews.insert(&task_hash, &reviews);

        for ordinal in 2..5 {
            self.pending_review.push(&Assignment {
                task_hash: task_hash.clone(),
                ordinal,
            });
        }
    }
}

impl TaskSet {
    fn apply_for_assignment_internal(
        &mut self,
        account_id: &AccountId,
        now: u64,
        bid: Balance,
    ) -> AccountState {
        self.account_states
            .insert(
                account_id,
                &AccountState::WaitsForAssignment {
                    until: self.faucet.request_task(now),
                    bid,
                },
            )
            .unwrap_or(AccountState::NonExistent)
    }

    fn get_task_hash(
        is_honeypot: bool,
        honeypot_preimage: Vec<u8>,
        task_preimage: Vec<u8>,
    ) -> TaskHash {
        TaskHash(
            env::sha256(
                &[
                    env::sha256(
                        &[
                            if is_honeypot { [1; 1] } else { [0; 1] }.as_ref(),
                            honeypot_preimage.as_ref(),
                        ]
                        .concat(),
                    ),
                    task_preimage,
                ]
                .concat(),
            )
            .try_into()
            .unwrap(),
        )
    }

    fn mint_assignment(&mut self, account_id: &AccountId, rand: [u8; 2]) -> Option<Assignment> {
        // Make three attempts to claim an assignment. An attempt is failed if it is a review of a
        // task performed or already reviewed by the account
        for attempt in 0..3 {
            let has_enough_reviews = self.pending_review.len() >= MIN_PENDING_REVIEW_TO_ASSIGN;
            let has_any_reviews = self.pending_review.len() > 0;
            let has_unsolved = !self.unassigned_tasks.is_empty();

            return if !has_any_reviews && !has_unsolved {
                None
            } else if has_unsolved && (!has_enough_reviews || rand[0] <= 120 || attempt > 0) {
                self.unassigned_tasks.dequeue().map(|task_hash| Assignment {
                    ordinal: 0,
                    task_hash,
                })
            } else {
                let ordinal = (rand[1] as u64 + attempt) % self.pending_review.len();
                let assignment = self.pending_review.swap_remove(ordinal);

                let may_be_reviews = self.reviews.get(&assignment.task_hash);
                let solution = self.solutions.get(&assignment.task_hash);
                let already_reviewed = if let Some(reviews) = may_be_reviews {
                    reviews
                        .iter()
                        .any(|x| x.as_ref().map_or(false, |x| &x.reviewed_by == account_id))
                } else {
                    false
                };

                let solved_myself = solution.map_or(false, |x| &x.performed_by == account_id);

                if already_reviewed || solved_myself {
                    self.pending_review.push(&assignment);
                    continue;
                }

                Some(assignment)
            };
        }
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;
    use near_sdk::test_utils::test_env;

    #[derive(PartialEq, Eq)]
    enum SanityTestFailure {
        None,
        ClaimTooEarly,
        ClaimWrongBid,
        SolutionTooEarly,
        FinalizeRejectedTask,
        ChallengeAcceptedTask,
        ChallengeTooEarly,
        DoubleFinalize,
        DoubleChallenge,
        FinalizeChallengeTooEarly,
        DoubleFinalizeChallenge,
        PartialCreditForRejected,
        FinalizeAfterPartialCreditReject,
    }

    #[test]
    fn test_tasks_sanity() {
        test_tasks_sanity_internal(SanityTestFailure::None);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_claim_too_early() {
        test_tasks_sanity_internal(SanityTestFailure::ClaimTooEarly);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_claim_wrong_bid() {
        test_tasks_sanity_internal(SanityTestFailure::ClaimWrongBid);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_solution_too_early() {
        test_tasks_sanity_internal(SanityTestFailure::SolutionTooEarly);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_finalize_rejected() {
        test_tasks_sanity_internal(SanityTestFailure::FinalizeRejectedTask);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_challenge_accepted() {
        test_tasks_sanity_internal(SanityTestFailure::ChallengeAcceptedTask);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_challenge_too_early() {
        test_tasks_sanity_internal(SanityTestFailure::ChallengeTooEarly);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_double_finalize() {
        test_tasks_sanity_internal(SanityTestFailure::DoubleFinalize);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_double_challenge() {
        test_tasks_sanity_internal(SanityTestFailure::DoubleChallenge);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_finalize_challenge_too_ealry() {
        test_tasks_sanity_internal(SanityTestFailure::FinalizeChallengeTooEarly);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_double_finalize_challenge() {
        test_tasks_sanity_internal(SanityTestFailure::DoubleFinalizeChallenge);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_partial_credit_for_rejected() {
        test_tasks_sanity_internal(SanityTestFailure::PartialCreditForRejected);
    }

    #[test]
    #[should_panic]
    fn test_tasks_sanity_finalize_after_partial_credit_reject() {
        test_tasks_sanity_internal(SanityTestFailure::FinalizeAfterPartialCreditReject);
    }

    fn test_tasks_sanity_internal(fail_at: SanityTestFailure) {
        const MTASKS_PER_SECOND: u64 = 100; // 1 task every 10 seconds
        const NEARCENT: Balance = 1_000_000_000_000_000_000_000_000 / 100;

        test_env::setup();

        let tasks = vec![
            (false, vec![0u8; 8]),
            (false, vec![1; 8]),
            (true, vec![2; 8]),
            (true, vec![3; 8]),
        ]
        .into_iter()
        .map(|(honey, preimage)| {
            (
                honey,
                preimage.clone(),
                TaskSet::get_task_hash(honey, b"moo".to_vec(), preimage),
            )
        })
        .collect::<Vec<_>>();

        let accounts: Vec<AccountId> = vec![
            "test0".to_string(),
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string(),
            "test4".to_string(),
        ];

        let mut taskset = TaskSet::new(b"moo".as_ref(), 0, 0, MTASKS_PER_SECOND);

        accounts.iter().for_each(|x| taskset.register_account(x));

        // Add the first two tasks
        taskset.add_tasks(tasks.iter().take(2).map(|x| x.2.clone()).collect());

        // Apply for an assignment
        taskset.apply_for_assignment(&accounts[0], 0, 20 * NEARCENT);

        // Make sure it is instantly given
        assert_eq!(
            taskset.account_states.get(&accounts[0]).unwrap(),
            AccountState::WaitsForAssignment {
                until: 10_000_000_000,
                bid: 20 * NEARCENT
            }
        );

        // Claim the assignment
        let mut now = 0;

        // Claim a new assignment
        now += 10_000_000_000 - 1;
        if fail_at == SanityTestFailure::ClaimTooEarly {
            taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);
            return;
        }
        now += 1;

        if fail_at == SanityTestFailure::ClaimWrongBid {
            taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT + 1);
            return;
        }
        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);

        // The second account shouldn't have an assignment yet
        assert!(taskset.get_current_assignment(&accounts[1]).is_none());
        assert_eq!(
            taskset.account_states.get(&accounts[1]).unwrap(),
            AccountState::Idle
        );

        // The first should. Also run some sanity checks on it
        let assignment = taskset.get_current_assignment(&accounts[0]).unwrap();
        assert_eq!(assignment.ordinal, 0);
        assert_eq!(assignment.task_hash, tasks[0].2);
        assert_eq!(taskset.unassigned_tasks.peek_front().unwrap(), tasks[1].2);

        assert_eq!(
            taskset.account_states.get(&accounts[0]).unwrap(),
            AccountState::HasAssignment {
                since: 10_000_000_000,
                assignment: assignment,
                bid: 20 * NEARCENT,
            }
        );

        // Submit a solution
        now += TIMEOUT_TO_REVEAL_SOLUTION - 1;
        if fail_at == SanityTestFailure::SolutionTooEarly {
            taskset.submit_solution(
                accounts[0].clone(),
                SolutionHash([0; 32]),
                false,
                now,
                20 * NEARCENT,
            );
            return;
        }

        now += 1;
        taskset.submit_solution(
            accounts[0].clone(),
            SolutionHash([0; 32]),
            false,
            now,
            20 * NEARCENT,
        );

        assert_eq!(
            taskset.get_task_review_state(&tasks[0].2),
            TaskReviewState::NotReviewed
        );

        assert!(taskset.get_current_assignment(&accounts[0]).is_none());
        assert_eq!(
            taskset.account_states.get(&accounts[0]).unwrap(),
            AccountState::WaitsForAssignment {
                until: 130_000_000_000,
                bid: 20 * NEARCENT
            }
        );

        now += 10_000_000_000;

        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);

        // If `allow_instant` is true, the solution must be accepted even if not enough time has
        // passed
        taskset.submit_solution(
            accounts[0].clone(),
            SolutionHash([0; 32]),
            true,
            now,
            20 * NEARCENT,
        );

        assert_eq!(
            taskset.get_task_review_state(&tasks[1].2),
            TaskReviewState::NotReviewed
        );

        // Apply for an assignment for the second account
        taskset.apply_for_assignment(&accounts[1], now, 21 * NEARCENT);

        // The next assignment must be a review. Make sure the original submitter cannot apply for
        // it!
        now += 100_000_000_000;
        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);
        assert!(taskset.get_current_assignment(&accounts[0]).is_none());
        // This should also have removed the account
        assert_eq!(
            taskset.get_account_state(&accounts[0]),
            AccountState::NonExistent
        );
        // Reintroduce them
        taskset.register_account(&accounts[0]);
        taskset.apply_for_assignment(&accounts[0], 0, 20 * NEARCENT);

        // Assign a review to the second account
        now += 50_000_000;
        taskset.claim_assignment(&accounts[1], now, [0; 2], 21 * NEARCENT);
        assert_eq!(
            taskset.get_current_assignment(&accounts[1]).unwrap(),
            Assignment {
                // It's task 1 and not 0 because the review for task 0 was requeued when the 0th
                // participant returned it
                task_hash: tasks[0].2.clone(),
                ordinal: 1
            }
        );
        // Submit the review
        taskset.submit_review(
            accounts[1].clone(),
            ReviewResult::Reject,
            true,
            now,
            21 * NEARCENT,
        );

        assert_eq!(
            taskset.get_task_review_state(&tasks[0].2),
            TaskReviewState::PendingFinalization(false)
        );

        now += 130_000_000;
        // Get another review
        taskset.claim_assignment(&accounts[1], now, [0; 2], 21 * NEARCENT);
        assert_eq!(
            taskset.get_current_assignment(&accounts[1]).unwrap(),
            Assignment {
                task_hash: tasks[1].2.clone(),
                ordinal: 1
            }
        );
        // Submit it as well
        taskset.submit_review(
            accounts[1].clone(),
            ReviewResult::Accept,
            true,
            now,
            21 * NEARCENT,
        );

        assert_eq!(
            taskset.get_task_review_state(&tasks[1].2),
            TaskReviewState::PendingFinalization(true)
        );

        assert_eq!(
            taskset.reviews.get(&tasks[0].2).unwrap()[1]
                .clone()
                .unwrap()
                .review_result,
            ReviewResult::Reject
        );

        assert_eq!(
            taskset.reviews.get(&tasks[1].2).unwrap()[1]
                .clone()
                .unwrap()
                .review_result,
            ReviewResult::Accept
        );

        // Finalize the accepted task
        if fail_at == SanityTestFailure::FinalizeRejectedTask {
            let _ = taskset.finalize_task(false, b"moo".to_vec(), tasks[0].1.clone());
            return;
        }
        if fail_at == SanityTestFailure::ChallengeAcceptedTask {
            taskset.challenge(
                false,
                b"moo".to_vec(),
                tasks[1].1.clone(),
                now + TIMEOUT_BEFORE_CHALLENGE * 1000,
            );
            return;
        }
        assert_eq!(
            taskset.finalize_task(false, b"moo".to_vec(), tasks[1].1.clone()),
            (
                Reward {
                    account_id: accounts[0].clone(),
                    inner: RewardInner::Successful {
                        reward: 20 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[1].clone(),
                    inner: RewardInner::Successful {
                        reward: 21 * NEARCENT
                    }
                }
            )
        );

        assert_eq!(
            taskset.get_task_review_state(&tasks[1].2),
            TaskReviewState::None
        );

        if fail_at == SanityTestFailure::DoubleFinalize {
            let _ = taskset.finalize_task(false, b"moo".to_vec(), tasks[1].1.clone());
            return;
        }

        // Challenge the rejected task
        now += TIMEOUT_BEFORE_CHALLENGE - 1 - 130_000_000;
        if fail_at == SanityTestFailure::ChallengeTooEarly {
            taskset.challenge(false, b"moo".to_vec(), tasks[0].1.clone(), now);
            return;
        }
        now += 1;
        taskset.challenge(false, b"moo".to_vec(), tasks[0].1.clone(), now);

        assert_eq!(
            taskset.get_task_review_state(&tasks[0].2),
            TaskReviewState::Challenged
        );

        if fail_at == SanityTestFailure::DoubleChallenge {
            taskset.challenge(false, b"moo".to_vec(), tasks[0].1.clone(), now);
            return;
        }

        // Carry out the challenge process
        for i in 2..5 {
            taskset.apply_for_assignment(&accounts[i], now, (20 + i as u128) * NEARCENT);
        }
        now += 300_000_000_000;

        // When test2 claims review with ordinal 2 at index 0, it gets swapped with review with
        // ordinal 4, so test3 is expected to get ordinal 4, and test4 will get the remaining one
        let expected_ordinals = [2, 4, 3];
        for i in 2..5 {
            let ordinal = expected_ordinals[i - 2];
            taskset.claim_assignment(&accounts[i], now, [0; 2], (20 + i as u128) * NEARCENT);
            assert_eq!(
                taskset.get_current_assignment(&accounts[i]).unwrap(),
                Assignment {
                    task_hash: tasks[0].2.clone(),
                    ordinal
                }
            );

            if i == 4 && fail_at == SanityTestFailure::FinalizeChallengeTooEarly {
                taskset.finalize_challenged_task(&tasks[0].2);
                return;
            }

            assert_eq!(
                taskset.get_task_review_state(&tasks[0].2),
                TaskReviewState::Challenged
            );

            taskset.submit_review(
                accounts[i].clone(),
                if i == 4 {
                    ReviewResult::Reject
                } else {
                    ReviewResult::Accept
                },
                true,
                now,
                (20 + i as u128) * NEARCENT,
            );
        }

        assert_eq!(
            taskset.get_task_review_state(&tasks[0].2),
            TaskReviewState::ChallengeFinalization
        );

        // Finalize the challenged task
        assert_eq!(
            taskset.finalize_challenged_task(&tasks[0].2),
            vec![
                Reward {
                    account_id: accounts[0].clone(),
                    inner: RewardInner::Successful {
                        reward: 20 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[2].clone(),
                    inner: RewardInner::Successful {
                        reward: 22 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[3].clone(),
                    inner: RewardInner::Successful {
                        reward: 23 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[1].clone(),
                    inner: RewardInner::Failed { is_honeypot: false }
                },
                Reward {
                    account_id: accounts[4].clone(),
                    inner: RewardInner::Failed { is_honeypot: false }
                },
            ],
        );

        if fail_at == SanityTestFailure::DoubleFinalizeChallenge {
            taskset.finalize_challenged_task(&tasks[0].2);
            return;
        }

        // Add the next two tasks, that are honeypots
        taskset.add_tasks(tasks.iter().skip(2).take(2).map(|x| x.2.clone()).collect());

        // Claim the assignment
        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);

        let assignment = taskset.get_current_assignment(&accounts[0]).unwrap();
        assert_eq!(assignment.ordinal, 0);
        assert_eq!(assignment.task_hash, tasks[2].2);
        assert_eq!(taskset.unassigned_tasks.peek_front().unwrap(), tasks[3].2);

        // Submit a solution
        taskset.submit_solution(
            accounts[0].clone(),
            SolutionHash([0; 32]),
            true,
            now,
            20 * NEARCENT,
        );

        // Claim a new assignment and submit a solution
        now += 100_000_000_000;
        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);

        taskset.submit_solution(
            accounts[0].clone(),
            SolutionHash([0; 32]),
            true,
            now,
            20 * NEARCENT,
        );

        // Assign a review to the second account
        taskset.claim_assignment(&accounts[1], now, [0; 2], 21 * NEARCENT);
        assert_eq!(
            taskset.get_current_assignment(&accounts[1]).unwrap(),
            Assignment {
                // It's task 1 and not 0 because the review for task 0 was requeued when the 0th
                // participant returned it
                task_hash: tasks[2].2.clone(),
                ordinal: 1
            }
        );
        // Submit the review
        taskset.submit_review(
            accounts[1].clone(),
            ReviewResult::Accept,
            true,
            now,
            21 * NEARCENT,
        );
        now += 300_000_000_000;
        // Get another review
        taskset.claim_assignment(&accounts[1], now, [0; 2], 21 * NEARCENT);
        assert_eq!(
            taskset.get_current_assignment(&accounts[1]).unwrap(),
            Assignment {
                task_hash: tasks[3].2.clone(),
                ordinal: 1
            }
        );
        // Submit it as well
        taskset.submit_review(
            accounts[1].clone(),
            ReviewResult::Reject,
            true,
            now,
            21 * NEARCENT,
        );

        if fail_at == SanityTestFailure::PartialCreditForRejected {
            let _ = taskset.honeypot_partial_credit(accounts[1].clone(), &tasks[3].2);
        }

        assert_eq!(
            taskset.reviews.get(&tasks[2].2).unwrap()[1]
                .clone()
                .unwrap()
                .review_result,
            ReviewResult::Accept
        );

        assert_eq!(
            &taskset.reviews.get(&tasks[3].2).unwrap()[1]
                .clone()
                .unwrap()
                .review_result,
            &ReviewResult::Reject
        );

        // Finalize the rejected honeypot
        assert_eq!(
            taskset.finalize_task(true, b"moo".to_vec(), tasks[3].1.clone()),
            (
                Reward {
                    account_id: accounts[0].clone(),
                    inner: RewardInner::Successful {
                        reward: 20 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[1].clone(),
                    inner: RewardInner::Successful {
                        reward: 21 * NEARCENT
                    }
                }
            )
        );

        // Challenge the accepted honeypot
        now += TIMEOUT_BEFORE_CHALLENGE;
        taskset.challenge(true, b"moo".to_vec(), tasks[2].1.clone(), now);

        // Carry out the challenge process
        now += 1_000_000_000;

        // This time also try claiming reviews by test0 and test1 and make sure they don't get any
        // assignment
        for i in 0..3 {
            taskset.claim_assignment(&accounts[i], now, [0; 2], (20 + i as u128) * NEARCENT);

            if i < 2 {
                assert!(taskset.get_current_assignment(&accounts[i]).is_none());
                // This should also have removed the account
                assert_eq!(
                    taskset.get_account_state(&accounts[i]),
                    AccountState::NonExistent
                );
                // Reintroduce them
                taskset.register_account(&accounts[i]);
                taskset.apply_for_assignment(&accounts[i], 0, (20 + i as Balance) * NEARCENT);
            } else {
                assert_eq!(
                    taskset.get_current_assignment(&accounts[i]).unwrap(),
                    Assignment {
                        task_hash: tasks[2].2.clone(),
                        ordinal: 3
                    }
                );

                taskset.submit_review(
                    accounts[i].clone(),
                    ReviewResult::Accept,
                    true,
                    now,
                    (20 + i as u128) * NEARCENT,
                );
            }
        }
        // TODO: duplocate reviews

        let expected_ordinals = [2, 4];
        for i in 3..5 {
            let ordinal = expected_ordinals[i - 3];
            taskset.claim_assignment(&accounts[i], now, [0; 2], (20 + i as u128) * NEARCENT);
            assert_eq!(
                taskset.get_current_assignment(&accounts[i]).unwrap(),
                Assignment {
                    task_hash: tasks[2].2.clone(),
                    ordinal
                }
            );

            taskset.submit_review(
                accounts[i].clone(),
                if i == 3 {
                    ReviewResult::Accept
                } else {
                    ReviewResult::Reject
                },
                true,
                now,
                (20 + i as u128) * NEARCENT,
            );
        }
        // Finalize the challenged task
        assert_eq!(
            taskset.finalize_challenged_task(&tasks[2].2),
            vec![
                Reward {
                    account_id: accounts[1].clone(),
                    inner: RewardInner::Successful {
                        reward: 21 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[3].clone(),
                    inner: RewardInner::Successful {
                        reward: 23 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[2].clone(),
                    inner: RewardInner::Successful {
                        reward: 22 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[0].clone(),
                    inner: RewardInner::Failed { is_honeypot: true }
                },
                Reward {
                    account_id: accounts[4].clone(),
                    inner: RewardInner::Failed { is_honeypot: true }
                },
            ],
        );

        now += 1_000_000_000;

        // The failed challenge must have returned the task to the unassigned set

        // Claim the assignment
        taskset.claim_assignment(&accounts[0], now, [0; 2], 20 * NEARCENT);

        let assignment = taskset.get_current_assignment(&accounts[0]).unwrap();
        assert_eq!(assignment.ordinal, 0);
        assert_eq!(assignment.task_hash, tasks[2].2);
        assert_eq!(taskset.unassigned_tasks.peek_front(), None);

        // Submit a solution
        taskset.submit_solution(
            accounts[0].clone(),
            SolutionHash([0; 32]),
            true,
            now,
            20 * NEARCENT,
        );

        // Assign a review to the second account
        now += 100_000_000_000;
        taskset.claim_assignment(&accounts[1], now, [0; 2], 21 * NEARCENT);
        assert_eq!(
            taskset.get_current_assignment(&accounts[1]).unwrap(),
            Assignment {
                task_hash: tasks[2].2.clone(),
                ordinal: 1
            }
        );
        // Submit the review
        taskset.submit_review(
            accounts[1].clone(),
            ReviewResult::Accept,
            true,
            now,
            21 * NEARCENT,
        );

        // Reconsider
        assert_eq!(
            taskset.honeypot_partial_credit(accounts[1].clone(), &tasks[2].2),
            (
                Reward {
                    account_id: accounts[0].clone(),
                    inner: RewardInner::Successful {
                        reward: 20 * NEARCENT
                    }
                },
                Reward {
                    account_id: accounts[1].clone(),
                    inner: RewardInner::PartialHoneyPot {
                        reward_base: 21 * NEARCENT
                    }
                }
            )
        );

        if fail_at == SanityTestFailure::FinalizeAfterPartialCreditReject {
            let _ = taskset.finalize_task(true, b"moo".to_vec(), tasks[2].1.clone());
        }
    }
}
