use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::Balance;

const NANOSECOND: u64 = 1_000_000_000;
const PRICE_CHANGE_UP: Balance = 1_005;
const PRICE_CHANGE_DOWN: Balance = 995;
const PRICE_CHANGE_DENOM: Balance = 1_000;

#[derive(BorshDeserialize, BorshSerialize)]
pub struct TasksFaucet {
    max_price: Balance,
    min_price: Balance,
    mtasks_per_second: u64,
    cur_price: Balance,
    next_task_at: u64,
}

impl TasksFaucet {
    pub fn new(max_price: Balance, min_price: Balance, mtasks_per_second: u64) -> Self {
        Self {
            max_price,
            min_price,
            mtasks_per_second,
            cur_price: max_price,
            next_task_at: 0,
        }
    }

    pub fn update_prices(&mut self, new_min_price: Balance, new_max_price: Balance) {
        self.min_price = new_min_price;
        self.max_price = new_max_price;
        if new_min_price > self.cur_price {
            self.cur_price = new_min_price
        } else if new_max_price < self.cur_price {
            self.cur_price = new_max_price
        }
    }

    pub fn update_mtasks_per_second(&mut self, mtasks_per_second: u64) {
        self.mtasks_per_second = mtasks_per_second;
    }

    pub fn get_next_price_and_wait_time(&self, now: u64) -> (Balance, u64) {
        let when = self.next_task_at
            + (NANOSECOND * 1000 + self.mtasks_per_second - 1) / self.mtasks_per_second;

        let price = if when + 60 * NANOSECOND <= now {
            // If not a single task was assigned in a minute, reset to max price
            self.max_price
        } else if when < now + 10 * NANOSECOND {
            // If the queue is less than 10 seconds long, increase the price
            u128::min(
                self.cur_price * PRICE_CHANGE_UP / PRICE_CHANGE_DENOM,
                self.max_price,
            )
        } else {
            // Otherwise decrease the price
            u128::max(
                self.cur_price * PRICE_CHANGE_DOWN / PRICE_CHANGE_DENOM,
                self.min_price,
            )
        };

        (price, when)
    }

    // Returns time at which the task will be granted
    pub fn request_task(&mut self, now: u64) -> u64 {
        let (next_price, when) = self.get_next_price_and_wait_time(now);

        self.cur_price = next_price;

        self.next_task_at = if when < now { now } else { when };

        self.next_task_at
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;
    use near_sdk::test_utils::test_env;

    #[test]
    fn test_faucet_prices() {
        test_env::setup();

        const NEARCENT: Balance = 10_000_000_000_000_000_000_000;
        const MAX_PRICE: Balance = 500 * NEARCENT;
        const MIN_PRICE: Balance = 100 * NEARCENT;
        const TASKS_PER_SECOND: u64 = 1;

        let mut now = NANOSECOND * 3600 * 24;
        let mut faucet = TasksFaucet::new(MAX_PRICE, MIN_PRICE, 1000 * TASKS_PER_SECOND);

        let mut cur_price = MAX_PRICE;
        assert_eq!(faucet.cur_price, cur_price);

        for _ in 0..10 {
            faucet.request_task(now);
            assert_eq!(faucet.cur_price, cur_price);
        }

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_DOWN / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_DOWN / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        now += NANOSECOND;

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_DOWN / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        now += NANOSECOND * 3;

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_UP / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_DOWN / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        now += NANOSECOND * 3;

        faucet.request_task(now);
        cur_price = cur_price * PRICE_CHANGE_UP / PRICE_CHANGE_DENOM;
        assert_eq!(faucet.cur_price, cur_price);

        now += NANOSECOND * 80;

        faucet.request_task(now);
        cur_price = MAX_PRICE;
        assert_eq!(faucet.cur_price, cur_price);
    }

    #[test]
    fn test_faucet_speed() {
        const TASKS_TO_MAINTAIN: usize = 20;
        const TASKS_PER_SECOND: u64 = 8;
        const TIME_TO_RUN_SEC: u64 = 1000;
        const TIME_TO_RUN_NANO: u64 = TIME_TO_RUN_SEC * 1_000_000_000;

        test_env::setup_free();
        let mut faucet = TasksFaucet::new(0, 0, 1000 * TASKS_PER_SECOND);

        let mut tasks_assigned = 0;

        let mut now = 0;
        let mut tasks_at = vec![];
        while now < TIME_TO_RUN_NANO {
            while tasks_at.len() < TASKS_TO_MAINTAIN {
                tasks_at.push(faucet.request_task(now));
            }

            let min_task_at = *tasks_at.iter().min().unwrap();
            assert!(min_task_at >= now);

            now = min_task_at;
            for i in (0..tasks_at.len()).rev() {
                while i < tasks_at.len() && tasks_at[i] == min_task_at {
                    tasks_assigned += 1;
                    tasks_at[i] = tasks_at[tasks_at.len() - 1];
                    tasks_at.pop();
                }
            }
        }

        println!("{}", tasks_assigned);
        assert!(tasks_assigned <= TIME_TO_RUN_SEC * TASKS_PER_SECOND + 1);
        assert!(tasks_assigned >= TIME_TO_RUN_SEC * (TASKS_PER_SECOND - 1));
    }
}
