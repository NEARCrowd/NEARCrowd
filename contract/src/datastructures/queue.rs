use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::Vector;
use near_sdk::env;
use std::str;

const ERR_BYTES: &[u8] = b"Error on deque";
const ERR: &str = unsafe {
    str::from_utf8_unchecked(ERR_BYTES)
};

#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct Queue<T: near_sdk::borsh::BorshDeserialize> {
    left: u64,
    prefix: Vec<u8>,
    v: Vector<T>,
}

impl<T> Queue<T>
where
    T: BorshSerialize + BorshDeserialize,
{
    pub fn new(id: Vec<u8>) -> Self {
        Self {
            left: 0,
            prefix: id.clone(),
            v: Vector::new(id),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.left == self.v.len()
    }

    pub fn len(&self) -> u64 {
        self.v.len() - self.left
    }

    pub fn enqueue(&mut self, element: &T) {
        self.v.push(element);
    }

    #[allow(unused)]
    pub fn peek_front(&self) -> Option<T> {
        if self.left == self.v.len() {
            None
        } else {
            self.v.get(self.left)
        }
    }

    pub fn dequeue(&mut self) -> Option<T> {
        if self.left == self.v.len() {
            None
        } else {
            let lookup_key = &[&self.prefix, &self.left.to_le_bytes()[..]].concat();
            self.left += 1;

            let raw_last_value = if env::storage_remove(&lookup_key) {
                match env::storage_get_evicted() {
                    Some(x) => x,
                    None => env::panic_str(ERR),
                }
            } else {
                
                env::panic_str(ERR)
            };
            let ret = match T::try_from_slice(&raw_last_value) {
                Ok(x) => x,
                Err(_) => env::panic_str(ERR),
            };
            Some(ret)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;    

    #[test]
    fn test_queue_sanity() {    
        let mut q: Queue<u64> = Queue::new(vec![b'a']);

        assert_eq!(q.dequeue(), None);

        q.enqueue(&5);
        q.enqueue(&6);

        assert_eq!(q.dequeue(), Some(5));

        q.enqueue(&7);

        assert_eq!(q.dequeue(), Some(6));
        assert_eq!(q.dequeue(), Some(7));
        assert_eq!(q.dequeue(), None);
    }
}
