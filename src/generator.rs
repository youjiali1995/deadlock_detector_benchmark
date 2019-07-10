use super::error::Error;
use futures::prelude::*;
use kvproto::deadlock::*;
use rand::prelude::*;

pub struct Generator {
    range: u64,
    rng: ThreadRng,
}

impl Generator {
    pub fn new(range: u64) -> Self {
        Self {
            range,
            rng: ThreadRng::default(),
        }
    }

    pub fn generate(&mut self) -> DeadlockRequest {
        let tp = match self.rng.gen_range(0, 3) {
            0 => DeadlockRequestType::Detect,
            1 => DeadlockRequestType::CleanUpWaitFor,
            2 => DeadlockRequestType::CleanUp,
            _ => unreachable!(),
        };
        let mut entry = WaitForEntry::new();
        entry.set_txn(self.rng.gen_range(0, self.range));
        entry.set_wait_for_txn(self.rng.gen_range(0, self.range));
        entry.set_key_hash(self.rng.gen_range(0, self.range));
        let mut req = DeadlockRequest::new();
        req.set_tp(tp);
        req.set_entry(entry);
        req
    }

    pub fn generate_deadlock_entries(&self) -> (DeadlockRequest, DeadlockRequest) {
        let mut entry = WaitForEntry::new();
        entry.set_txn(self.range);
        entry.set_wait_for_txn(self.range + 1);
        entry.set_key_hash(self.range);
        let mut req1 = DeadlockRequest::new();
        req1.set_tp(DeadlockRequestType::Detect);
        req1.set_entry(entry);

        let mut entry = WaitForEntry::new();
        entry.set_txn(self.range + 1);
        entry.set_wait_for_txn(self.range);
        entry.set_key_hash(self.range + 1);
        let mut req2 = DeadlockRequest::new();
        req2.set_tp(DeadlockRequestType::Detect);
        req2.set_entry(entry);

        (req1, req2)
    }
}

impl Stream for Generator {
    type Item = DeadlockRequest;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(Some(self.generate())))
    }
}
