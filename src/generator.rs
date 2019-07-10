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

    /// Generates a detect request and a clean-up-wait-for request.
    pub fn generate(&mut self) -> (DeadlockRequest, DeadlockRequest) {
        let txn = self.rng.gen_range(0, self.range);
        let mut wait_for_txn = txn;
        while wait_for_txn == txn {
            wait_for_txn = self.rng.gen_range(0, self.range);
        }

        let mut entry = WaitForEntry::new();
        entry.set_txn(txn);
        entry.set_wait_for_txn(wait_for_txn);
        entry.set_key_hash(self.rng.gen_range(0, self.range));
        let mut detect_req = DeadlockRequest::new();
        detect_req.set_tp(DeadlockRequestType::Detect);
        detect_req.set_entry(entry);

        let mut clean_up_req = detect_req.clone();
        clean_up_req.set_tp(DeadlockRequestType::CleanUpWaitFor);
        (detect_req, clean_up_req)
    }

    /// Generates two detect requests out of the range which cause deadlock.
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
    type Item = (DeadlockRequest, DeadlockRequest);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(Some(self.generate())))
    }
}
