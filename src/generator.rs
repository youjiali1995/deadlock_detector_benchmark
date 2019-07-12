use super::error::Error;
use futures::prelude::*;
use kvproto::deadlock::*;
use rand::prelude::*;

pub struct Generator {
    rng: ThreadRng,
    range: u64,
    timestamp: u64,
}

impl Generator {
    pub fn new(range: u64) -> Self {
        Self {
            rng: ThreadRng::default(),
            range,
            timestamp: 0,
        }
    }

    /// Generates a detect request
    pub fn generate(&mut self) -> DeadlockRequest {
        let mut entry = WaitForEntry::new();
        entry.set_txn(self.timestamp);

        let mut wait_for_txn = self.timestamp;
        while wait_for_txn == self.timestamp {
            wait_for_txn = self.rng.gen_range(
                if self.timestamp < self.range {
                    0
                } else {
                    self.timestamp - self.range
                },
                self.timestamp + self.range,
            );
        }
        entry.set_wait_for_txn(wait_for_txn);
        entry.set_key_hash(self.rng.gen());
        let mut req = DeadlockRequest::new();
        req.set_tp(DeadlockRequestType::Detect);
        req.set_entry(entry);
        self.timestamp += 1;
        req
    }

    /// Generates two detect requests with maximum timestamp.
    pub fn generate_deadlock_entries(&self) -> (DeadlockRequest, DeadlockRequest) {
        let mut entry = WaitForEntry::new();
        entry.set_txn(u64::max_value() - 1);
        entry.set_wait_for_txn(u64::max_value());
        entry.set_key_hash(0);
        let mut req1 = DeadlockRequest::new();
        req1.set_tp(DeadlockRequestType::Detect);
        req1.set_entry(entry);

        let mut entry = WaitForEntry::new();
        entry.set_txn(u64::max_value());
        entry.set_wait_for_txn(u64::max_value() - 1);
        entry.set_key_hash(0);
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
