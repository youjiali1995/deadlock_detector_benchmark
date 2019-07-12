#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate log;

#[macro_use]
pub mod error;
mod client;
mod generator;

use client::Client;
use error::Error;
use futures::prelude::*;
use generator::Generator;
use kvproto::deadlock::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use structopt::StructOpt;
use tokio_core::reactor::Core;
use tokio_timer::Delay;

use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;
use std::time::{Duration, Instant};

pub type Result<T> = std::result::Result<T, error::Error>;

static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static DEADLOCKS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long = "addr", default_value = "127.0.0.1:20160")]
    /// The address of the TiKV
    addr: String,

    #[structopt(long = "thread-num", default_value = "1")]
    /// The number of threads sending requests to the TiKV
    thread_num: usize,

    #[structopt(long = "requests", default_value = "10000")]
    /// The number of requests each thread sending to the TiKV
    requests: usize,

    #[structopt(long = "delay", default_value = "3000")]
    /// The delay of the `CleanUp` requests sending to the TiKV
    delay: usize,

    #[structopt(long = "range", default_value = "1000")]
    /// The range of `wait_for_txn`.
    /// If `range` is 1000, and the current timestamp is 2000, the generated detect request will
    /// be `Detect { txn: 2000, wait_for_txn: [2000-1000, 2000+1000), hash: random_value }`
    range: usize,
}

fn init_logger() {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
}

fn benchmark(opt: Arc<Opt>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut generator = Generator::new(opt.range as u64);
    let (entry1, entry2) = generator.generate_deadlock_entries();

    let mut client = Client::new(&opt.addr);
    let requests = opt.requests;
    let entry = entry2.clone();
    let (send, recv) = client.register_detect_handler(Box::new(move |resp| {
        DEADLOCKS.fetch_add(1, Ordering::Relaxed);
        // When recving the deadlock response corresponding to the last detect request, all the previous requests are handled.
        if resp.get_entry() == entry.get_entry() {
            debug!("send finished");
            REQUESTS.fetch_add(requests, Ordering::Relaxed);
            Err(Error::Other(box_err!("finished")))
        } else {
            Ok(())
        }
    }));
    handle.spawn(send.map_err(|e| error!("client failed: {:?}", e)));

    for _ in 0..opt.requests {
        let req = generator.generate();
        client.detect(req.clone()).unwrap();
    }

    client.detect(entry1).unwrap();
    client.detect(entry2).unwrap();
    core.run(recv).unwrap_err();
}

fn start_benchmark_workers(opt: Arc<Opt>) -> Vec<JoinHandle<()>> {
    let mut handles = Vec::with_capacity(opt.thread_num);
    for _ in 0..opt.thread_num {
        let opt = Arc::clone(&opt);
        handles.push(thread::spawn(|| benchmark(opt)));
    }
    handles
}

fn clean_up(opt: Arc<Opt>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut generator = Generator::new(opt.range as u64);
    let (entry1, entry2) = generator.generate_deadlock_entries();

    let mut client = Client::new(&opt.addr);
    let requests = opt.requests;
    let (send, recv) = client.register_detect_handler(Box::new(move |_| {
        REQUESTS.fetch_add(requests, Ordering::Relaxed);
        Err(Error::Other(box_err!("finished")))
    }));
    let delay = Delay::new(Instant::now() + Duration::from_millis(opt.delay as u64))
        .then(move |_| send.map_err(|_| ()));
    handle.spawn(delay);

    for _ in 0..opt.requests {
        let mut req = generator.generate();
        req.set_tp(DeadlockRequestType::CleanUp);
        client.detect(req).unwrap();
    }
    client.detect(entry1).unwrap();
    client.detect(entry2).unwrap();
    core.run(recv).unwrap_err();
}

fn start_clean_up_worker(opt: Arc<Opt>) -> JoinHandle<()> {
    thread::spawn(|| clean_up(opt))
}

fn main() {
    let opt = Arc::new(Opt::from_args());

    init_logger();

    let now = Instant::now();

    let mut handles = vec![];

    handles.append(&mut start_benchmark_workers(Arc::clone(&opt)));
    handles.push(start_clean_up_worker(Arc::clone(&opt)));

    for handle in handles {
        let _ = handle.join();
    }

    let elapsed = now.elapsed().as_millis();
    let requests = REQUESTS.load(Ordering::Relaxed);
    info!(
        "{} requests finished in {}ms, deadlocks: {}, qps: {:.2}",
        requests,
        elapsed,
        DEADLOCKS.load(Ordering::Relaxed),
        (requests as f64) / (elapsed as f64) * 1000f64
    );
}
