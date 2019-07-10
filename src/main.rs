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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use structopt::StructOpt;
use tokio_core::reactor::Core;

use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use std::io::Write;
use std::time::Instant;

pub type Result<T> = std::result::Result<T, error::Error>;

static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static DEADLOCKS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long = "addr", default_value = "127.0.0.1:20160")]
    addr: String,

    #[structopt(long = "thread-num", default_value = "1")]
    thread_num: usize,

    #[structopt(long = "range", default_value = "10000")]
    range: usize,

    #[structopt(long = "requests", default_value = "10000")]
    requests: usize,
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

    for _ in 0..opt.requests / 2 {
        let reqs = generator.generate();
        client.detect(reqs.0).unwrap();
        client.detect(reqs.1).unwrap();
    }
    client.detect(entry1).unwrap();
    client.detect(entry2).unwrap();
    core.run(recv).unwrap_err();
}

fn start_benchmark_workers(opt: Opt) {
    let opt = Arc::new(opt);
    let mut handles = Vec::with_capacity(opt.thread_num);
    for _ in 0..opt.thread_num {
        let opt = Arc::clone(&opt);
        handles.push(thread::spawn(|| benchmark(opt)));
    }

    for handle in handles {
        let _ = handle.join();
    }
}

fn main() {
    let opt = Opt::from_args();

    init_logger();

    let now = Instant::now();

    start_benchmark_workers(opt);

    let elapsed = now.elapsed().as_millis();
    let requests = REQUESTS.load(Ordering::Relaxed);
    info!(
        "{} requests finished in {}ms, deadlocks: {}, qps: {}",
        requests,
        elapsed,
        DEADLOCKS.load(Ordering::Relaxed),
        requests / (elapsed as usize) * 1000
    );
}
