use std::io;
use std::io::ErrorKind;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chashmap::CHashMap;

const MICROS_PER_SEC: u64 = 1_000_000;
const NANOS_PER_MICRO: u32 = 1_000;

pub struct Stats {
    requests: CHashMap<&'static str, StatValue>,
    requests_with_params: CHashMap<String, StatValue>,
    count: AtomicUsize,

    count_net: AtomicUsize,
    count_accept: AtomicUsize,
    count_accept_by_thread: [AtomicUsize; 4],
    count_accept_and_read: AtomicUsize,
    count_read: AtomicUsize,
    read_errors: CHashMap<ErrorKind, usize>,
    write_errors: CHashMap<ErrorKind, usize>,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            requests: CHashMap::new(),
            requests_with_params: CHashMap::new(),
            count: AtomicUsize::new(0),

            count_net: AtomicUsize::new(0),
            count_accept: AtomicUsize::new(0),
            count_accept_by_thread: [AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), ],
            count_accept_and_read: AtomicUsize::new(0),
            count_read: AtomicUsize::new(0),
            read_errors: CHashMap::new(),
            write_errors: CHashMap::new(),
        }
    }

    pub fn register(&self, request_type: &'static str, elapsed: Duration, params: &Vec<(String, String)>) {
        let elapsed_micros = elapsed.as_secs() * MICROS_PER_SEC + (elapsed.subsec_nanos() / NANOS_PER_MICRO) as u64;

        let mut conditions: Vec<String> = params.iter()
            .filter(|(k, _)| k != "limit" && k != "query_id" && k != "order" && k != "keys")
            .map(|(k, v)| if k.ends_with("_null") { k.clone() + "=" + v } else { k.clone() })
            .collect();
        conditions.sort();

        self.requests.upsert(request_type,
                             || StatValue { count: 1, total_time_micros: elapsed_micros, max_time_micros: elapsed_micros },
                             |stat| {
                                 stat.count += 1;
                                 stat.total_time_micros += elapsed_micros;
                                 if elapsed_micros > stat.max_time_micros {
                                     let i = elapsed_micros;
                                     stat.max_time_micros = i;
                                 }
                             });
        self.requests_with_params.upsert(format!("{}_{:?}", request_type.to_string(), conditions),
                                         || StatValue { count: 1, total_time_micros: elapsed_micros, max_time_micros: elapsed_micros },
                                         |stat| {
                                             stat.count += 1;
                                             stat.total_time_micros += elapsed_micros;
                                             if elapsed_micros > stat.max_time_micros {
                                                 let i = elapsed_micros;
                                                 stat.max_time_micros = i;
                                             }
                                         });

        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if (count + 1) % 1000 == 0 {
            self.print();
        }
    }

    pub fn print(&self) {
        info!("*** stats requests: count: {}", self.count.load(Ordering::SeqCst));
        self.requests.clone().into_iter().for_each(|(k, v)| {
            info!("{}: count: {}, mean: {:.2} ms, max: {:.2} ms", k, v.count, v.total_time_micros as f64 / v.count as f64 / 1000.0, v.max_time_micros as f64 / 1000.0);
        });
        info!("top mean:");
        let mut requests_with_params: Vec<(_, _)> = self.requests_with_params.clone().into_iter().collect();
        requests_with_params.sort_by_key(|(_, v)| v.total_time_micros / v.count as u64);
        requests_with_params.iter().rev()
            .take(10)
            .for_each(|(k, v)| {
                info!("{}: count: {}, mean: {:.2} ms, max: {:.2} ms", k, v.count, v.total_time_micros as f64 / v.count as f64 / 1000.0, v.max_time_micros as f64 / 1000.0);
            });
        info!("top max:");
        let mut requests_with_params: Vec<(_, _)> = self.requests_with_params.clone().into_iter().collect();
        requests_with_params.sort_by_key(|(_, v)| v.max_time_micros);
        requests_with_params.iter().rev()
            .take(20)
            .for_each(|(k, v)| {
                info!("{}: count: {}, mean: {:.2} ms, max: {:.2} ms", k, v.count, v.total_time_micros as f64 / v.count as f64 / 1000.0, v.max_time_micros as f64 / 1000.0);
            });
        info!("top popular:");
        let mut requests_with_params: Vec<(_, _)> = self.requests_with_params.clone().into_iter().collect();
        requests_with_params.sort_by_key(|(_, v)| v.count);
        requests_with_params.iter().rev()
            .filter(|(k, v)| k.starts_with("FILTER") && (v.total_time_micros / v.count as u64) >= 100 as u64)
            .take(20)
            .for_each(|(k, v)| {
                info!("{}: count: {}, mean: {:.2} ms, max: {:.2} ms", k, v.count, v.total_time_micros as f64 / v.count as f64 / 1000.0, v.max_time_micros as f64 / 1000.0);
            });
    }

    pub fn register_read(&self) {
        let count_net = self.count_net.fetch_add(1, Ordering::SeqCst);
        self.count_read.fetch_add(1, Ordering::SeqCst);
        if (count_net + 1) % 1000 == 0 {
            self.print_net();
        }
    }

    pub fn register_accept(&self, thread_id: usize) {
        let count_net = self.count_net.fetch_add(1, Ordering::SeqCst);
        self.count_accept.fetch_add(1, Ordering::SeqCst);
        self.count_accept_by_thread[thread_id].fetch_add(1, Ordering::SeqCst);
        if (count_net + 1) % 1000 == 0 {
            self.print_net();
        }
    }

    pub fn register_accept_and_read(&self) {
        let count_net = self.count_net.fetch_add(1, Ordering::SeqCst);
        self.count_accept_and_read.fetch_add(1, Ordering::SeqCst);
        if (count_net + 1) % 1000 == 0 {
            self.print_net();
        }
    }

    pub fn register_read_error(&self, kind: ErrorKind) {
        let count_net = self.count_net.fetch_add(1, Ordering::SeqCst);
        self.read_errors.upsert(kind,
                                || 1,
                                |count| { *count += 1; },
        );
        if *self.read_errors.get(&kind).unwrap() <= 5 {
            error!("{}", io::Error::from(kind));
        }
        if (count_net + 1) % 1000 == 0 {
            self.print_net();
        }
    }

    pub fn register_write_error(&self, kind: ErrorKind) {
        let count_net = self.count_net.fetch_add(1, Ordering::SeqCst);
        self.write_errors.upsert(kind,
                                 || 1,
                                 |count| { *count += 1; },
        );
        if *self.write_errors.get(&kind).unwrap() <= 5 {
            error!("{}", io::Error::from(kind));
        }
        if (count_net + 1) % 1000 == 0 {
            self.print_net();
        }
    }

    pub fn print_net(&self) {
        info!("*** stats net count: {}: accept {} [{},{},{},{}], read_accept {}, read {}",
              self.count_net.load(Ordering::SeqCst),
              self.count_accept.load(Ordering::SeqCst),
              self.count_accept_by_thread[0].load(Ordering::SeqCst),
              self.count_accept_by_thread[1].load(Ordering::SeqCst),
              self.count_accept_by_thread[2].load(Ordering::SeqCst),
              self.count_accept_by_thread[3].load(Ordering::SeqCst),
              self.count_accept_and_read.load(Ordering::SeqCst),
              self.count_read.load(Ordering::SeqCst));

        if !self.read_errors.is_empty() {
            info!("read errors:");
            let mut read_errors: Vec<(_, _)> = self.read_errors.clone().into_iter().collect();
            read_errors.sort_by_key(|(_, v)| *v);
            read_errors.iter().rev()
                .take(10)
                .for_each(|(k, v)| {
                    info!("{}: count: {}", io::Error::from(*k), v);
                });
        }

        if !self.write_errors.is_empty() {
            info!("write errors:");
            let mut write_errors: Vec<(_, _)> = self.write_errors.clone().into_iter().collect();
            write_errors.sort_by_key(|(_, v)| *v);
            write_errors.iter().rev()
                .take(10)
                .for_each(|(k, v)| {
                    info!("{}: count: {}", io::Error::from(*k), v);
                });
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct StatKey {
    request: &'static str,
    params: String,
}

#[derive(Clone, Debug)]
struct StatValue {
    count: u32,
    total_time_micros: u64,
    max_time_micros: u64,
}
