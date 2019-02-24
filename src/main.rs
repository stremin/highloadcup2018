#[macro_use]
extern crate enum_map;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use mio::{IoVec, Poll, PollOpt, Ready, Token};
#[cfg(target_os = "linux")]
use mio::Event;
#[cfg(not(target_os = "linux"))]
use mio::Events;
use mio::net::TcpListener;
use mio::net::TcpStream;
use net2::TcpBuilder;
#[cfg(unix)]
use net2::unix::UnixTcpBuilderExt;
use percent_encoding::{DEFAULT_ENCODE_SET, percent_encode};
use spin;

use crate::storage::Storage;
use crate::utils::StatusCode;

mod storage;
mod filter;
mod group;
mod recommend;
mod suggest;
mod utils;
mod topn;
mod group_index;
mod stats;
mod filter_index;
mod bits;
mod process;

lazy_static! {
    static ref COMMON_HEADERS: Vec<&'static str> = vec![
        "content-type: application/json, charset=utf-8",
        "date: Sun, 13 Jan 2019 18:40:03 GMT",
        "server: hlc",
        "connection: keep-alive", // вроде бы танк смотрит только на ответ
    ];
    static ref COMMON_HEADERS_AS_STR: String = COMMON_HEADERS.join("\r\n") + "\r\n";
    static ref STATUS_400: String = "HTTP/1.1 400 Bad Request\r\n".to_string() +
        &COMMON_HEADERS_AS_STR +
        "content-length: 0\r\n" +
        "\r\n";
}

fn main() {
    env_logger::init();

    let matches = clap::App::new("hlc2018")
        .arg(clap::Arg::with_name("PORT")
            .help("Port to listen at")
            .required(true)
            .index(1))
        .arg(clap::Arg::with_name("DATA_DIR")
            .help("Data directory")
            .required(true)
            .index(2))
        .arg(clap::Arg::with_name("threads")
            .help("Receiving threads")
            .short("t")
            .long("threads")
            .takes_value(true)
            .default_value("4"))
        .arg(clap::Arg::with_name("no-stats")
            .help("Disable statistics")
            .long("no-stats"))
        .arg(clap::Arg::with_name("cache")
            .help("Use response cache")
            .long("cache")
            .takes_value(true)
            .possible_values(&["on", "off", "random"])
            .default_value("off"))
        .get_matches();

    let port = matches.value_of("PORT").unwrap().parse::<u16>().unwrap();
    let data_dir = matches.value_of("DATA_DIR").unwrap();
    let num_threads = matches.value_of("threads").unwrap().parse::<usize>().unwrap();
    let record_stats = !matches.is_present("no-stats");

    let cache = match matches.value_of("cache").unwrap() {
        "on" => true,
        "off" => false,
        "random" => rand::random(),
        _ => unreachable!(),
    };
    info!("using response cache: {}", cache);

    #[cfg(target_os = "linux")]
        {
            use std::fs::File;
            use std::io::BufRead;
            use std::io::BufReader;
            use std::path::Path;

            if let Ok(version_file) = File::open(Path::new("/proc/version")) {
                let version_first_line = BufReader::new(version_file).lines().next().unwrap().unwrap();
                info!("version: {}", version_first_line);
            }
            if let Ok(cpuinfo_file) = File::open(Path::new("/proc/cpuinfo")) {
                let mut model = None;
                let mut mhz = None;
                for line in BufReader::new(cpuinfo_file).lines() {
                    if let Ok(line) = line {
                        if line.starts_with("model name") && model.is_none() {
                            model = Some(line.clone());
                        }
                        if line.starts_with("cpu MHz") && mhz.is_none() {
                            mhz = Some(line.clone());
                        }
                    }
                }
                info!("{}", model.unwrap_or("".to_string()));
                info!("{}", mhz.unwrap_or("".to_string()));
            }

            if let Err(err) = nix::sys::mman::mlockall(nix::sys::mman::MlockAllFlags::MCL_CURRENT | nix::sys::mman::MlockAllFlags::MCL_FUTURE) {
                warn!("mlockall error: {}", err);
            }
        }

    let storage = Arc::new(RwLock::new(storage::Storage::load(data_dir)));
    debug!("{:?}", storage.read().unwrap().accounts[1]);

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    // TODO accept4? tcp_defer_accept?

    const SERVER: Token = Token(0);

    let mut threads = Vec::new();
    for thread_id in 0..num_threads {
        // poll threads
        let storage = storage.clone();
        let thread_data = Arc::new(ThreadData {
            server: bind(&addr).unwrap(),
            poll: Poll::new().unwrap(),
            connections: spin::Mutex::new(HashMap::new()),
        });
        thread_data.poll.register(&thread_data.server, SERVER, Ready::readable(), PollOpt::edge()).unwrap();
        threads.push(thread::spawn(move || {
            let thread_data = thread_data.clone();
            let mut events = Events::with_capacity(1024);
            loop {
                poll(&thread_data.poll, &mut events); // epoll 0
                for event in events.iter() {
//                    debug!("{} {:?}", i, event);
                    match event.token() {
                        SERVER => {
                            loop {
                                match thread_data.server.accept() {
                                    Ok((stream, addr2)) => {
                                        // debug!("accepted thread_id {} {:?}", thread_id, addr2);
                                        stream.set_nodelay(true).unwrap();
                                        if record_stats {
                                            storage.read().unwrap().stats.register_accept(thread_id);
                                        }
                                        let token = Token(addr2.port() as usize);
                                        thread_data.poll.register(&stream, token, Ready::readable() /*| Ready::writable()*/, PollOpt::edge()).unwrap(); // TODO EPOLLEXCLUSIVE ?
                                        let conn_id = token.0;
                                        {
                                            thread_data.connections.lock().insert(conn_id, Connection { stream, buf: [0; 8192], len: 0 });
                                            let mut remove_conn = false;
                                            try_read_and_process(&thread_data.connections, &storage, true, record_stats, cache, &mut remove_conn, thread_id, conn_id);
                                            if remove_conn {
                                                //warn!("remove_conn1 {}", conn_id);
                                                thread_data.connections.lock().remove(&conn_id);
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if err.kind() == io::ErrorKind::WouldBlock {
                                            break;
                                        } else {
                                            error!("accept error: {}", err);
                                        }

                                    }
                                }
                            }
                        }

                        Token(conn_id) => {
                            // debug!("poll thread_id {}: {}/{} conn_id {}", thread_id, index + 1, events.events.len(), conn_id);
                            let mut remove_conn = false;
                            try_read_and_process(&thread_data.connections, &storage, false, record_stats, cache, &mut remove_conn, thread_id, conn_id);
                            if remove_conn {
                                // warn!("remove_conn2 {}", conn_id);
                                thread_data.connections.lock().remove(&conn_id);
                            }
                        }
                    }
                }
            }
        }));
    }

    thread::sleep(Duration::from_secs(std::u64::MAX));
}

fn try_read_and_process(connections: &spin::Mutex<HashMap<usize, Connection>>, storage: &Arc<RwLock<storage::Storage>>, after_accept: bool, record_stats: bool, cache: bool, remove_conn: &mut bool, thread_id: usize, conn_id: usize) {
    let mut full_request: Option<Vec<u8>> = None;
    if let Some(conn) = connections.lock().get_mut(&conn_id) {
        match try_read(conn, &storage, after_accept, record_stats) {
            Ok(new_data) => {
                if new_data {
                    let request = conn.buf[0..conn.len].to_vec(); // TODO avoid clone
                    match can_process_request(request.as_slice()) {
                        Ok(can_process) => if can_process {
                            full_request = Some(request);
                        },
                        Err(status_code) => {
                            send_response(&status_response2(status_code), conn, remove_conn, &storage);
                        }
                    };
                } else {}
            }
            Err(_err) => {
                *remove_conn = true;
            }
        }
    }
    if full_request.is_some() {
        let result = process_request(full_request.unwrap().as_slice(), &storage, record_stats, cache, thread_id, conn_id, &mut |body: Result<Cow<[u8]>, StatusCode>| {
            let storage = storage.clone();
            let response = match body {
                Ok(body) => "HTTP/1.1 200 ?\r\n".to_string() +
                    &COMMON_HEADERS_AS_STR +
                    "content-length: " + &body.len().to_string() + "\r\n\r\n" +
                    std::str::from_utf8(&body).expect("from_utf8(&body)"),
                Err(status_code) => status_response2(status_code)
            };
            if let Some(conn) = connections.lock().get_mut(&conn_id) {
                send_response(&response, conn, remove_conn, &storage);
            }
        });
        if result.is_err() {
            if let Some(conn) = connections.lock().get_mut(&conn_id) {
                send_response(&status_response2(result.unwrap_err()), conn, remove_conn, &storage);
            }
        }
    }
}

fn send_response(response: &String, conn: &mut Connection, remove_conn: &mut bool, storage: &Arc<RwLock<Storage>>) {
    conn.len = 0;
    match conn.stream.write_bufs(&[response.as_bytes().into()]) {
        Ok(len) => {
//            debug!("write {}", len);
            if len != response.len() {
                error!("failed to write full result");
                panic!("failed to write full result"); // TODO
            }
        }
        Err(err) => {
            // TODO WouldBlock ?
            error!("write error: {}", err);
            storage.read().expect("storage.read()").stats.register_write_error(err.kind());
            *remove_conn = true;
        }
    }
}

// based on mio
fn bind(addr: &SocketAddr) -> io::Result<TcpListener> {
    let tcp_builder = TcpBuilder::new_v4()?;

    tcp_builder.reuse_address(true)?;
    #[cfg(unix)]
        tcp_builder.reuse_port(true)?;

    tcp_builder.bind(addr)?;

    let listener = tcp_builder.listen(1024)?;
    TcpListener::from_std(listener)
}

fn try_read(conn: &mut Connection, storage: &Arc<RwLock<storage::Storage>>, after_accept: bool, record_stats: bool) -> Result<bool, io::Error> {
    let mut new_data = false;
    loop {
        match conn.stream.read_bufs(&mut [IoVec::from_bytes_mut(&mut conn.buf[conn.len..]).expect("IoVec::from_bytes_mut")]) {
            Ok(len2) => {
//                debug!("{}+{}", conn.len, len2);
                if len2 == 0 {
                    return Ok(new_data);
                }
                new_data = true;
                if record_stats {
                    if after_accept {
                        storage.read().expect("storage.read()").stats.register_accept_and_read();
                    } else {
                        storage.read().expect("storage.read()").stats.register_read();
                    }
                }
                conn.len += len2;
            }
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
//                debug!("read WouldBlock: {}", err);
                    return Ok(new_data);
                } else {
                    error!("read error: {}", err);
                    storage.read().expect("storage.read()").stats.register_read_error(err.kind());
                    return Err(err);
                }
            }
        }
    }
}

fn status_response2(status_code: StatusCode) -> String {
    "HTTP/1.1 ".to_string() + status_code.as_str() + " ?\r\n" +
        &COMMON_HEADERS_AS_STR +
        "content-length: 0\r\n\r\n"
}

fn can_process_request(request: &[u8]) -> Result<bool, StatusCode> {
    // TODO from_utf8_unchecked
    // TODO для этой функции не нужны строки
    let request = std::str::from_utf8(request).or_else(|_| Err(StatusCode::BAD_REQUEST))?;
    let (head, body) = match request.find("\r\n\r\n") {
        Some(index0) => (
            request[..index0].trim(), // почему-то в POST был перевод каретки в начале сообщения
            &request[index0 + 4..]
        ),
        None => return Ok(false),
    };
//    debug!("head {}", head);
//    debug!("body {}", body);
    if head.starts_with("GET ") {
        return Ok(true);
    }
    if !head.starts_with("POST ") {
        error!("only GET and POST are supported: #{}#", head);
        return Err(StatusCode::BAD_REQUEST);
    }
    for line in head.split("\n") {
//        debug!("line {}", line);
        if line.contains("Content-Length") {
            let index = line.find(':').ok_or_else(|| {
                error!("bad content-length: {}", line);
                StatusCode::BAD_REQUEST
            })?;
            let value = line[index + 1..].trim();
//            debug!("value {}", value);
            let length = value.parse::<usize>().or_else(|_| {
                error!("bad content-length: {}", line);
                Err(StatusCode::BAD_REQUEST)
            })?;
//            debug!("{} -> {} {}", line, length, body.len());
            if length < body.len() && body[length..].trim() != "" {
                error!("extra content: {}", &body[length..]);
            }
            return Ok(length <= body.len());
        }
    }
    Ok(false)
}

fn process_request<RF: FnMut(Result<Cow<[u8]>, StatusCode>)>(request: &[u8], storage: &Arc<RwLock<storage::Storage>>, record_stats: bool, cache: bool, thread_id: usize, conn_id: usize, resp_f: RF) -> Result<(), StatusCode> {
    let (path, query, body) = parse_request(request)?;
    process::process(path, query, body, storage, record_stats, cache, thread_id, conn_id, resp_f)
//    Err(StatusCode::BAD_REQUEST)
}

fn parse_request(request: &[u8]) -> Result<(&str, Option<&str>, Option<&[u8]>), StatusCode> {
    // TODO from_utf8_unchecked
    // TODO для этой функции не нужны строки
    let request = std::str::from_utf8(request).or_else(|_| Err(StatusCode::BAD_REQUEST))?;
//    debug!("request: {}: {}", request.len(), percent_encode(request.as_bytes(), DEFAULT_ENCODE_SET).to_string());
    let request = request.trim_start();
    let index0 = request.find("\r\n").ok_or_else(|| {
        error!("bad request (first line 1): {}", request);
        StatusCode::BAD_REQUEST
    })?;
    let line = &request[..index0];
//    debug!("line: {}", line);
    let index1 = line.find(' ').ok_or_else(|| {
        error!("bad request (first line 2): {}", percent_encode(request.as_bytes(), DEFAULT_ENCODE_SET).to_string());
        error!("bad request (first line 2): {}", line);
        StatusCode::BAD_REQUEST
    })?;
    let index2 = line.rfind(' ').ok_or_else(|| {
        error!("bad request (first line 3): {}", request);
        error!("bad request (first line 3): {}", line);
        StatusCode::BAD_REQUEST
    })?;
    let url = &line[index1 + 1..index2];
//    debug!("url: {}", url);
    let index3 = url.find('?').ok_or(StatusCode::NOT_FOUND)?;
    let path = &url[0..index3];
//    debug!("path: {}", path);
    let query = Some(&url[index3 + 1..]);
//    debug!("query: {}", query.unwrap());
    let index4 = match request.find("\r\n\r\n") {
        Some(index) => index + 4,
        None => {
            error!("bad request (head -> body): {}", request);
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    let body = if index4 == request.len() { None } else { Some(&request[index4..]) };
    if body.is_some() {
//        debug!("body: {}", body.unwrap());
    } else {
//        debug!("body empty");
    }
    Ok((path, query, body.map(|b| b.as_bytes())))
}

fn poll(poll: &mio::Poll, events: &mut Events) {
    #[cfg(not(target_os = "linux"))]
        poll.poll(events, Some(Duration::from_secs(0))).unwrap();

    #[cfg(target_os = "linux")]
        {
            // based on mio
            // TODO syscall?

            use libc::{self};
            use std::os::unix::io::AsRawFd;

            events.events.clear();
            unsafe {
                let cnt = libc::epoll_wait(poll.as_raw_fd(),
                                           events.events.as_mut_ptr(),
                                           events.events.capacity() as i32,
                                           0);
                if cnt == -1 {
                    panic!("epoll_wait error");
                }
                let cnt = cnt as usize;
                events.events.set_len(cnt);

//                for i in 0..cnt {
//                    if events.events[i].u64 as usize == usize::MAX {
//                        events.events.remove(i);
//                        return;
//                    }
//                }
            }
        }
}

struct Connection {
    stream: TcpStream,
    buf: [u8; 8192],
    len: usize,
//    result: Vec<u8>,
}

struct ThreadData {
    server: TcpListener,
    poll: Poll,
    connections: spin::Mutex<HashMap<usize, Connection>>,
}

#[cfg(target_os = "linux")]
pub struct Events {
    // based on mio
    events: Vec<libc::epoll_event>,
}

#[cfg(target_os = "linux")]
impl Events {
    pub fn with_capacity(capacity: usize) -> Events {
        Events {
            events: Vec::with_capacity(capacity)
        }
    }

    pub fn iter(&self) -> EventIter {
        EventIter {
            inner: self,
            pos: 0,
        }
    }

    fn get(&self, pos: usize) -> Option<Event> {
        use libc::c_int;
        use libc::{EPOLLOUT, EPOLLIN, EPOLLPRI, EPOLLERR, EPOLLHUP, EPOLLRDHUP};
        use mio::unix::UnixReady;

        self.events.get(pos).map(|event| {
            let epoll = event.events as c_int;
            let mut kind = Ready::empty();

            if (epoll & EPOLLIN) != 0 {
                kind = kind | Ready::readable();
            }

            if (epoll & EPOLLPRI) != 0 {
                kind = kind | Ready::readable() | UnixReady::priority();
            }

            if (epoll & EPOLLOUT) != 0 {
                kind = kind | Ready::writable();
            }

            // EPOLLHUP - Usually means a socket error happened
            if (epoll & EPOLLERR) != 0 {
                kind = kind | UnixReady::error();
            }

            if (epoll & EPOLLRDHUP) != 0 || (epoll & EPOLLHUP) != 0 {
                kind = kind | UnixReady::hup();
            }

            let token = self.events[pos].u64;

            Event::new(kind, Token(token as usize))
        })
    }
}

#[cfg(target_os = "linux")]
pub struct EventIter<'a> {
    inner: &'a Events,
    pos: usize,
}

#[cfg(target_os = "linux")]
impl<'a> Iterator for EventIter<'a> {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}
