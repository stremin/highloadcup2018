use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::Iterator;
use std::sync::{Arc, RwLock};
//use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::Instant;

use percent_encoding::percent_decode;
use regex::Regex;
use spin;

use crate::filter;
use crate::group;
use crate::recommend;
use crate::storage::Storage;
use crate::suggest;
use crate::utils::StatusCode;

lazy_static! {
    static ref CACHE: spin::Mutex<HashMap<String, Vec<u8>>> = spin::Mutex::new(HashMap::new());
}

pub fn process<RF: FnMut(Result<Cow<[u8]>, StatusCode>)>(path: &str, query: Option<&str>, body: Option<&[u8]>, storage: &Arc<RwLock<Storage>>, record_stats: bool, cache: bool, _thread_id: usize, _conn_id: usize, mut resp_f: RF) -> Result<(), StatusCode> {
//    static REQUEST_COUNT: AtomicUsize = AtomicUsize::new(0);
//    let count = REQUEST_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
//    if count >= 0 && count < 700 {
//        debug!("tid {} cid {} count {} {}?{}", _thread_id, _conn_id, count, path, query.unwrap_or(""));
//    }

    lazy_static! {
        static ref URL_RE: Regex = Regex::new(r"^/accounts/(?:(filter)|(group)|(\d+)/recommend|(\d+)/suggest|(new)|(\d+)|(likes))/?$").unwrap();
    }

    let caps = URL_RE.captures(path);
//    debug!("{:?}", caps);

//    debug!("{:?}", head.uri.query());
//    debug!("{:?}", parse_query(head.uri.query().unwrap()));

    if caps.is_some() {
        let params = parse_query(query.unwrap());

        let caps2 = caps.unwrap();
        if caps2.get(1).is_some() {
            // filter
            execute_with_cache("FILTER", "FILTER_CACHED", storage, &params, record_stats, cache, resp_f,
                               || "F:".to_string() + query.unwrap_or(""),
                               || filter::filter(&storage.read().unwrap(), &params),
                               |r| serde_json::to_vec(r).unwrap(),
            )?;
            return Ok(());
        } else if caps2.get(2).is_some() {
            // group
            execute_with_cache("GROUP", "GROUP_CACHED", storage, &params, record_stats, cache, resp_f,
                               || "G:".to_string() + query.unwrap_or(""),
                               || group::group(&storage.read().unwrap(), &params),
                               |r| serde_json::to_vec(r).unwrap(),
            )?;
            return Ok(());
        } else if caps2.get(3).is_some() {
            // recommend
            let id = caps2.get(3).unwrap().as_str().parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
            execute_with_cache("RECOMMEND", "RECOMMEND_CACHED", storage, &params, record_stats, cache, resp_f,
                               || "R:".to_string() + &id.to_string() + ":" + query.unwrap_or(""),
                               || recommend::recommend(&storage.read().unwrap(), id, &params),
                               |r| serde_json::to_vec(r).unwrap(),
            )?;
            return Ok(());
        } else if caps2.get(4).is_some() {
            // suggest
            let id = caps2.get(4).unwrap().as_str().parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
            execute_with_cache("SUGGEST", "SUGGEST_CACHED", storage, &params, record_stats, cache, resp_f,
                               || "S:".to_string() + &id.to_string() + ":" + query.unwrap_or(""),
                               || suggest::suggest(&storage.read().unwrap(), id, &params),
                               |r| serde_json::to_vec(r).unwrap(),
            )?;
            return Ok(());
        } else if caps2.get(5).is_some() {
            // new
            let start = if record_stats { Some(Instant::now()) } else { None };
            let mut elapsed_early: Option<Duration> = None;
            let result = storage.write().unwrap().new_account(body.unwrap(), &mut |status_code| {
                if record_stats {
                    elapsed_early = Some(start.unwrap().elapsed());
                }
                resp_f(Err(status_code));
            });
            CACHE.lock().clear();
            if record_stats {
                if elapsed_early.is_some() {
                    &storage.read().unwrap().stats.register("NEW_EARLY", elapsed_early.unwrap(), &params);
                }
                &storage.read().unwrap().stats.register("NEW", start.unwrap().elapsed(), &params);
            }
            if result.is_err() {
                resp_f(Err(result.unwrap_err()));
            }
            return Ok(());
        } else if caps2.get(6).is_some() {
            // update
            let id = caps2.get(6).unwrap().as_str().parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
            let start = if record_stats { Some(Instant::now()) } else { None };
            let mut elapsed_early: Option<Duration> = None;
            let result = storage.write().unwrap().update_account(id, body.unwrap(), &mut |status_code| {
                if record_stats {
                    elapsed_early = Some(start.unwrap().elapsed());
                }
                resp_f(Err(status_code));
            });
            CACHE.lock().clear();
            if record_stats {
                if elapsed_early.is_some() {
                    &storage.read().unwrap().stats.register("UPDATE_EARLY", elapsed_early.unwrap(), &params);
                }
                &storage.read().unwrap().stats.register("UPDATE", start.unwrap().elapsed(), &params);
            }
            if result.is_err() {
                resp_f(Err(result.unwrap_err()));
            }
            return Ok(());
        } else if caps2.get(7).is_some() {
            // likes
            let start = if record_stats { Some(Instant::now()) } else { None };
            let mut elapsed_early: Option<Duration> = None;
            let result = storage.write().unwrap().update_likes(body.unwrap(), &mut |status_code| {
                if record_stats {
                    elapsed_early = Some(start.unwrap().elapsed());
                }
                resp_f(Err(status_code));
            });
            CACHE.lock().clear();
            if record_stats {
                if elapsed_early.is_some() {
                    &storage.read().unwrap().stats.register("LIKES_EARLY", elapsed_early.unwrap(), &params);
                }
                &storage.read().unwrap().stats.register("LIKES", start.unwrap().elapsed(), &params);
            }
            if result.is_err() {
                resp_f(Err(result.unwrap_err()));
            }
            return Ok(());
        }
    }
    Err(StatusCode::NOT_FOUND)
}

fn execute_with_cache<R, RF, CF, PF, MRF>(name: &'static str, name_cache: &'static str, storage: &Arc<RwLock<Storage>>, params: &Vec<(String, String)>, record_stats: bool, cache: bool, mut resp_f: RF, cache_key_f: CF, process_f: PF, make_response_f: MRF) -> Result<(), StatusCode>
    where RF: FnMut(Result<Cow<[u8]>, StatusCode>), CF: FnOnce() -> String, PF: FnOnce() -> Result<R, StatusCode>, MRF: FnOnce(&R) -> Vec<u8> {

    let start = if record_stats { Some(Instant::now()) } else { None };
    let cache_key: String;
    if cache {
        cache_key = cache_key_f();
        if let Some(response) = CACHE.lock().get(&cache_key) {
            resp_f(Ok(Cow::from(response)));
            if record_stats {
                &storage.read().unwrap().stats.register(name_cache, start.unwrap().elapsed(), &params);
            }
            return Ok(());
        }
    } else {
        cache_key = String::new();
    }
    let process_result: R = process_f()?;
    if record_stats {
        &storage.read().unwrap().stats.register(name, start.unwrap().elapsed(), &params);
    }
    let response = make_response_f(&process_result);
    resp_f(Ok(Cow::from(&response)));
    if cache {
        CACHE.lock().insert(cache_key, response);
    }
    Ok(())
}

fn parse_query(query: &str) -> Vec<(String, String)> { // TODO avoid String creation
    query.split('&').map(|part: &str| match part.find('=') {
        Some(index) => (decode_query_part(&part[0..index]), decode_query_part(&part[index + 1..])),
        None => (decode_query_part(&part), String::new())
    }).collect()
}

fn decode_query_part(str: &str) -> String {
    percent_decode(str.replace("+", " ").as_bytes()).decode_utf8().unwrap().to_string() // TODO faster replace?
}
