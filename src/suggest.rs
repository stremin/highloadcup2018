use std::collections::HashMap;
use std::i64;

use crate::storage::Account;
use crate::storage::AccountJson;
use crate::storage::AccountsJson;
use crate::storage::Like;
use crate::storage::Storage;
use crate::utils::EMPTY_LIKE_LIST;
use crate::utils::insert_into_sorted_vec;
use crate::utils::StatusCode;

#[inline(never)]
pub fn suggest(storage: &Storage, id: i32, params: &Vec<(String, String)>) -> Result<AccountsJson, StatusCode> {
    let person = storage.accounts[id as usize].as_ref().ok_or(StatusCode::NOT_FOUND)?;
    if person.sex == 0 {
        Err(StatusCode::BAD_REQUEST)?;
    }
    let matcher = match make_matcher(storage, &params)? {
        Some(matcher) => matcher,
        None => return Ok(AccountsJson { accounts: Vec::new() })
    };

    if person.likes.is_empty() {
        return Ok(AccountsJson { accounts: Vec::new() });
    }

//    debug!("person: {:?}", person);

    let likes_index = if person.sex == storage.consts.male { &storage.indexes.likes_index_male } else { &storage.indexes.likes_index_female };

    let mut map: HashMap<i32, f64> = HashMap::with_capacity(1000);
    person.likes.iter().for_each(|id| {
        let vec = merge_multiple_likes(likes_index.get(id).unwrap_or(&EMPTY_LIKE_LIST));
        let mut ts = None;
        for like2 in &vec {
            if like2.id == person.id {
                ts = Some(like2.ts);
                break;
            }
        }
        let ts = ts.unwrap();
        for like2 in &vec {
            if like2.id != person.id {
                let similarity = map.entry(like2.id).or_insert(0.0);
                let diff = (ts - like2.ts).abs();
                *similarity += if diff == 0 { 1.0 } else { 1.0 / diff as f64 };
            }
        }
    });

    let mut similar_likes: Vec<SimilarLikes> = map.iter().filter(|(_, v)| **v > 0.0).map(|(k, v)| SimilarLikes { id: *k, similarity: *v }).collect();
    similar_likes.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap().then(a.id.cmp(&b.id)));
//    debug!("similar_likes: {:?}", similar_likes);

    let mut known_ids = Vec::<i32>::new();
    Ok(AccountsJson {
        accounts: similar_likes.iter()
            .filter_map(|similar_like| {
//                debug!("account {} sim {}: {:?}", similar_like.id, similar_like.similarity, &storage.accounts[similar_like.id as usize]);
                storage.accounts[similar_like.id as usize].as_ref()
            })
            .filter(|account| account.sex == person.sex && matches(account, &matcher))
            .map(|account| get_new_likes(&person.likes, &account.likes))
            .flat_map(|new_likes| {
//                debug!("new_likes {:?}", new_likes.iter().rev().cloned().collect::<Vec<i32>>());
                new_likes.into_iter().rev()
            })
            .filter(|id| {
                if !known_ids.contains(id) {
                    known_ids.push(*id);
                    true
                } else {
                    false
                }
            })
            .filter_map(|id| storage.accounts[id as usize].as_ref())
            .map(|account| AccountJson {
                id: Some(account.id),
                email: account.email.as_ref().map(|email| email.clone()),
                status: storage.dict.get_value(account.status),
                sname: storage.dict.get_value(account.sname),
                fname: storage.dict.get_value(account.fname),
                phone: None,
                sex: None,
                birth: None,
                country: None,
                city: None,
                joined: None,
                interests: Vec::new(),
                likes: Vec::new(),
                premium: None,

            })
            .take(matcher.limit)
            .collect()
    })
}

fn make_matcher(storage: &Storage, params: &Vec<(String, String)>) -> Result<Option<Matcher>, StatusCode> {
    let mut matcher = Matcher {
        limit: 0,
        country: 0,
        city: 0,
    };

    let mut empty_result = false;

    for (key, value) in params {
        match key.as_str() {
            "query_id" => {}
            "limit" => {
                matcher.limit = value.parse::<usize>().map_err(|_| StatusCode::BAD_REQUEST)?;
                if matcher.limit == 0 {
                    return Err(StatusCode::BAD_REQUEST);
                }
            }
            "country" => {
                if value.is_empty() {
                    Err(StatusCode::BAD_REQUEST)?
                }
                matcher.country = storage.dict.get_existing_key(value).unwrap_or(0);
                if matcher.country == 0 {
                    empty_result = true;
                }
            }
            "city" => {
                if value.is_empty() {
                    Err(StatusCode::BAD_REQUEST)?
                }
                matcher.city = storage.dict.get_existing_key(value).unwrap_or(0);
                if matcher.city == 0 {
                    empty_result = true;
                }
            }
            _ => return Err(StatusCode::BAD_REQUEST)
        }
    }
    if empty_result {
        return Ok(None);
    }
    Ok(Some(matcher))
}

fn matches(account: &Account, matcher: &Matcher) -> bool {
    if matcher.country != 0 && account.country != matcher.country {
        return false;
    }
    if matcher.city != 0 && account.city != matcher.city {
        return false;
    }
    return true;
}

fn merge_multiple_likes(likes: &Vec<Like>) -> Vec<Like> {
    if likes.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();

    let mut id = likes[0].id;
    let mut ts_sum = likes[0].ts as i64;
    let mut count = 1;
    for like in &likes[1..] {
        if like.id != id {
            result.push(Like { id, ts: (ts_sum / count) as i32 });
            id = like.id;
            ts_sum = like.ts as i64;
            count = 1;
        } else {
            ts_sum += like.ts as i64;
            count += 1;
        }
    }
    result.push(Like { id, ts: (ts_sum / count) as i32 });

//    if !crate::utils::vec_compare(likes, &result) {
//        debug!("original: {:?}", likes);
//        debug!("merged  : {:?}", &result);
//    }

    result
}

fn get_new_likes(my_likes: &Vec<i32>, other_likes: &Vec<i32>) -> Vec<i32> {
    let mut new_likes = Vec::new();
    let mut pos1 = 0;
    let mut pos2 = 0;
    while pos2 < other_likes.len() {
        if pos1 < my_likes.len() && my_likes[pos1] < other_likes[pos2] {
            pos1 += 1;
        } else if pos1 >= my_likes.len() || my_likes[pos1] > other_likes[pos2] {
            insert_into_sorted_vec(other_likes[pos2], &mut new_likes);
            pos2 += 1;
        } else {
            let like_id = my_likes[pos1];
            while pos1 < my_likes.len() && my_likes[pos1] == like_id {
                pos1 += 1;
            }
            while pos2 < other_likes.len() && other_likes[pos2] == like_id {
                pos2 += 1;
            }
        }
    }
    new_likes
}

#[derive(Debug)]
struct Matcher {
    limit: usize,
    country: i32,
    city: i32,
}

#[derive(Debug)]
struct SimilarLikes {
    id: i32,
    similarity: f64,
}