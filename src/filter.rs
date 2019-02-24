use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::free::kmerge;
use itertools::Itertools;
use itertools::kmerge_by;

use crate::bits::Bits;
use crate::storage;
use crate::storage::Account;
use crate::storage::AccountJson;
use crate::storage::AccountsJson;
use crate::storage::NULL_DATE;
use crate::storage::Premium;
use crate::storage::Storage;
use crate::utils::EMPTY_INT_LIST;
use crate::utils::EMPTY_LIKE_LIST;
use crate::utils::KeySet;
use crate::utils::retain_all_sorted;
use crate::utils::seconds_from_year;
use crate::utils::StatusCode;

#[derive(Clone, Debug)]
enum Mode {
    FastInterests,
    Standard,
}

impl Copy for Mode {}

lazy_static! {
    static ref keys_to_filter_mode: HashMap<KeySet, Mode> = {
        let mut map: HashMap<KeySet, Mode> = HashMap::new();
        map.insert(KeySet::new(&vec!["interests_contains", "sex_eq"]), Mode::FastInterests);
        map.insert(KeySet::new(&vec!["interests_contains", "sex_eq", "status_eq"]), Mode::FastInterests);
        map.insert(KeySet::new(&vec!["interests_contains", "sex_eq", "status_neq"]), Mode::FastInterests);
        map
    };
}

#[inline(never)]
pub fn filter(storage: &Storage, params: &Vec<(String, String)>) -> Result<AccountsJson, StatusCode> {
    let matcher = match make_matcher(storage, &params)? {
        Some(matcher) => matcher,
        None => return Ok(AccountsJson { accounts: Vec::new() })
    };

    Ok(try_fast_index(storage, &matcher)
        .or_else(|| try_index(storage, &matcher))
        .or_else(|| Some(full_scan(storage, &matcher)))
        .unwrap())
}

#[inline(never)]
fn try_fast_index(storage: &Storage, matcher: &Matcher) -> Option<AccountsJson> {
    match storage.indexes.filter_index.get_result(&matcher) {
        Some(ids) =>
            Some(AccountsJson {
                accounts: ids.iter().rev()
                    .filter_map(|id| storage.accounts[*id as usize].as_ref())
                    .filter(|account| matches(*account, &matcher, storage))
                    .map(|account| {
                        make_result(storage, &matcher, account)
                    })
                    .take(matcher.limit)
                    .collect()
            }),
        None => None
    }
}

#[inline(never)]
fn try_index(storage: &Storage, matcher: &Matcher) -> Option<AccountsJson> {
    let (interest1, interest2) = match &matcher.interests_contains {
        Some(interests_contains) => {
            let mut iter = interests_contains.into_iter();
            (iter.next(), iter.next())
        }
        None => (None, None)
    };

    if !matcher.likes_contains.is_empty() {
        let mut vec: Option<Vec<i32>> = None;
//        let like = matcher.likes_contains[0];
//        vec = Some(storage.indexes.likes_index_male.get(&like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id)
//            .merge(storage.indexes.likes_index_female.get(&like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id))
//            .dedup()
//            .collect());
        for like in &matcher.likes_contains {
            let vec3 =
                storage.indexes.likes_index_male.get(&like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id)
                    .merge(storage.indexes.likes_index_female.get(&like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id))
                    .dedup()
                    .collect();
            match vec.as_mut() {
                None => vec = Some(vec3),
                Some(mut ids) => retain_all_sorted(&mut ids, &vec3),
            }
        }
        Some(process_rev_iter(vec.unwrap().iter().rev(), storage, matcher))
    } else if interest1.is_some() && interest2.is_some() {
        let interest1 = interest1.unwrap();
        let interest2 = interest2.unwrap();
        let key = if interest1 < interest2 { (interest1, interest2) } else { (interest2, interest1) };
        Some(process_rev_iter(storage.indexes.interests2_index.get(&key).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
    } else if matcher.city != 0 {
        Some(process_rev_iter(storage.indexes.city_index.get(&matcher.city).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
    } else if !matcher.city_any.is_empty() {
        Some(process_rev_iter(kmerge_by(matcher.city_any.iter().map(|city| storage.indexes.city_index.get(&city).unwrap_or(&EMPTY_INT_LIST).iter().rev()), rev_id).dedup(), storage, matcher))
    } else if let Some(interest) = interest1 {
        if matcher.sex != 0 {
            let interests_index = if matcher.sex == storage.consts.male { &storage.indexes.interests_index_male } else { &storage.indexes.interests_index_female };
            Some(process_rev_iter(interests_index.get(&interest).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
        } else {
            Some(process_rev_iter(storage.indexes.interests_index.get(&interest).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
        }
    } else if matcher.country != 0 {
        Some(process_rev_iter(storage.indexes.country_index.get(&matcher.country).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
    } else if matcher.birth_year != 0 {
        Some(process_rev_iter(storage.indexes.birth_index.get(&matcher.birth_year).unwrap_or(&EMPTY_INT_LIST).iter().rev(), storage, matcher))
    } else if !matcher.fname_any.is_empty() {
        Some(process_rev_iter(kmerge_by(matcher.fname_any.iter().map(|fname| storage.indexes.fname_index.get(&fname).unwrap_or(&EMPTY_INT_LIST).iter().rev()), rev_id).dedup(), storage, matcher))
    } else if matcher.interests_any.is_some() {
        Some(process_rev_iter(kmerge_by(matcher.interests_any.as_ref().unwrap().into_iter().map(|interest| storage.indexes.interests_index.get(&interest).unwrap_or(&EMPTY_INT_LIST).iter().rev()), rev_id).dedup(), storage, matcher))
    } else {
        None
    }
}

fn rev_id(a: &&i32, b: &&i32) -> bool {
    a > b
}

fn process_rev_iter<'a, I>(iter: I, storage: &Storage, matcher: &Matcher) -> AccountsJson
    where I: Iterator<Item=&'a i32> {
    AccountsJson {
        accounts: iter
            .filter_map(|id| storage.accounts[*id as usize].as_ref())
            .filter(|account| matches(account, &matcher, storage))
            .map(|account| {
                make_result(storage, &matcher, account)
            })
            .take(matcher.limit)
            .collect()
    }
}

#[inline(never)]
fn full_scan(storage: &Storage, matcher: &Matcher) -> AccountsJson {
    AccountsJson {
        accounts: (0..storage.max_id + 1).rev()
            .filter_map(|id| storage.accounts[id].as_ref())
            .filter(|account| matches(account, &matcher, storage))
            .map(|account| {
                make_result(storage, &matcher, account)
            })
            .take(matcher.limit)
            .collect()
    }
}

fn make_matcher(storage: &storage::Storage, params: &Vec<(String, String)>) -> Result<Option<Matcher>, StatusCode> {
    let mut matcher = Matcher {
        limit: 0,
        conditions: Vec::new(),
        mode: Mode::Standard,

        sex: 0,
        email_domain: None,
        email_lt: None,
        email_gt: None,
        status_eq: 0,
        status_neq: 0,
        fname: 0,
        fname_any: Vec::new(),
        fname_null0: false,
        fname_null1: false,
        sname: 0,
        sname_starts: None,
        sname_null0: false,
        sname_null1: false,
        phone_code: 0,
        phone_null0: false,
        phone_null1: false,
        country: 0,
        country_null0: false,
        country_null1: false,
        city: 0,
        city_any: Vec::new(),
        city_null0: false,
        city_null1: false,
        birth_lt: NULL_DATE,
        birth_gt: NULL_DATE,
        birth_from: NULL_DATE,
        birth_to: NULL_DATE,
        birth_year: 0,
        interests_contains: None,
        interests_any: None,
        likes_contains: Vec::new(),
        premium_now: false,
        premium_null0: false,
        premium_null1: false,
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
            _ => {
                match key.as_str() {
                    "sex_eq" => {
                        matcher.sex = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.sex == 0 {
                            empty_result = true;
                        }
                    }
                    "email_domain" => {
                        // TODO check domain exists?
                        matcher.email_domain = Some("@".to_string() + value);
                    }
                    "email_lt" => {
                        matcher.email_lt = Some(value.clone());
                    }
                    "email_gt" => {
                        matcher.email_gt = Some(value.clone());
                    }
                    "status_eq" => {
                        matcher.status_eq = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.status_eq == 0 {
                            empty_result = true;
                        }
                    }
                    "status_neq" => {
                        matcher.status_neq = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.status_neq == 0 {
                            empty_result = true;
                        }
                    }
                    "fname_eq" => {
                        matcher.fname = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.fname == 0 {
                            empty_result = true;
                        }
                    }
                    "fname_any" => {
                        matcher.fname_any = value.split(',').map(|v| storage.dict.get_existing_key(&v.to_string()).unwrap_or(0)).collect();
                    }
                    "fname_null" => {
                        match value.as_str() {
                            "0" => matcher.fname_null0 = true,
                            "1" => matcher.fname_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "sname_eq" => {
                        matcher.sname = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.sname == 0 {
                            empty_result = true;
                        }
                    }
                    "sname_starts" => {
                        matcher.sname_starts = Some(value.clone());
                    }
                    "sname_null" => {
                        match value.as_str() {
                            "0" => matcher.sname_null0 = true,
                            "1" => matcher.sname_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "phone_code" => {
                        matcher.phone_code = value.parse().or_else(|_| Err(StatusCode::BAD_REQUEST))?;
                    }
                    "phone_null" => {
                        match value.as_str() {
                            "0" => matcher.phone_null0 = true,
                            "1" => matcher.phone_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "country_eq" => {
                        matcher.country = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.country == 0 {
                            empty_result = true;
                        }
                    }
                    "country_null" => {
                        match value.as_str() {
                            "0" => matcher.country_null0 = true,
                            "1" => matcher.country_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "city_eq" => {
                        matcher.city = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.city == 0 {
                            empty_result = true;
                        }
                    }
                    "city_any" => {
                        matcher.city_any = value.split(',').map(|v| storage.dict.get_existing_key(&v.to_string()).unwrap_or(0)).collect();
                    }
                    "city_null" => {
                        match value.as_str() {
                            "0" => matcher.city_null0 = true,
                            "1" => matcher.city_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "birth_lt" => {
                        matcher.birth_lt = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                    }
                    "birth_gt" => {
                        matcher.birth_gt = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                    }
                    "birth_year" => {
                        matcher.birth_year = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                        matcher.birth_from = seconds_from_year(matcher.birth_year);
                        matcher.birth_to = seconds_from_year(matcher.birth_year + 1);
                    }
                    "interests_contains" => {
                        let vec: Vec<i32> = value.split(',').map(|v| storage.interest_dict.get_existing_key(&v.to_string()).unwrap_or(0)).collect();
                        if vec.contains(&0) {
                            empty_result = true;
                        }
                        matcher.interests_contains = Some(Bits::from_vec(vec));
                    }
                    "interests_any" => {
                        let vec = value.split(',').map(|v| storage.interest_dict.get_existing_key(&v.to_string()).unwrap_or(0)).collect();
                        matcher.interests_any = Some(Bits::from_vec(vec));
                    }
                    "likes_contains" => {
                        // https://stackoverflow.com/questions/26368288/how-do-i-stop-iteration-and-return-an-error-when-iteratormap-returns-a-result
                        let parts: Result<Vec<_>, _> = value.split(',').map(|v| { v.parse::<i32>() }).collect();
                        matcher.likes_contains = parts.map_err(|_| StatusCode::BAD_REQUEST)?;
                        matcher.likes_contains.sort();
                        matcher.likes_contains.dedup();
                    }
                    "premium_now" => {
                        match value.as_str() {
                            "1" => matcher.premium_now = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    "premium_null" => {
                        match value.as_str() {
                            "0" => matcher.premium_null0 = true,
                            "1" => matcher.premium_null1 = true,
                            _ => return Err(StatusCode::BAD_REQUEST)
                        }
                    }
                    _ => return Err(StatusCode::BAD_REQUEST)
                };
                matcher.conditions.push(key.clone());
            }
        }
    }
    if empty_result {
        return Ok(None);
    }
    matcher.mode = *keys_to_filter_mode.get(&KeySet::new2(&matcher.conditions)).unwrap_or(&Mode::Standard);
    Ok(Some(matcher))
}

fn matches(account: &Account, matcher: &Matcher, storage: &Storage) -> bool {
    // TODO fast paths for popular combinations
    // TODO убрать, эффекта нет?
    match matcher.mode {
        Mode::FastInterests => {
            if matcher.sex != 0 && matcher.sex != account.sex {
                return false;
            }
            if matcher.status_eq != 0 && account.status != matcher.status_eq {
                return false;
            }
            if matcher.status_neq != 0 && account.status == matcher.status_neq {
                return false;
            }
            if matcher.interests_contains.is_some() {
                if account.interests.is_empty() {
                    return false;
                }
                if !account.interests.contains_all(matcher.interests_contains.as_ref().unwrap()) {
                    return false;
                }
            }
            return true;
        }
        Mode::Standard => {
            if matcher.sex != 0 && matcher.sex != account.sex {
                return false;
            }
            if matcher.email_domain.is_some() && !account.email.as_ref().unwrap().ends_with(matcher.email_domain.as_ref().unwrap()) {
                return false; // TODO dict?
            }
            if matcher.email_lt.is_some() && account.email.as_ref().unwrap().borrow() as &String >= matcher.email_lt.as_ref().unwrap() {
                return false;
            }
            if matcher.email_gt.is_some() && account.email.as_ref().unwrap().borrow() as &String <= matcher.email_gt.as_ref().unwrap() {
                return false;
            }
            if matcher.status_eq != 0 && account.status != matcher.status_eq {
                return false;
            }
            if matcher.status_neq != 0 && account.status == matcher.status_neq {
                return false;
            }
            if matcher.fname != 0 && account.fname != matcher.fname {
                return false;
            }
            if !matcher.fname_any.is_empty() && (account.fname == 0 || !matcher.fname_any.contains(&account.fname)) {
                return false;
            }
            if matcher.fname_null0 && account.fname == 0 {
                return false;
            }
            if matcher.fname_null1 && account.fname != 0 {
                return false;
            }
            if matcher.sname != 0 && account.sname != matcher.sname {
                return false;
            }
            if matcher.sname_starts.is_some() && (account.sname == 0 || !storage.dict.get_value(account.sname).as_ref().unwrap().starts_with(matcher.sname_starts.as_ref().unwrap())) {
                return false;
            }
            if matcher.sname_null0 && account.sname == 0 {
                return false;
            }
            if matcher.sname_null1 && account.sname != 0 {
                return false;
            }
            if matcher.phone_code != 0 && (account.phone_number == 0 || account.phone_code != matcher.phone_code) {
                return false;
            }
            if matcher.phone_null0 && account.phone_number == 0 {
                return false;
            }
            if matcher.phone_null1 && account.phone_number != 0 {
                return false;
            }
            if matcher.country != 0 && account.country != matcher.country {
                return false;
            }
            if matcher.country_null0 && account.country == 0 {
                return false;
            }
            if matcher.country_null1 && account.country != 0 {
                return false;
            }
            if matcher.city != 0 && account.city != matcher.city {
                return false;
            }
            if !matcher.city_any.is_empty() && (account.city == 0 || !matcher.city_any.contains(&account.city)) {
                return false;
            }
            if matcher.city_null0 && account.city == 0 {
                return false;
            }
            if matcher.city_null1 && account.city != 0 {
                return false;
            }
            if matcher.birth_lt != NULL_DATE && account.birth >= matcher.birth_lt {
                return false;
            }
            if matcher.birth_gt != NULL_DATE && account.birth <= matcher.birth_gt {
                return false;
            }
            if matcher.birth_year != 0 && (account.birth < matcher.birth_from || account.birth >= matcher.birth_to) {
                return false;
            }
            if matcher.interests_contains.is_some() {
                if account.interests.is_empty() {
                    return false;
                }
                if !account.interests.contains_all(matcher.interests_contains.as_ref().unwrap()) {
                    return false;
                }
            }
            if matcher.interests_any.is_some() {
                if account.interests.is_empty() {
                    return false;
                }
                if !account.interests.contains_any(&matcher.interests_any.as_ref().unwrap()) {
                    return false;
                }
            }
            if !matcher.likes_contains.is_empty() {
                if account.likes.is_empty() {
                    return false;
                }
                if matcher.likes_contains.iter().find(|id| !account.likes.contains(*id)).is_some() { // TODO binary?
                    return false;
                }
            }
            if matcher.premium_now && !account.is_premium {
                return false;
            }
            if matcher.premium_null0 && account.premium_start == NULL_DATE {
                return false;
            }
            if matcher.premium_null1 && account.premium_start != NULL_DATE {
                return false;
            }
            return true;
        }
    };
}

fn make_result(storage: &Storage, matcher: &Matcher, account: &Account) -> AccountJson {
    AccountJson {
        id: Some(account.id),
        email: account.email.as_ref().map(|email| email.clone()),
        sex: if matcher.sex != 0 { storage.dict.get_value(account.sex) } else { None },
        sname: if matcher.sname != 0 || matcher.sname_starts.is_some() || matcher.sname_null0 || matcher.sname_null1 {
            storage.dict.get_value(account.sname)
        } else {
            None
        },
        fname: if matcher.fname != 0 || !matcher.fname_any.is_empty() || matcher.fname_null0 || matcher.fname_null1 {
            storage.dict.get_value(account.fname)
        } else {
            None
        },
        phone: if (matcher.phone_code != 0 || matcher.phone_null0 || matcher.phone_null1) && account.phone_number != 0 {
            Some(Arc::new("8(".to_string() + account.phone_code.to_string().as_str() + ")" + &account.phone_number.to_string().as_str()[1..]))
        } else {
            None
        },
        birth: if matcher.birth_lt != NULL_DATE || matcher.birth_gt != NULL_DATE || matcher.birth_year != 0 {
            Some(account.birth)
        } else {
            None
        },
        country: if matcher.country != 0 || matcher.country_null0 || matcher.country_null1 {
            storage.dict.get_value(account.country)
        } else {
            None
        },
        city: if matcher.city != 0 || !matcher.city_any.is_empty() || matcher.city_null0 || matcher.city_null1 {
            storage.dict.get_value(account.city)
        } else {
            None
        },
        joined: None,
        status: if matcher.status_eq != 0 || matcher.status_neq != 0 { storage.dict.get_value(account.status) } else { None },
        interests: Vec::new(),
        likes: Vec::new(),
        premium: if (matcher.premium_now || matcher.premium_null0 || matcher.premium_null1) && account.premium_start != NULL_DATE {
            Some(Premium { start: account.premium_start, finish: account.premium_finish })
        } else {
            None
        },
    }
}

#[derive(Debug, Clone)]
pub struct Matcher {
    limit: usize,
    pub conditions: Vec<String>,
    mode: Mode,

    pub sex: i32,
    // включая @
    email_domain: Option<String>,
    pub email_lt: Option<String>,
    pub email_gt: Option<String>,
    pub status_eq: i32,
    pub status_neq: i32,
    fname: i32,
    pub fname_any: Vec<i32>,
    fname_null0: bool,
    fname_null1: bool,
    sname: i32,
    sname_starts: Option<String>,
    sname_null0: bool,
    sname_null1: bool,
    pub phone_code: i32,
    phone_null0: bool,
    phone_null1: bool,
    country: i32,
    country_null0: bool,
    pub country_null1: bool,
    city: i32,
    pub city_any: Vec<i32>,
    city_null0: bool,
    pub city_null1: bool,
    birth_lt: i32,
    birth_gt: i32,
    birth_from: i32,
    birth_to: i32,
    birth_year: i32,
    pub interests_contains: Option<Bits>,
    pub interests_any: Option<Bits>,
    // без дублей
    likes_contains: Vec<i32>,
    premium_now: bool,
    premium_null0: bool,
    premium_null1: bool,
}