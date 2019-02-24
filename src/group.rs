use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;

use crate::storage::Account;
use crate::storage::Storage;
use crate::topn::TopN;
use crate::utils::EMPTY_LIKE_LIST;
use crate::utils::seconds_from_year;
use crate::utils::StatusCode;

#[inline(never)]
pub fn group(storage: &Storage, params: &Vec<(String, String)>) -> Result<GroupsJson, StatusCode> {
    let matcher = match make_matcher(storage, &params)? {
        Some(matcher) => matcher,
        None => return Ok(GroupsJson { groups: Vec::new() })
    };

    let groups: HashMap<GroupKey, i32> = match storage.indexes.group_index.get_result(&matcher) {
        Some(groups) => groups,
        None => {
            let mut groups = HashMap::new();

            if matcher.like != 0 {
                storage.indexes.likes_index_male.get(&matcher.like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id)
                    .merge(storage.indexes.likes_index_female.get(&matcher.like).unwrap_or(&EMPTY_LIKE_LIST).iter().map(|like| like.id))
                    .dedup()
                    .filter_map(|id| storage.accounts[id as usize].as_ref())
                    .filter(|account| matches(account, &matcher))
                    .for_each(|account| process_group(account, &matcher, &mut groups));
            } else {
                // full scan
                (0..storage.max_id + 1)
                    .filter_map(|id| storage.accounts[id].as_ref())
                    .filter(|account| matches(account, &matcher))
                    .for_each(|account| process_group(account, &matcher, &mut groups));
            }
            groups
        }
    };

    let mut result: TopN<OrderedGroupJson> = TopN::new(matcher.limit);
    groups.iter().for_each(|(k, v)| {
        result.push(OrderedGroupJson {
            matcher: &matcher,
            group_json: GroupJson {
                sex: storage.dict.get_value(k.sex),
                status: storage.dict.get_value(k.status),
                country: storage.dict.get_value(k.country),
                city: storage.dict.get_value(k.city),
                interests: storage.interest_dict.get_value(k.interests),
                count: *v,
            },
        });
    });

    Ok(GroupsJson {
        groups: result.into_sorted_vec().into_iter()
            .map(|g| g.group_json)
            .collect()
    })
}

fn process_group(account: &Account, matcher: &Matcher, groups: &mut HashMap<GroupKey, i32>) {
    if matcher.group_interests {
        account.interests.into_iter().for_each(|interest| {
            let count = groups.entry(GroupKey {
                sex: if matcher.group_sex { account.sex } else { 0 },
                status: if matcher.group_status { account.status } else { 0 },
                country: if matcher.group_country { account.country } else { 0 },
                city: if matcher.group_city { account.city } else { 0 },
                interests: interest,
            }
            ).or_insert(0);
            *count += 1;
        });
    } else {
        let count = groups.entry(GroupKey {
            sex: if matcher.group_sex { account.sex } else { 0 },
            status: if matcher.group_status { account.status } else { 0 },
            country: if matcher.group_country { account.country } else { 0 },
            city: if matcher.group_city { account.city } else { 0 },
            interests: 0,
        }
        ).or_insert(0);
        *count += 1;
    }
}

fn make_matcher(storage: &Storage, params: &Vec<(String, String)>) -> Result<Option<Matcher>, StatusCode> {
    let mut matcher = Matcher {
        limit: 0,
        order: 0,
        fields: vec![],
        keys: vec![],
        key_extractors: vec![],

        sex: 0,
        status: 0,
        country: 0,
        city: 0,
        birth: 0,
        birth_from: 0,
        birth_to: 0,
        joined: 0,
        joined_from: 0,
        joined_to: 0,
        interest: 0,
        like: 0,

        group_sex: false,
        group_status: false,
        group_country: false,
        group_city: false,
        group_interests: false,
    };

    let mut empty_result = false;

    for (key, value) in params {
        match key.as_str() {
            "query_id" => {}
            "keys" => {
                matcher.keys = value.split(",").map(|str| str.to_string()).collect();
                for key in &matcher.keys {
                    match key.as_str() {
                        "sex" => {
                            matcher.group_sex = true;
                            matcher.key_extractors.push(|group_json| &group_json.sex);
                        }
                        "status" => {
                            matcher.group_status = true;
                            matcher.key_extractors.push(|group_json| &group_json.status);
                        }
                        "country" => {
                            matcher.group_country = true;
                            matcher.key_extractors.push(|group_json| &group_json.country);
                        }
                        "city" => {
                            matcher.group_city = true;
                            matcher.key_extractors.push(|group_json| &group_json.city);
                        }
                        "interests" => {
                            matcher.group_interests = true;
                            matcher.key_extractors.push(|group_json| &group_json.interests);
                        }
                        _ => return Err(StatusCode::BAD_REQUEST),
                    }
                }
            }
            "order" => {
                matcher.order = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                if matcher.order != -1 && matcher.order != 1 {
                    return Err(StatusCode::BAD_REQUEST);
                }
            }
            "limit" => {
                matcher.limit = value.parse::<usize>().map_err(|_| StatusCode::BAD_REQUEST)?;
                if matcher.limit == 0 {
                    return Err(StatusCode::BAD_REQUEST);
                }
            }
            _ => {
                match key.as_str() {
                    "sex" => {
                        if value.is_empty() {
                            Err(StatusCode::BAD_REQUEST)?
                        }
                        matcher.sex = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.sex == 0 {
                            empty_result = true;
                        }
                    }
                    "status" => {
                        if value.is_empty() {
                            Err(StatusCode::BAD_REQUEST)?
                        }
                        matcher.status = storage.dict.get_existing_key(value).unwrap_or(0);
                        if matcher.status == 0 {
                            empty_result = true;
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
                    "birth" => {
                        matcher.birth = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                        matcher.birth_from = seconds_from_year(matcher.birth);
                        matcher.birth_to = seconds_from_year(matcher.birth + 1);
                    }
                    "joined" => {
                        matcher.joined = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                        matcher.joined_from = seconds_from_year(matcher.joined);
                        matcher.joined_to = seconds_from_year(matcher.joined + 1);
                    }
                    "interests" => {
                        if value.is_empty() {
                            Err(StatusCode::BAD_REQUEST)?
                        }
                        matcher.interest = storage.interest_dict.get_existing_key(value).unwrap_or(0);
                        if matcher.interest == 0 {
                            empty_result = true;
                        }
                    }
                    "likes" => {
                        matcher.like = value.parse::<i32>().map_err(|_| StatusCode::BAD_REQUEST)?;
                    }
                    _ => return Err(StatusCode::BAD_REQUEST)
                };
                matcher.fields.push(key.clone());
            }
        }
    }
    if empty_result {
        return Ok(None);
    }
    Ok(Some(matcher))
}

fn matches(account: &Account, matcher: &Matcher) -> bool {
    if matcher.sex != 0 && matcher.sex != account.sex {
        return false;
    }
    if matcher.status != 0 && account.status != matcher.status {
        return false;
    }
    if matcher.country != 0 && account.country != matcher.country {
        return false;
    }
    if matcher.city != 0 && account.city != matcher.city {
        return false;
    }
    if matcher.birth != 0 && (account.birth < matcher.birth_from || account.birth >= matcher.birth_to) {
        return false;
    }
    if matcher.joined != 0 && (account.joined < matcher.joined_from || account.joined >= matcher.joined_to) {
        return false;
    }
    if matcher.interest != 0 {
        if account.interests.is_empty() {
            return false;
        }
        if !account.interests.contains(matcher.interest) {
            return false;
        }
    }
    if matcher.like != 0 {
        if account.likes.is_empty() {
            return false;
        }
        if !account.likes.contains(&matcher.like) { // TODO binary?
            return false;
        }
    }
    return true;
}

fn cmp_dict(a: &Option<Arc<String>>, b: &Option<Arc<String>>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some(a), Some(b)) => a.cmp(&b)
    }
}

fn cmp_groups(matcher: &Matcher, a: &GroupJson, b: &GroupJson) -> Ordering {
    let cmp = a.count.cmp(&b.count)
        .then_with(|| {
            for key_extractor in &matcher.key_extractors {
                match cmp_dict(key_extractor(a), key_extractor(b)) {
                    Ordering::Equal => {}
                    cmp => return cmp
                }
            }
            Ordering::Equal
        });
    if matcher.order > 0 { cmp } else { cmp.reverse() }
}

impl<'a> Ord for OrderedGroupJson<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_groups(self.matcher, &self.group_json, &other.group_json)
    }
}

impl<'a> PartialOrd for OrderedGroupJson<'a> {
    fn partial_cmp(&self, other: &OrderedGroupJson) -> Option<Ordering> {
        Some(cmp_groups(self.matcher, &self.group_json, &other.group_json))
    }
}

impl<'a> PartialEq for OrderedGroupJson<'a> {
    fn eq(&self, other: &OrderedGroupJson) -> bool {
        cmp_groups(self.matcher, &self.group_json, &other.group_json) == Ordering::Equal
    }
}

impl<'a> Eq for OrderedGroupJson<'a> {}

pub struct Matcher {
    limit: usize,
    order: i32,
    fields: Vec<String>,
    pub keys: Vec<String>,
    key_extractors: Vec<fn(&GroupJson) -> &Option<Arc<String>>>,

    pub sex: i32,
    pub status: i32,
    pub country: i32,
    pub city: i32,
    pub birth: i32,
    pub birth_from: i32,
    pub birth_to: i32,
    pub joined: i32,
    pub joined_from: i32,
    pub joined_to: i32,
    pub interest: i32,
    pub like: i32,

    group_sex: bool,
    group_status: bool,
    group_country: bool,
    group_city: bool,
    group_interests: bool,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct GroupKey {
    pub sex: i32,
    pub status: i32,
    pub interests: i32,
    pub country: i32,
    pub city: i32,
}

struct OrderedGroupJson<'a> {
    matcher: &'a Matcher,
    group_json: GroupJson,
}

#[derive(Serialize, Debug)]
pub struct GroupsJson {
    groups: Vec<GroupJson>,
}

#[derive(Serialize, Debug, Clone)]
struct GroupJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    sex: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    interests: Option<Arc<String>>,
    count: i32,
}