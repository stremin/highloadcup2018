use std::cmp::Ordering;

use crate::storage::Account;
use crate::storage::AccountJson;
use crate::storage::AccountsJson;
use crate::storage::NULL_DATE;
use crate::storage::Premium;
use crate::storage::Storage;
use crate::topn::TopN;
use crate::utils::EMPTY_INT_LIST;
use crate::utils::merge_sorted;
use crate::utils::StatusCode;

#[inline(never)]
pub fn recommend(storage: &Storage, id: i32, params: &Vec<(String, String)>) -> Result<AccountsJson, StatusCode> {
    let person = storage.accounts[id as usize].as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let matcher = match make_matcher(storage, &params)? {
        Some(matcher) => matcher,
        None => return Ok(AccountsJson { accounts: Vec::new() })
    };

    if person.interests.is_empty() {
        return Ok(AccountsJson { accounts: Vec::new() });
    }


    let index = if person.sex == storage.consts.male { &storage.indexes.recommend_index_female } else { &storage.indexes.recommend_index_male };

    let mut result: TopN<OrderedAccount> = TopN::new(matcher.limit);

    let city_ids = if matcher.city != 0 { Some(storage.indexes.city_index.get(&matcher.city).unwrap_or(&EMPTY_INT_LIST)) } else { None };
    let country_ids = if matcher.country != 0 { Some(storage.indexes.country_index.get(&matcher.country).unwrap_or(&EMPTY_INT_LIST)) } else { None };
    let mut used_city = false;

    for recommend_order in 0..6 {
//        debug!("rorder {} interests len {}", recommend_order, person.interests.len());
        let mut ids = Vec::new();
        for interest in &person.interests {
            if let Some(array) = index.get(interest as usize) {
                let ids2 = &array[recommend_order as usize];
//                debug!("interest {} ids2 len {}", interest, ids2.len());
                if city_ids.is_some() && ids2.len() >= city_ids.unwrap().len() {
                    ids = city_ids.unwrap().clone();
                    used_city = true;
//                    debug!("used_city len {}", city_ids.unwrap().len());
                    result.clear();
                    break;
                }
                if country_ids.is_some() && ids2.len() >= country_ids.unwrap().len() {
                    ids = country_ids.unwrap().clone();
                    used_city = true;
//                    debug!("used_country len {}", country_ids.unwrap().len());
                    result.clear();
                    break;
                }
                ids = merge_sorted(&ids, ids2);
            }
        }
//        debug!("ids len {}", ids.len());
        ids.iter()
            .filter_map(|id| storage.accounts[*id as usize].as_ref())
            .filter(|account| used_city || account.recommend_order == recommend_order)
            .filter(|account| account.sex != person.sex)
            .filter(|account| matches(account, &matcher))
            .filter(|account| !account.interests.is_empty() && person.interests.contains_any(&account.interests))
            .for_each(|account| {
                result.push(OrderedAccount { person, account });
            });
        if used_city || result.len() >= matcher.limit {
            break;
        }
    }

    Ok(AccountsJson {
        accounts: result.into_sorted_vec().iter()
            .map(|account| account.account)
            .map(|account| {
                AccountJson {
                    id: Some(account.id),
                    email: Some(account.email.as_ref().unwrap().clone()),
                    status: storage.dict.get_value(account.status),
                    sname: storage.dict.get_value(account.sname),
                    fname: storage.dict.get_value(account.fname),
                    birth: if account.birth != NULL_DATE { Some(account.birth) } else { None },
                    premium: if account.premium_start != NULL_DATE { Some(Premium { start: account.premium_start, finish: account.premium_finish }) } else { None },

                    phone: None,
                    sex: None,
                    country: None,
                    city: None,
                    joined: None,
                    interests: vec![],
                    likes: vec![],
                }
            })
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

fn cmp_accounts(person: &Account, a: &Account, b: &Account) -> Ordering {
    a.recommend_order.cmp(&b.recommend_order)
        .then_with(|| person.interests.count_common(&b.interests).cmp(&person.interests.count_common(&a.interests)))
        .then_with(|| (a.birth - person.birth).abs().cmp(&(b.birth - person.birth).abs()))
        .then_with(|| a.id.cmp(&b.id))
}

struct OrderedAccount<'a> {
    person: &'a Account,
    account: &'a Account,
}

impl<'a> Ord for OrderedAccount<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_accounts(self.person, self.account, other.account)
    }
}

impl<'a> PartialOrd for OrderedAccount<'a> {
    fn partial_cmp(&self, other: &OrderedAccount) -> Option<Ordering> {
        Some(cmp_accounts(self.person, self.account, other.account))
    }
}

impl<'a> PartialEq for OrderedAccount<'a> {
    fn eq(&self, other: &OrderedAccount) -> bool {
        cmp_accounts(self.person, self.account, other.account) == Ordering::Equal
    }
}

impl<'a> Eq for OrderedAccount<'a> {}

#[derive(Debug)]
struct Matcher {
    limit: usize,
    country: i32,
    city: i32,
}
