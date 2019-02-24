use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use enum_map::EnumMap;

use crate::filter::Matcher;
use crate::storage::Account;
use crate::storage::Consts;
use crate::utils::EMPTY_INT_LIST;
use crate::utils::insert_into_sorted_vec;
use crate::utils::Key1;
use crate::utils::Key2;
use crate::utils::Key3;
use crate::utils::KeySet;
use crate::utils::merge_sorted;

const KEEP_TOP: usize = 500; // храним не все номера учеток, а только хвост
const KEEP_TOP_EMAIL: usize = 5000; // эдесь хвост нужен больше, так как идут запросы lt/gt с двумя буквами

#[derive(Enum, Clone, Debug)]
enum FilterType {
    SexCountryNull,
    CountryNull,
    SexCityNull,
    CityNull,
    EmailLt,
    EmailGt,
    EmailLtSex,
    EmailGtSex,
    CountryNullPhoneCode,
    CityNullPhoneCode,
    FnameCountryNullSex,
    FnameCityNullSex,
    FnameSex,
    FnameCountryNull,
    FnameCityNull,
    EmailLtCityNull,
    EmailGtCityNull,
    EmailLtCountryNullSex,
    EmailGtCountryNullSex,
}

impl Copy for FilterType {}

lazy_static! {
    static ref keys_to_filter_type: HashMap<KeySet, FilterType> = {
        let mut map: HashMap<KeySet, FilterType> = HashMap::new();
        map.insert(KeySet::new(&vec!["sex_eq", "country_null"]), FilterType::SexCountryNull);
        map.insert(KeySet::new(&vec!["country_null"]), FilterType::CountryNull);
        map.insert(KeySet::new(&vec!["sex_eq", "city_null"]), FilterType::SexCityNull);
        map.insert(KeySet::new(&vec!["city_null"]), FilterType::CityNull);
        map.insert(KeySet::new(&vec!["email_lt"]), FilterType::EmailLt);
        map.insert(KeySet::new(&vec!["email_gt"]), FilterType::EmailGt);
        map.insert(KeySet::new(&vec!["email_lt", "sex_eq"]), FilterType::EmailLtSex);
        map.insert(KeySet::new(&vec!["email_gt", "sex_eq"]), FilterType::EmailGtSex);
        map.insert(KeySet::new(&vec!["country_null", "phone_code"]), FilterType::CountryNullPhoneCode);
        map.insert(KeySet::new(&vec!["city_null", "phone_code"]), FilterType::CityNullPhoneCode);
        map.insert(KeySet::new(&vec!["fname_any", "country_null", "sex_eq"]), FilterType::FnameCountryNullSex);
        map.insert(KeySet::new(&vec!["fname_any", "city_null", "sex_eq"]), FilterType::FnameCityNullSex);
        map.insert(KeySet::new(&vec!["fname_any", "sex_eq"]), FilterType::FnameSex); // почему-то обычного индекса по fname не достаточно - передают невозможные комбинации (имя другого пола)?
        map.insert(KeySet::new(&vec!["fname_any", "country_null"]), FilterType::FnameCountryNull);
        map.insert(KeySet::new(&vec!["fname_any", "city_null"]), FilterType::FnameCityNull);
        map.insert(KeySet::new(&vec!["email_lt", "city_null"]), FilterType::EmailLtCityNull);
        map.insert(KeySet::new(&vec!["email_gt", "city_null"]), FilterType::EmailGtCityNull);
        map.insert(KeySet::new(&vec!["email_lt", "country_null", "sex_eq"]), FilterType::EmailLtCountryNullSex);
        map.insert(KeySet::new(&vec!["email_gt", "country_null", "sex_eq"]), FilterType::EmailGtCountryNullSex);
        map
    };
}

pub struct FilterIndex {
    // filterType -> filterKey -> list
    map1: EnumMap<FilterType, HashMap<Key1, Vec<i32>>>,
    map2: EnumMap<FilterType, HashMap<Key2, Vec<i32>>>,
    map3: EnumMap<FilterType, HashMap<Key3, Vec<i32>>>,
}

impl FilterIndex {
    pub fn new() -> FilterIndex {
        FilterIndex {
            map1: enum_map! { _ => HashMap::new() },
            map2: enum_map! { _ => HashMap::new() },
            map3: enum_map! { _ => HashMap::new() },
        }
    }

    pub fn update_account(&mut self, account: &Account, consts: &Consts) {
        update_filter(&mut self.map2, FilterType::SexCountryNull, Key2::new(account.sex, if account.country == 0 { 1 } else { 0 }), account);
        update_filter(&mut self.map1, FilterType::CountryNull, Key1::new(if account.country == 0 { 1 } else { 0 }), account);
        update_filter(&mut self.map2, FilterType::SexCityNull, Key2::new(account.sex, if account.city == 0 { 1 } else { 0 }), account);
        update_filter(&mut self.map1, FilterType::CityNull, Key1::new(if account.city == 0 { 1 } else { 0 }), account);
        for ch in first_letter2(&account.email)..'z' as i32 {
            update_filter2(&mut self.map1, FilterType::EmailLt, Key1::new(ch), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map2, FilterType::EmailLtSex, Key2::new(ch, account.sex), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map2, FilterType::EmailLtCityNull, Key2::new(ch, if account.city == 0 { 1 } else { 0 }), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map3, FilterType::EmailLtCountryNullSex, Key3::new(ch, if account.country == 0 { 1 } else { 0 }, account.sex), account, KEEP_TOP_EMAIL);
        }
        for ch in 'a' as i32..first_letter2(&account.email) + 1 {
            update_filter2(&mut self.map1, FilterType::EmailGt, Key1::new(ch), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map2, FilterType::EmailGtSex, Key2::new(ch, account.sex), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map2, FilterType::EmailGtCityNull, Key2::new(ch, if account.city == 0 { 1 } else { 0 }), account, KEEP_TOP_EMAIL);
            update_filter2(&mut self.map3, FilterType::EmailGtCountryNullSex, Key3::new(ch, if account.country == 0 { 1 } else { 0 }, account.sex), account, KEEP_TOP_EMAIL);
        }
        update_filter(&mut self.map2, FilterType::CountryNullPhoneCode, Key2::new(if account.country == 0 { 1 } else { 0 }, account.phone_code), account);
        update_filter(&mut self.map2, FilterType::CityNullPhoneCode, Key2::new(if account.city == 0 { 1 } else { 0 }, account.phone_code), account);
        update_filter(&mut self.map3, FilterType::FnameCountryNullSex, Key3::new(account.fname, if account.country == 0 { 1 } else { 0 }, account.sex), account);
        update_filter(&mut self.map3, FilterType::FnameCityNullSex, Key3::new(account.fname, if account.city == 0 { 1 } else { 0 }, account.sex), account);
        update_filter(&mut self.map2, FilterType::FnameCountryNull, Key2::new(account.fname, if account.country == 0 { 1 } else { 0 }), account);
        update_filter(&mut self.map2, FilterType::FnameCityNull, Key2::new(account.fname, if account.city == 0 { 1 } else { 0 }), account);
        update_filter(&mut self.map2, FilterType::FnameSex, Key2::new(account.fname, account.sex), account);
    }

    pub fn get_result(&self, matcher: &Matcher) -> Option<Cow<[i32]>> {
        let filter_type = keys_to_filter_type.get(&KeySet::new2(&matcher.conditions));
        if filter_type.is_none() {
            return None;
        }
        if let Some(interests_contains) = &matcher.interests_contains {
            if interests_contains.count() > 1 {
                return None; // вариант для нескольких интересов пришлось отключить
            }
        }
        let map1 = &self.map1[*filter_type.unwrap()];
        let map2 = &self.map2[*filter_type.unwrap()];
        let map3 = &self.map3[*filter_type.unwrap()];
        match filter_type.unwrap() {
            FilterType::CountryNull |
            FilterType::CityNull |
            FilterType::EmailLt |
            FilterType::EmailGt => {
                Some(Cow::from(map1.get(&make_key1(*filter_type.unwrap(), &matcher)).unwrap_or(&EMPTY_INT_LIST)))
            }
            FilterType::SexCountryNull |
            FilterType::SexCityNull |
            FilterType::EmailLtSex |
            FilterType::EmailGtSex |
            FilterType::CountryNullPhoneCode |
            FilterType::CityNullPhoneCode |
            FilterType::EmailLtCityNull |
            FilterType::EmailGtCityNull => {
                Some(Cow::from(map2.get(&make_key2(*filter_type.unwrap(), &matcher)).unwrap_or(&EMPTY_INT_LIST)))
            }
            FilterType::EmailLtCountryNullSex |
            FilterType::EmailGtCountryNullSex => {
                Some(Cow::from(map3.get(&make_key3(*filter_type.unwrap(), &matcher)).unwrap_or(&EMPTY_INT_LIST)))
            }
            FilterType::FnameCountryNullSex => {
                let mut vec: Vec<i32> = Vec::new();
                for fname in &matcher.fname_any {
                    let key = Key3::new(*fname, if matcher.country_null1 { 1 } else { 0 }, matcher.sex);
                    vec = merge_sorted(&vec, map3.get(&key).unwrap_or(&EMPTY_INT_LIST));
                }
                Some(Cow::from(vec))
            }
            FilterType::FnameCityNullSex => {
                let mut vec: Vec<i32> = Vec::new();
                for fname in &matcher.fname_any {
                    let key = Key3::new(*fname, if matcher.city_null1 { 1 } else { 0 }, matcher.sex);
                    vec = merge_sorted(&vec, map3.get(&key).unwrap_or(&EMPTY_INT_LIST));
                }
                Some(Cow::from(vec))
            }
            FilterType::FnameSex => {
                let mut vec: Vec<i32> = Vec::new();
                for fname in &matcher.fname_any {
                    let key = Key2::new(*fname, matcher.sex);
                    vec = merge_sorted(&vec, map2.get(&key).unwrap_or(&EMPTY_INT_LIST));
                }
                Some(Cow::from(vec))
            }
            FilterType::FnameCountryNull => {
                let mut vec: Vec<i32> = Vec::new();
                for fname in &matcher.fname_any {
                    let key = Key2::new(*fname, if matcher.country_null1 { 1 } else { 0 });
                    vec = merge_sorted(&vec, map2.get(&key).unwrap_or(&EMPTY_INT_LIST));
                }
                Some(Cow::from(vec))
            }
            FilterType::FnameCityNull => {
                let mut vec: Vec<i32> = Vec::new();
                for fname in &matcher.fname_any {
                    let key = Key2::new(*fname, if matcher.city_null1 { 1 } else { 0 });
                    vec = merge_sorted(&vec, map2.get(&key).unwrap_or(&EMPTY_INT_LIST));
                }
                Some(Cow::from(vec))
            }
        }
    }
}

fn update_filter<K: Eq + Hash>(map: &mut EnumMap<FilterType, HashMap<K, Vec<i32>>>, filter_type: FilterType, filter_key: K, account: &Account) {
    update_filter2(map, filter_type, filter_key, account, KEEP_TOP);
}

fn update_filter2<K: Eq + Hash>(map: &mut EnumMap<FilterType, HashMap<K, Vec<i32>>>, filter_type: FilterType, filter_key: K, account: &Account, limit: usize) {
    let mut vec = map[filter_type].entry(filter_key).or_insert_with(|| Vec::new());
    insert_into_sorted_vec(account.id, &mut vec);
    if vec.len() > limit {
        vec.remove(0);
    }
}

fn other_status1(status: i32, consts: &Consts) -> i32 {
    if status == consts.free_status {
        consts.hard_status
    } else if status == consts.hard_status {
        consts.free_status
    } else if status == consts.taken_status {
        consts.free_status
    } else {
        panic!("unexpected status {}", status)
    }
}

fn other_status2(status: i32, consts: &Consts) -> i32 {
    if status == consts.free_status {
        consts.taken_status
    } else if status == consts.hard_status {
        consts.taken_status
    } else if status == consts.taken_status {
        consts.hard_status
    } else {
        panic!("unexpected status {}", status)
    }
}

fn make_key1(filter_type: FilterType, matcher: &Matcher) -> Key1 {
    match filter_type {
        FilterType::CountryNull => Key1::new(if matcher.country_null1 { 1 } else { 0 }),
        FilterType::CityNull => Key1::new(if matcher.city_null1 { 1 } else { 0 }),
        FilterType::EmailLt => Key1::new(first_letter(&matcher.email_lt)),
        FilterType::EmailGt => Key1::new(first_letter(&matcher.email_gt)),
        _ => unreachable!(),
    }
}

fn make_key2(filter_type: FilterType, matcher: &Matcher) -> Key2 {
    match filter_type {
        FilterType::SexCountryNull => Key2::new(matcher.sex, if matcher.country_null1 { 1 } else { 0 }),
        FilterType::SexCityNull => Key2::new(matcher.sex, if matcher.city_null1 { 1 } else { 0 }),
        FilterType::EmailLtSex => Key2::new(first_letter(&matcher.email_lt), matcher.sex),
        FilterType::EmailGtSex => Key2::new(first_letter(&matcher.email_gt), matcher.sex),
        FilterType::CountryNullPhoneCode => Key2::new(if matcher.country_null1 { 1 } else { 0 }, matcher.phone_code),
        FilterType::CityNullPhoneCode => Key2::new(if matcher.city_null1 { 1 } else { 0 }, matcher.phone_code),
        FilterType::EmailLtCityNull => Key2::new(first_letter(&matcher.email_lt), if matcher.city_null1 { 1 } else { 0 }),
        FilterType::EmailGtCityNull => Key2::new(first_letter(&matcher.email_gt), if matcher.city_null1 { 1 } else { 0 }),
        _ => unreachable!(),
    }
}

fn make_key3(filter_type: FilterType, matcher: &Matcher) -> Key3 {
    match filter_type {
        FilterType::EmailLtCountryNullSex => Key3::new(first_letter(&matcher.email_lt), if matcher.country_null1 { 1 } else { 0 }, matcher.sex),
        FilterType::EmailGtCountryNullSex => Key3::new(first_letter(&matcher.email_gt), if matcher.country_null1 { 1 } else { 0 }, matcher.sex),
        _ => unreachable!(),
    }
}

fn first_letter(opt_str: &Option<String>) -> i32 {
    opt_str.as_ref().unwrap().as_bytes()[0] as i32
}

fn first_letter2(opt_str: &Option<Arc<String>>) -> i32 {
    opt_str.as_ref().unwrap().as_bytes()[0] as i32
}