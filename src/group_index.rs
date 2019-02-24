use std::collections::HashMap;

use enum_map::EnumMap;

use crate::group::GroupKey;
use crate::group::Matcher;
use crate::storage::Account;
use crate::utils::Key;
use crate::utils::KeySet;
use crate::utils::year_from_seconds;

#[derive(Enum, Clone, Debug)]
enum FilterType {
    None,
    Sex,
    Status,
    SexStatus,
    Joined,
    JoinedSex,
    JoinedStatus,
    Interests,
    JoinedInterests,
    Birth,
    Country,
    City,
    BirthStatus,
    CountryBirth,
    BirthInterests,
    SexBirth,
    CityBirth,
    CountryJoined,
    CityJoined,
}

impl Copy for FilterType {}

#[derive(Enum, Clone, Debug)]
enum GroupType {
    Sex,
    Status,
    City,
    Country,
    Interests,
    SexCity,
    SexCountry,
    StatusCity,
    StatusCountry,
}

impl Copy for GroupType {}

lazy_static! {
    static ref keys_to_group_type: HashMap<KeySet, GroupType> = {
        let mut map: HashMap<KeySet, GroupType> = HashMap::new();
        map.insert(KeySet::new(&vec!["sex"]), GroupType::Sex);
        map.insert(KeySet::new(&vec!["status"]), GroupType::Status);
        map.insert(KeySet::new(&vec!["city"]), GroupType::City);
        map.insert(KeySet::new(&vec!["country"]), GroupType::Country);
        map.insert(KeySet::new(&vec!["interests"]), GroupType::Interests);
        map.insert(KeySet::new(&vec!["sex", "city"]), GroupType::SexCity);
        map.insert(KeySet::new(&vec!["sex", "country"]), GroupType::SexCountry);
        map.insert(KeySet::new(&vec!["status", "city"]), GroupType::StatusCity);
        map.insert(KeySet::new(&vec!["status", "country"]), GroupType::StatusCountry);
        map
    };
}

pub struct GroupIndex {
    // filterType -> filterKey -> groupType -> groupingKey -> count
    map: EnumMap<FilterType, HashMap<Key, EnumMap<GroupType, HashMap<Key, i32>>>>
}

impl GroupIndex {
    pub fn new() -> GroupIndex {
        GroupIndex {
            map: enum_map! { _ => HashMap::new() },
        }
    }

    pub fn update_account(&mut self, account: &Account, incr: i32) {
        self.update_filter(FilterType::None, Key::new(), account, incr);
        self.update_filter(FilterType::Sex, Key::new1(account.sex), account, incr);
        self.update_filter(FilterType::Status, Key::new1(account.status), account, incr);
        self.update_filter(FilterType::SexStatus, Key::new2(account.sex, account.status), account, incr);
        self.update_filter(FilterType::Joined, Key::new1(year_from_seconds(account.joined)), account, incr);
        self.update_filter(FilterType::JoinedSex, Key::new2(year_from_seconds(account.joined), account.sex), account, incr);
        self.update_filter(FilterType::JoinedStatus, Key::new2(year_from_seconds(account.joined), account.status), account, incr);
        account.interests.into_iter().for_each(|interest| {
            self.update_filter(FilterType::Interests, Key::new1(interest), account, incr);
            self.update_filter(FilterType::JoinedInterests, Key::new2(year_from_seconds(account.joined), interest), account, incr);
            self.update_filter(FilterType::BirthInterests, Key::new2(year_from_seconds(account.birth), interest), account, incr);
        });
        self.update_filter(FilterType::Birth, Key::new1(year_from_seconds(account.birth)), account, incr);
        self.update_filter(FilterType::Country, Key::new1(account.country), account, incr);
        self.update_filter(FilterType::City, Key::new1(account.city), account, incr);
        self.update_filter(FilterType::BirthStatus, Key::new2(year_from_seconds(account.birth), account.status), account, incr);
        self.update_filter(FilterType::CountryBirth, Key::new2(account.country, year_from_seconds(account.birth)), account, incr);
        self.update_filter(FilterType::SexBirth, Key::new2(account.sex, year_from_seconds(account.birth)), account, incr);
        self.update_filter(FilterType::CityBirth, Key::new2(account.city, year_from_seconds(account.birth)), account, incr);
        self.update_filter(FilterType::CountryJoined, Key::new2(account.country, year_from_seconds(account.joined)), account, incr);
        self.update_filter(FilterType::CityJoined, Key::new2(account.city, year_from_seconds(account.joined)), account, incr);
    }

    fn update_filter(&mut self, filter_type: FilterType, filter_key: Key, account: &Account, incr: i32) {
        let group_map = self.map[filter_type].entry(filter_key).or_insert_with(|| enum_map! { _ => HashMap::new() });
        account.interests.into_iter().for_each(|interest| {
            let group_key = make_group_key_from_account(&GroupType::Interests, account, interest);
            let count = group_map[GroupType::Interests].entry(group_key).or_insert_with(|| 0);
            *count += incr;
        });
        // отдельная запись с пустым интересом
        group_map.iter_mut().for_each(|(k, v)| {
            match k {
                GroupType::Interests => {}
                _ => {
                    let group_key = make_group_key_from_account(&k, account, 0);
                    let count = v.entry(group_key).or_insert_with(|| 0);
                    *count += incr;
                }
            }
        });
    }

    pub fn get_result(&self, matcher: &Matcher) -> Option<HashMap<GroupKey, i32>> {
        let filter_type = get_filter_type(matcher);
        let group_type = keys_to_group_type.get(&KeySet::new2(&matcher.keys)); // TODO avoid clone
        if filter_type.is_none() || group_type.is_none() {
            return None;
        }
        match self.map[filter_type.unwrap()].get(&make_filter_key(matcher, filter_type.as_ref().unwrap())) {
            None => {
                Some(HashMap::new())
            }
            Some(groups) => {
                // debug!("{:?} {:?} {:?}", filter_type, group_type, groups[*group_type.unwrap()].len());
                Some(groups[*group_type.unwrap()].iter()
                    .filter(|(_, v)| **v > 0)
                    .map(|(k, v)| (make_group_key_from_key(k, group_type.unwrap()), *v))
                    .collect())
            }
        }
    }
}

fn make_filter_key(matcher: &Matcher, filter_type: &FilterType) -> Key {
    match filter_type {
        FilterType::None => Key::new(),
        FilterType::Sex => Key::new1(matcher.sex),
        FilterType::Status => Key::new1(matcher.status),
        FilterType::SexStatus => Key::new2(matcher.sex, matcher.status),
        FilterType::Joined => Key::new1(matcher.joined),
        FilterType::JoinedSex => Key::new2(matcher.joined, matcher.sex),
        FilterType::JoinedStatus => Key::new2(matcher.joined, matcher.status),
        FilterType::Interests => Key::new1(matcher.interest),
        FilterType::JoinedInterests => Key::new2(matcher.joined, matcher.interest),
        FilterType::Birth => Key::new1(matcher.birth),
        FilterType::Country => Key::new1(matcher.country),
        FilterType::City => Key::new1(matcher.city),
        FilterType::BirthStatus => Key::new2(matcher.birth, matcher.status),
        FilterType::CountryBirth => Key::new2(matcher.country, matcher.birth),
        FilterType::BirthInterests => Key::new2(matcher.birth, matcher.interest),
        FilterType::SexBirth => Key::new2(matcher.sex, matcher.birth),
        FilterType::CityBirth => Key::new2(matcher.city, matcher.birth),
        FilterType::CountryJoined => Key::new2(matcher.country, matcher.joined),
        FilterType::CityJoined => Key::new2(matcher.city, matcher.joined),
    }
}

fn make_group_key_from_account(group_type: &GroupType, account: &Account, interest: i32) -> Key {
    match group_type {
        GroupType::Sex => Key::new1(account.sex),
        GroupType::Status => Key::new1(account.status),
        GroupType::City => Key::new1(account.city),
        GroupType::Country => Key::new1(account.country),
        GroupType::Interests => Key::new1(interest),
        GroupType::SexCity => Key::new2(account.sex, account.city),
        GroupType::SexCountry => Key::new2(account.sex, account.country),
        GroupType::StatusCity => Key::new2(account.status, account.city),
        GroupType::StatusCountry => Key::new2(account.status, account.country),
    }
}

fn make_group_key_from_key(key: &Key, group_type: &GroupType) -> GroupKey {
    match group_type {
        GroupType::Sex => GroupKey { sex: key.key1, status: 0, city: 0, country: 0, interests: 0 },
        GroupType::Status => GroupKey { sex: 0, status: key.key1, city: 0, country: 0, interests: 0 },
        GroupType::City => GroupKey { sex: 0, status: 0, city: key.key1, country: 0, interests: 0 },
        GroupType::Country => GroupKey { sex: 0, status: 0, city: 0, country: key.key1, interests: 0 },
        GroupType::Interests => GroupKey { sex: 0, status: 0, city: 0, country: 0, interests: key.key1 },
        GroupType::SexCity => GroupKey { sex: key.key1, status: 0, city: key.key2, country: 0, interests: 0 },
        GroupType::SexCountry => GroupKey { sex: key.key1, status: 0, city: 0, country: key.key2, interests: 0 },
        GroupType::StatusCity => GroupKey { sex: 0, status: key.key1, city: key.key2, country: 0, interests: 0 },
        GroupType::StatusCountry => GroupKey { sex: 0, status: key.key1, city: 0, country: key.key2, interests: 0 },
    }
}

fn get_filter_type(matcher: &Matcher) -> Option<FilterType> {
    if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::None);
    } else if matcher.sex != 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::Sex);
    } else if matcher.sex == 0 &&
        matcher.status != 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::Status);
    } else if matcher.sex != 0 &&
        matcher.status != 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::SexStatus);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::Joined);
    } else if matcher.sex != 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::JoinedSex);
    } else if matcher.sex == 0 &&
        matcher.status != 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::JoinedStatus);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest != 0 &&
        matcher.like == 0 {
        return Some(FilterType::Interests);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest != 0 &&
        matcher.like == 0 {
        return Some(FilterType::JoinedInterests);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::Birth);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country != 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::Country);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city != 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::City);
    } else if matcher.sex == 0 &&
        matcher.status != 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::BirthStatus);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country != 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::CountryBirth);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest != 0 &&
        matcher.like == 0 {
        return Some(FilterType::BirthInterests);
    } else if matcher.sex != 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country == 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::SexBirth);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city != 0 &&
        matcher.country == 0 &&
        matcher.birth != 0 &&
        matcher.joined == 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::CityBirth);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city == 0 &&
        matcher.country != 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::CountryJoined);
    } else if matcher.sex == 0 &&
        matcher.status == 0 &&
        matcher.city != 0 &&
        matcher.country == 0 &&
        matcher.birth == 0 &&
        matcher.joined != 0 &&
        matcher.interest == 0 &&
        matcher.like == 0 {
        return Some(FilterType::CityJoined);
    }
    None
}
