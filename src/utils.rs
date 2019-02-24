use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveDateTime;

use crate::storage::Like;

lazy_static! {
    pub static ref EMPTY_INT_LIST: Vec<i32> = Vec::new();
    pub static ref EMPTY_LIKE_LIST: Vec<Like> = Vec::new();
}

pub fn year_from_seconds(seconds: i32) -> i32 {
    NaiveDateTime::from_timestamp(seconds as i64, 0).year()
}

pub fn seconds_from_year(year: i32) -> i32 {
    NaiveDate::from_ymd(year, 1, 1).and_hms(0, 0, 0).timestamp() as i32
}

pub fn insert_into_sorted_vec(value: i32, vec: &mut Vec<i32>) {
    match vec.binary_search(&value) {
        Ok(_pos) => {}
        Err(pos) => vec.insert(pos, value),
    }
}

/// В vec1 оставить только те элементы, которые есть в vec2.
pub fn retain_all_sorted(vec1: &mut Vec<i32>, vec2: &Vec<i32>) {
    let mut pos1 = 0; // позиция, куда перемещаются элементы первого списка
    let mut pos2 = 0; // позиция, в которой сравнивается элемент первого списка

    for value2 in vec2 {
        if pos2 >= vec1.len() {
            break;
        }
        while pos2 < vec1.len() && vec1[pos2] < *value2 {
            pos2 += 1;
        }
        if pos2 < vec1.len() && vec1[pos2] == *value2 {
            if pos1 < pos2 {
                vec1[pos1] = *value2;
            }
            pos1 += 1;
            pos2 += 1;
        }
    }
    vec1.resize(pos1, 0);
}

pub fn merge_sorted_to(vec1: &Vec<i32>, vec2: &Vec<i32>, result: &mut Vec<i32>) {
    result.reserve(vec1.len() + vec2.len());
    if vec1.is_empty() {
        result.extend(vec2.iter());
        return;
    }
    if vec2.is_empty() {
        result.extend(vec1.iter());
        return;
    }
    let mut iter1 = vec1.iter();
    let mut iter2 = vec2.iter();
    let mut item1 = iter1.next();
    let mut item2 = iter2.next();
    while item1.is_some() && item2.is_some() {
        if item1.unwrap() == item2.unwrap() {
            result.push(*item1.unwrap());
            item1 = iter1.next();
            item2 = iter2.next();
        } else if item1.unwrap() < item2.unwrap() {
            result.push(*item1.unwrap());
            item1 = iter1.next();
        } else {
            result.push(*item2.unwrap());
            item2 = iter2.next();
        }
    }
    while item1.is_some() {
        result.push(*item1.unwrap());
        item1 = iter1.next();
    }
    while item2.is_some() {
        result.push(*item2.unwrap());
        item2 = iter2.next();
    }
}

pub fn merge_sorted(vec1: &Vec<i32>, vec2: &Vec<i32>) -> Vec<i32> {
    let mut result: Vec<i32> = Vec::new();
    merge_sorted_to(vec1, vec2, &mut result);
    result
}

//pub fn vec_compare<T: PartialEq>(vec1: &[T], vec2: &[T]) -> bool {
//    (vec1.len() == vec2.len()) && vec1.iter().zip(vec2).all(|(a,b)| a == b)
//}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retain_all_sorted() {
        {
            let mut vec1 = vec![1, 2, 3];
            retain_all_sorted(&mut vec1, &vec![1, 3, 5]);
            assert_eq!(vec1, vec![1, 3])
        }
        {
            let mut vec1 = vec![1, 2, 3];
            retain_all_sorted(&mut vec1, &vec![0, 3, 5]);
            assert_eq!(vec1, vec![3])
        }
        {
            let mut vec1 = vec![1, 2, 3];
            retain_all_sorted(&mut vec1, &vec![1, 2]);
            assert_eq!(vec1, vec![1, 2])
        }
        {
            let mut vec1 = vec![1, 2, 3];
            retain_all_sorted(&mut vec1, &vec![]);
            assert_eq!(vec1, Vec::<i32>::new())
        }
        {
            let mut vec1 = vec![];
            retain_all_sorted(&mut vec1, &vec![1, 3, 5]);
            assert_eq!(vec1, Vec::<i32>::new())
        }
    }

    #[test]
    fn test_merge_sorted() {
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![1, 2, 3], &vec![3, 5], &mut result);
            assert_eq!(result, vec![1, 2, 3, 5]);
        }
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![3, 5], &vec![1, 2, 3], &mut result);
            assert_eq!(result, vec![1, 2, 3, 5]);
        }
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![1, 2, 3], &vec![4, 5], &mut result);
            assert_eq!(result, vec![1, 2, 3, 4, 5]);
        }
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![4, 5], &vec![1, 2, 3], &mut result);
            assert_eq!(result, vec![1, 2, 3, 4, 5]);
        }
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![3], &vec![1, 4], &mut result);
            assert_eq!(result, vec![1, 3, 4]);
        }
        {
            let mut result = Vec::new();
            merge_sorted_to(&vec![1, 4], &vec![3], &mut result);
            assert_eq!(result, vec![1, 3, 4]);
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Key {
    pub key1: i32,
    pub key2: i32,
}

impl Key {
    pub fn new() -> Key {
        Key::new1(0)
    }

    pub fn new1(key1: i32) -> Key {
        Key::new2(key1, 0)
    }

    pub fn new2(key1: i32, key2: i32) -> Key {
        Key { key1, key2 }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Key1 {
    pub key1: i32,
}

impl Key1 {
    pub fn new(key1: i32) -> Key1 {
        Key1 { key1 }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Key2 {
    pub key1: i32,
    pub key2: i32,
}

impl Key2 {
    pub fn new(key1: i32, key2: i32) -> Key2 {
        Key2 { key1, key2 }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Key3 {
    pub key1: i32,
    pub key2: i32,
    pub key3: i32,
}

impl Key3 {
    pub fn new(key1: i32, key2: i32, key3: i32) -> Key3 {
        Key3 { key1, key2, key3 }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct KeySet {
    keys: Vec<String>,
}

impl KeySet {
    pub fn new(vec: &Vec<&str>) -> KeySet {
        let mut keys: Vec<String> = vec.iter().map(|key| key.to_string()).collect();
        keys.sort();
        KeySet { keys }
    }

    pub fn new2(vec: &Vec<String>) -> KeySet {
        let mut keys = vec.clone();
        keys.sort();
        KeySet { keys }
    }
}

pub struct StatusCode(u16);

impl StatusCode {
    //    pub const OK: StatusCode = StatusCode(200);
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    pub const CREATED: StatusCode = StatusCode(201);
    pub const ACCEPTED: StatusCode = StatusCode(202);

    pub fn as_str(&self) -> &str {
        match self.0 {
            200 => "200",
            400 => "400",
            404 => "404",
            201 => "201",
            202 => "202",
            _ => unimplemented!(),
        }
    }
}

impl std::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}