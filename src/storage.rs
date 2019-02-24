use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use regex::Regex;
use zip::ZipArchive;

use crate::bits::Bits;
use crate::filter_index::FilterIndex;
use crate::group_index::GroupIndex;
use crate::stats::Stats;
use crate::utils::insert_into_sorted_vec;
use crate::utils::StatusCode;
use crate::utils::year_from_seconds;

pub const NULL_DATE: i32 = core::i32::MIN;
const MAX_ID: usize = 2_000_000;
static VALID_SEXES: [&str; 2] = ["m", "f"];
static VALID_STATUSES: [&str; 3] = ["свободны", "заняты", "всё сложно"];

lazy_static! {
    static ref PHONE_PATTERN: Regex = Regex::new("8\\((\\d{3})\\)(\\d{1,9})").unwrap();
}

pub struct Storage {
    // не получается сделать массив, так как нет конструктора копирования для инициализации None
    pub accounts: Vec<Option<Account>>,
    pub max_id: usize,
    pub now: i32,
    pub dict: Dict,
    pub interest_dict: Dict,
    pub consts: Consts,
    pub indexes: Indexes,
    pub stats: Stats,
}

pub struct Consts {
    pub free_status: i32,
    pub hard_status: i32,
    pub taken_status: i32,
    pub male: i32,
    pub female: i32,
}

pub struct Indexes {
    pub known_emails: HashSet<Arc<String>>,
    pub known_phones: HashSet<(i32, i32)>,
    pub likes_index_male: HashMap<i32, Vec<Like>>,
    pub likes_index_female: HashMap<i32, Vec<Like>>,
    pub interests_index: HashMap<i32, Vec<i32>>,
    pub interests_index_male: HashMap<i32, Vec<i32>>,
    pub interests_index_female: HashMap<i32, Vec<i32>>,
    pub interests2_index: HashMap<(i32, i32), Vec<i32>>,
    pub city_index: HashMap<i32, Vec<i32>>,
    pub country_index: HashMap<i32, Vec<i32>>,
    pub birth_index: HashMap<i32, Vec<i32>>,
    pub fname_index: HashMap<i32, Vec<i32>>,
    pub recommend_index_male: Vec<[Vec<i32>; 6]>,
    pub recommend_index_female: Vec<[Vec<i32>; 6]>,
    pub filter_index: FilterIndex,
    pub group_index: GroupIndex,
    pub similarity: HashMap<(i32, i32), f32>,
}

pub struct Dict {
    map: HashMap<Arc<String>, i32>,
    list: Vec<Arc<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccountsJson {
    pub accounts: Vec<AccountJson>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccountJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sname: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fname: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phone: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sex: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<Arc<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub joined: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Arc<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interests: Vec<Arc<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub likes: Vec<Like>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub premium: Option<Premium>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Like {
    pub id: i32,
    pub ts: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Premium {
    pub start: i32,
    pub finish: i32,
}

#[derive(Deserialize, Debug)]
struct LikesJson {
    likes: Vec<LikeJson>,
}

#[derive(Deserialize, Debug)]
struct LikeJson {
    liker: i32,
    likee: i32,
    ts: i32,
}

#[derive(Debug)]
pub struct Account {
    pub id: i32,
    pub sex: i32,
    pub email: Option<Arc<String>>,
    pub sname: i32,
    pub fname: i32,
    pub phone_number: i32,
    pub phone_code: i32,
    pub birth: i32,
    pub country: i32,
    pub city: i32,
    pub joined: i32,
    pub status: i32,
    pub interests: Bits,
    // unique, sorted by like.id
    pub likes: Vec<i32>,
    pub premium_start: i32,
    pub premium_finish: i32,

    pub is_premium: bool,
    pub recommend_order: u8,
}

impl Storage {
    pub fn load(path: &str) -> Storage {
        info!("loading data...");

        let options_file = File::open(Path::new(path).join("options.txt")).unwrap();
        let options_first_line = BufReader::new(options_file).lines().next().unwrap().unwrap();
        let now = options_first_line.parse::<i32>().unwrap();
        info!("options now: {}", now);

        let mut storage = Storage {
            accounts: Vec::new(),
            max_id: 0,
            now,
            dict: Dict::new(),
            interest_dict: Dict::new(),
            consts: Consts {
                free_status: 0,
                hard_status: 0,
                taken_status: 0,
                male: 0,
                female: 0,
            },
            indexes: Indexes {
                known_emails: HashSet::new(),
                known_phones: HashSet::new(),
                likes_index_male: HashMap::new(),
                likes_index_female: HashMap::new(),
                interests_index: HashMap::new(),
                interests_index_male: HashMap::new(),
                interests_index_female: HashMap::new(),
                interests2_index: HashMap::new(),
                city_index: HashMap::new(),
                country_index: HashMap::new(),
                birth_index: HashMap::new(),
                fname_index: HashMap::new(),
                recommend_index_male: Vec::new(),
                recommend_index_female: Vec::new(),
                filter_index: FilterIndex::new(),
                group_index: GroupIndex::new(),
                similarity: HashMap::new(),
            },
            stats: Stats::new(),
        };
        for _id in 0..MAX_ID {
            storage.accounts.push(None);
        }
        storage.consts.free_status = storage.dict.get_key(&Arc::new("свободны".to_string()));
        storage.consts.hard_status = storage.dict.get_key(&Arc::new("всё сложно".to_string()));
        storage.consts.taken_status = storage.dict.get_key(&Arc::new("заняты".to_string()));
        storage.consts.male = storage.dict.get_key(&Arc::new("m".to_string()));
        storage.consts.female = storage.dict.get_key(&Arc::new("f".to_string()));

        let zip_file = File::open(Path::new(path).join("data.zip")).unwrap();
        let mut zip = ZipArchive::new(BufReader::new(zip_file)).unwrap();
        let mut count = 0;
        for i in 0..zip.len() {
            let file = zip.by_index(i).unwrap();
            debug!("loading {}", file.name());
            let accounts_json: AccountsJson = serde_json::from_reader(BufReader::new(file)).unwrap();
            for account_json in accounts_json.accounts.iter() {
                let id = account_json.id.unwrap() as usize;
                let account_option = &mut storage.accounts[id];
                *account_option = Some(account_from_json(account_json, &mut storage.dict, &mut storage.interest_dict, true).unwrap());
                calc_account_fields(account_option.as_mut().unwrap(), storage.now, storage.consts.free_status, storage.consts.hard_status);
                for like in &account_json.likes {
                    update_likes_index(&storage.consts, &mut storage.indexes, account_option.as_ref().unwrap(), like.id, like.ts)
                }
                count += 1;
                if id > storage.max_id {
                    storage.max_id = id;
                }
            }
        }
        info!("loaded {} accounts, max id {}", count, storage.max_id);

        info!("dict size {}", storage.dict.max_key());
        info!("interests dict size {}", storage.interest_dict.max_key());

        info!("indexing...");
        // likes уже проиндексированы при загрузке
        for account in storage.accounts.iter() {
            if account.is_some() {
                update_account_index(&storage.consts, &mut storage.indexes, account.as_ref().unwrap());
                update_group_index(&mut storage.indexes, account.as_ref().unwrap(), 1);
            }
        }
        info!("indexing done");

        storage
    }

    pub fn new_account(&mut self, bytes: &[u8], success_response_f: &mut FnMut(StatusCode) -> ()) -> Result<(), StatusCode> {
        let account_json: AccountJson = serde_json::from_slice(bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
        let id = match account_json.id {
            Some(id) => id,
            None => Err(StatusCode::BAD_REQUEST)?,
        };
        let account_option = &mut self.accounts[id as usize];
        if account_option.is_some() ||
            self.indexes.known_emails.contains(account_json.email.as_ref().unwrap()) {
            Err(StatusCode::BAD_REQUEST)?;
        }
        if account_json.phone.is_some() {
            if let Some(phone_pair) = parse_phone(account_json.phone.as_ref().unwrap().as_str()).map_err(|_| StatusCode::BAD_REQUEST)? {
                if self.indexes.known_phones.contains(&phone_pair) {
                    Err(StatusCode::BAD_REQUEST)?;
                }
            }
        }

        success_response_f(StatusCode::CREATED);

        *account_option = Some(account_from_json(&account_json, &mut self.dict, &mut self.interest_dict, true).map_err(|_| StatusCode::BAD_REQUEST)?);
        if id as usize > self.max_id {
            self.max_id = id as usize;
        }

        calc_account_fields(account_option.as_mut().unwrap(), self.now, self.consts.free_status, self.consts.hard_status);
        update_account_index(&self.consts, &mut self.indexes, account_option.as_ref().unwrap());
        update_group_index(&mut self.indexes, account_option.as_ref().unwrap(), 1);
        for like in &account_json.likes {
            update_likes_index(&self.consts, &mut self.indexes, account_option.as_ref().unwrap(), like.id, like.ts)
        }
        Ok(())
    }

    pub fn update_account(&mut self, id: i32, bytes: &[u8], success_response_f: &mut FnMut(StatusCode) -> ()) -> Result<(), StatusCode> {
        let account_json: AccountJson = serde_json::from_slice(bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
        let update = account_from_json(&account_json, &mut self.dict, &mut self.interest_dict, false).map_err(|_| StatusCode::BAD_REQUEST)?;

        let account = self.accounts[id as usize].as_mut().ok_or(StatusCode::NOT_FOUND)?;
        if update.email.is_some() && update.email.as_ref().unwrap() != account.email.as_ref().unwrap() {
            if self.indexes.known_emails.contains(update.email.as_ref().unwrap()) {
                Err(StatusCode::BAD_REQUEST)?;
            } else {
                self.indexes.known_emails.remove(account.email.as_ref().unwrap());
            }
        }
        let phone_pair = (update.phone_code, update.phone_number);
        if update.phone_number != 0 && phone_pair != (account.phone_code, account.phone_number) {
            if self.indexes.known_phones.contains(&phone_pair) {
                Err(StatusCode::BAD_REQUEST)?;
            } else {
                self.indexes.known_phones.remove(&phone_pair);
            }
        }

        success_response_f(StatusCode::ACCEPTED);

        update_group_index(&mut self.indexes, account, -1);

        if update.email.is_some() {
            account.email = update.email.clone();
        }
        if update.sname != 0 {
            account.sname = update.sname;
        }
        if update.fname != 0 {
            account.fname = update.fname;
        }
        if update.phone_number != 0 {
            account.phone_number = update.phone_number;
            account.phone_code = update.phone_code;
        }
        if update.sex != 0 {
            account.sex = update.sex;
        }
        if update.birth != NULL_DATE {
            account.birth = update.birth;
        }
        if update.country != 0 {
            account.country = update.country;
        }
        if update.city != 0 {
            account.city = update.city;
        }
        if update.joined != NULL_DATE {
            account.joined = update.joined;
        }
        if update.status != 0 {
            account.status = update.status;
        }
        if !update.interests.is_empty() {
            account.interests = update.interests.clone();
        }
        if update.premium_start != NULL_DATE {
            account.premium_start = update.premium_start;
            account.premium_finish = update.premium_finish;
        }
        calc_account_fields(account, self.now, self.consts.free_status, self.consts.hard_status);
        update_account_index(&self.consts, &mut self.indexes, account);
        update_group_index(&mut self.indexes, account, 1);
        Ok(())
    }

    pub fn update_likes(&mut self, bytes: &[u8], success_response_f: &mut FnMut(StatusCode) -> ()) -> Result<(), StatusCode> {
        let likes_json: LikesJson = serde_json::from_slice(bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
        for like in &likes_json.likes {
            if self.accounts[like.liker as usize].is_none() || self.accounts[like.likee as usize].is_none() {
                Err(StatusCode::BAD_REQUEST)?;
            }
        }

        success_response_f(StatusCode::ACCEPTED);

        for like in &likes_json.likes {
            let account = self.accounts[like.liker as usize].as_mut().unwrap();
            insert_into_sorted_vec(like.likee, &mut account.likes);
            update_likes_index(&self.consts, &mut self.indexes, account, like.likee, like.ts);
        }
        Ok(())
    }
}

fn account_from_json(account_json: &AccountJson, dict: &mut Dict, interest_dict: &mut Dict, new_account: bool) -> Result<Account, String> {
    if new_account && account_json.id.is_none() {
        return Err("empty id".to_string());
    }
    if new_account && account_json.email.is_none() {
        return Err("empty email".to_string());
    }
    if account_json.email.is_some() && !account_json.email.as_ref().unwrap().contains("@") {
        return Err("invalid email".to_string());
    }
    if (new_account || account_json.sex.is_some()) && !VALID_SEXES.contains(&account_json.sex.as_ref().unwrap().as_str()) {
        return Err("invalid status".to_string());
    }
    if (new_account || account_json.status.is_some()) && !VALID_STATUSES.contains(&account_json.status.as_ref().unwrap().as_str()) {
        return Err("invalid status".to_string());
    }
    if new_account && account_json.birth.is_none() {
        return Err("empty birth".to_string());
    }
    if new_account && account_json.joined.is_none() {
        return Err("empty joined".to_string());
    }
    let mut phone_number = 0;
    let mut phone_code = 0;
    if account_json.phone.is_some() {
        if let Some(phone_pair) = parse_phone(account_json.phone.as_ref().unwrap().as_str())? {
            phone_code = phone_pair.0;
            phone_number = phone_pair.1;
        }
    }
    Ok(Account {
        id: account_json.id.unwrap_or(-1),
        email: account_json.email.as_ref().map(|email| email.clone()),
        sname: dict.get_key_from_option(&account_json.sname),
        fname: dict.get_key_from_option(&account_json.fname),
        phone_number,
        phone_code,
        sex: dict.get_key_from_option(&account_json.sex),
        birth: account_json.birth.unwrap_or(NULL_DATE),
        country: dict.get_key_from_option(&account_json.country),
        city: dict.get_key_from_option(&account_json.city),
        joined: account_json.joined.unwrap_or(NULL_DATE),
        status: dict.get_key_from_option(&account_json.status),
        interests: Bits::from_vec(account_json.interests.iter().map(|interest| interest_dict.get_key(&interest)).collect()),
        likes: {
            let mut vec: Vec<i32> = account_json.likes.iter().map(|like| &like.id).cloned().collect();
            vec.sort();
            vec.dedup();
            vec
        },
        premium_start: account_json.premium.as_ref().map_or(NULL_DATE, |premium| premium.start),
        premium_finish: account_json.premium.as_ref().map_or(NULL_DATE, |premium| premium.finish),

        is_premium: false,
        recommend_order: 0,
    })
}

fn parse_phone(phone: &str) -> Result<Option<(i32, i32)>, String> {
    if let Some(caps) = PHONE_PATTERN.captures(phone) {
        let phone_number = ("1".to_string() + caps.get(2).unwrap().as_str()).parse().or(Err("cannot parse phone"))?;
        let phone_code = caps.get(1).unwrap().as_str().parse().or(Err("cannot parse phone"))?;
        Ok(Some((phone_code, phone_number)))
    } else {
        Ok(None)
    }
}

fn calc_account_fields(account: &mut Account, now: i32, free_status: i32, hard_status: i32) {
    account.is_premium = account.premium_start != NULL_DATE && account.premium_start <= now && account.premium_finish > now;
    account.recommend_order = if account.is_premium { 0 } else { 3 };
    if account.status == free_status {
        // account.recommend_order += 0;
    } else if account.status == hard_status {
        account.recommend_order += 1;
    } else {
        account.recommend_order += 2;
    }
}

fn update_account_index(consts: &Consts, indexes: &mut Indexes, account: &Account) -> () {
    indexes.known_emails.insert(account.email.as_ref().unwrap().clone());
    indexes.known_phones.insert((account.phone_code, account.phone_number));
    for interest in &account.interests {
        update_index(&mut indexes.interests_index, interest, account.id);
        if account.sex == consts.male {
            update_recommend_index(&mut indexes.recommend_index_male, account, interest);
            update_index(&mut indexes.interests_index_male, interest, account.id);
        } else {
            update_recommend_index(&mut indexes.recommend_index_female, account, interest);
            update_index(&mut indexes.interests_index_female, interest, account.id);
        }
        for interest2 in &account.interests {
            if interest < interest2 {
                let vec = indexes.interests2_index.entry((interest, interest2)).or_insert_with(|| Vec::new());
                insert_into_sorted_vec(account.id, vec)
            }
        }
    }
    update_index(&mut indexes.city_index, account.city, account.id);
    update_index(&mut indexes.country_index, account.country, account.id);
    update_index(&mut indexes.birth_index, year_from_seconds(account.birth), account.id);
    update_index(&mut indexes.fname_index, account.fname, account.id);
    indexes.filter_index.update_account(account, consts);
}

fn update_index(index: &mut HashMap<i32, Vec<i32>>, value: i32, id: i32) {
    if value != 0 {
        let vec = index.entry(value).or_insert_with(|| Vec::new());
        insert_into_sorted_vec(id, vec)
    }
}

fn update_likes_index(consts: &Consts, indexes: &mut Indexes, account: &Account, likee: i32, ts: i32) {
    if account.sex == consts.male {
        let vec = indexes.likes_index_male.entry(likee).or_insert_with(|| Vec::new());
        insert_like_into_sorted_vec(Like { id: account.id, ts }, vec);
    } else {
        let vec = indexes.likes_index_female.entry(likee).or_insert_with(|| Vec::new());
        insert_like_into_sorted_vec(Like { id: account.id, ts }, vec);
    }
}

fn insert_like_into_sorted_vec(value: Like, vec: &mut Vec<Like>) {
    match vec.binary_search_by(|probe| probe.id.cmp(&value.id)) {
        Ok(pos) => vec.insert(pos, value), // чтобы вставить записи с одинаковым id и разным ts, но и полные дубли будут вставлены
        Err(pos) => vec.insert(pos, value),
    }
}

fn update_group_index(indexes: &mut Indexes, account: &Account, incr: i32) {
    indexes.group_index.update_account(account, incr);
}

fn update_recommend_index(index: &mut Vec<[Vec<i32>; 6]>, account: &Account, interest: i32) {
    while index.len() <= interest as usize {
        index.push([Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()]);
    }
    if let Some(array) = index.get_mut(interest as usize) {
        insert_into_sorted_vec(account.id, &mut array[account.recommend_order as usize])
    }
}

impl Dict {
    fn new() -> Dict {
        Dict {
            map: HashMap::new(),
            list: vec![Arc::new(String::new())],
        }
    }

    fn get_key(&mut self, str: &Arc<String>) -> i32 {
        let option = self.map.get(str);
        if option.is_some() {
            *option.unwrap()
        } else {
            let key: i32 = self.list.len() as i32;
            self.map.insert(str.clone(), key);
            self.list.push(str.clone());
            key
        }
    }

    fn get_key_from_option(&mut self, str: &Option<Arc<String>>) -> i32 {
        str.as_ref().map_or(0, |str| self.get_key(str))
    }

    pub fn get_existing_key(&self, str: &String) -> Option<i32> {
        self.map.get(str).map(|v| *v)
    }

    pub fn get_value(&self, key: i32) -> Option<Arc<String>> {
        if key != 0 {
            Some(self.list[key as usize].clone())
        } else {
            None
        }
    }

    pub fn max_key(&self) -> i32 {
        self.list.len() as i32 - 1
    }
}