use std::fs::File;
use std::path::PathBuf;
use std::collections::BTreeMap;
use anyhow::Result;

pub struct DbConfig {
    pub path: PathBuf,
    pub trees: Vec<String>,
}

pub struct Db {
    config: DbConfig,
    stores: BTreeMap<String, Store>,
}

struct Store {
    log: Log,
    index: Index,
    next_batch: u64,
}

struct Log {
    file: File,
}

struct Index {
    location: BTreeMap<Vec<u8>, u64>,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> { panic!() }
}

impl Db {
    pub fn new_batch(&self) -> u64 { panic!() }

    pub fn write(&self, tree: &str, batch: u64, key: &[u8], value: &[u8]) { panic!() }

    pub fn delete(&self, tree: &str, batch: u64, key: &[u8]) { panic!() }

    pub async fn commit_batch(&self, batch: u64) -> Result<()> { panic!() }

    pub fn abort_batch(&self, batch: u64) { panic!() }
}

impl Db {
    pub fn new_view(&self) -> u64 { panic!() }

    pub async fn read(&self, tree: &str, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> { panic!() }

    pub fn close_view(&self, view: u64) { panic!() }
}
