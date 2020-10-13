use std::sync::Arc;
use anyhow::Result;

mod inner {
    use std::fs::File;
    use std::path::PathBuf;
    use std::collections::BTreeMap;
    use anyhow::Result;

    pub struct DbConfig {
        pub path: PathBuf,
        pub trees: Vec<String>,
    }

    pub struct DbInner {
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

    impl DbInner {
        pub fn new_batch(&self, tree: &str) -> Result<u64> { panic!() }

        pub fn write(&self, tree: &str, batch: u64, key: &[u8], value: &[u8]) -> Result<()> { panic!() }

        pub fn delete(&self, tree: &str, batch: u64, key: &[u8]) -> Result<()> { panic!() }

        pub fn commit_batch(&self, tree: &str, batch: u64) -> Result<()> { panic!() }

        pub fn abort_batch(&self, tree: &str, batch: u64) -> Result<()> { panic!() }

        pub async fn sync(&self, tree: &str) -> Result<()> { panic!() }
    }

    impl DbInner {
        pub fn new_view(&self, tree: &str) -> Result<u64> { panic!() }

        pub fn read(&self, tree: &str, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> { panic!() }

        pub fn close_view(&self, tree: &str, view: u64) -> Result<()> { panic!() }
    }
}

use inner::DbInner;

pub type DbConfig = inner::DbConfig;

#[derive(Clone)]
pub struct Db(Arc<DbInner>);

pub struct WriteBatch;
pub struct ReadView;
pub struct WriteTree;
pub struct ReadTree;

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> { panic!() }

    pub fn read_view(&self) -> ReadView { panic!() }

    pub fn write_batch(&self) -> WriteBatch { panic!() }
}

impl WriteBatch {
    pub fn tree(&self) -> Result<WriteTree> { panic!() }

    pub async fn commit(self) -> Result<()> { panic!() }
}

impl ReadView {
    pub fn tree(&self) -> Result<ReadTree> { panic!() }
}

impl WriteTree {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> { panic!() }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> { panic!() }
}

impl ReadTree {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { panic!() }
}
