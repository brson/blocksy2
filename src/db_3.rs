use std::sync::Arc;
use std::fs::File;
use std::path::{PathBuf, Path};
use std::collections::BTreeMap;
use anyhow::Result;

mod fs_thread;

use fs_thread::FsThread;

pub struct DbConfig {
    pub path: PathBuf,
    pub trees: Vec<String>,
}

pub struct Db {
    config: DbConfig,
    stores: BTreeMap<String, Store>,
    fs_thread: Arc<FsThread>,
}

struct Store {
    log: Log,
    index: Index,
    next_batch: u64,
}

struct Log {
    path: PathBuf,
    fs_thread: Arc<FsThread>,
}

struct Index {
    location: BTreeMap<Vec<u8>, u64>,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {
        let fs_thread = FsThread::start()?;
        let fs_thread = Arc::new(fs_thread);
        let mut stores = BTreeMap::new();

        for tree in &config.trees {
            let path = tree_path(&config.path, tree)?;
            let store = Store::new(path, fs_thread.clone()).await?;
            stores.insert(tree.clone(), store);
        }

        return Ok(Db {
            config,
            stores,
            fs_thread,
        });

        fn tree_path(dir: &Path, tree: &str) -> Result<PathBuf> {
            panic!()
        }
    }
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

impl Store {
    async fn new(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<Store> {
        panic!()
    }
}
