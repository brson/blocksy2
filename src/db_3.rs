use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    next_batch: AtomicU64,
    next_view: AtomicU64,
}

struct Store {
    log: Log,
    index: Index,
}

struct Log {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
}

struct Index {
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
            next_batch: AtomicU64::new(0),
            next_view: AtomicU64::new(0),
        });

        fn tree_path(dir: &Path, tree: &str) -> Result<PathBuf> {
            panic!()
        }
    }
}

impl Db {
    pub fn new_batch(&self) -> u64 {
        let next = self.next_batch.fetch_add(1, Ordering::Relaxed);
        assert_ne!(next, u64::max_value());
        next
    }

    pub fn write(&self, tree: &str, batch: u64, key: &[u8], value: &[u8]) {
        let store = self.stores.get(tree).expect("tree");
        store.write(batch, key, value);
    }

    pub fn delete(&self, tree: &str, batch: u64, key: &[u8]) {
        let store = self.stores.get(tree).expect("tree");
        store.delete(batch, key);
    }

    pub async fn commit_batch(&self, batch: u64) -> Result<()> {
        let mut last_result = Ok(());
        for store in self.stores.values() {
            if last_result.is_ok() {
                last_result = store.commit_batch(batch).await;
            } else {
                store.abort_batch(batch);
            }
        }

        last_result
    }

    pub fn abort_batch(&self, batch: u64) {
        for store in self.stores.values() {
            store.abort_batch(batch);
        }
    }
}

impl Db {
    pub fn new_view(&self) -> u64 {
        let next = self.next_view.fetch_add(1, Ordering::Relaxed);
        assert_ne!(next, u64::max_value());
        next
    }

    pub async fn read(&self, tree: &str, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let store = self.stores.get(tree).expect("tree");
        Ok(store.read(view, key).await?)
    }

    pub fn close_view(&self, view: u64) {
        for store in self.stores.values() {
            store.close_view(view);
        }
    }
}

impl Store {
    async fn new(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<Store> {

        let log_path = log_path(&path);
        let log = Log::open(path, fs_thread).await?;
        let index = Index::new();

        return Ok(Store {
            log,
            index,
        });

        fn log_path(path: &Path) -> PathBuf {
            panic!()
        }
    }
}

impl Store {

    fn write(&self, batch: u64, key: &[u8], value: &[u8]) {
        self.log.write(batch, key, value);
    }

    fn delete(&self, batch: u64, key: &[u8]) {
        panic!()
    }

    async fn commit_batch(&self, batch: u64) -> Result<()> {
        panic!()
    }

    fn abort_batch(&self, batch: u64) {
        panic!()
    }
}

impl Store {
    async fn read(&self, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        panic!()
    }

    fn close_view(&self, view: u64) {
        panic!()
    }
}

impl Log {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<Log> {
        panic!()
    }
}

impl Log {
    fn write(&self, batch: u64, key: &[u8], value: &[u8]) {
        let path = self.path.clone();
        self.fs_thread.run(|fs| {
            let log = fs.open_append(&path)?;
            Ok(())
        });
    }
}

impl Index {
    fn new() -> Index {
        Index { }
    }
}
