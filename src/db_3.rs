use std::io::{Seek, SeekFrom, Write};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::fs::File;
use std::path::{PathBuf, Path};
use std::collections::BTreeMap;
use anyhow::{Result, Error};

mod fs_thread;
mod logcmd;

use fs_thread::FsThread;
use logcmd::LogCommand;

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
    index: Arc<Index>,
}

type LogCompletionCallback = Arc<dyn Fn(LogCommand, u64) + Send + Sync>;

struct Log {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
    completion_cb: LogCompletionCallback,
    errors: Arc<Mutex<BTreeMap<u64, Vec<Error>>>>,
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
        let index = Arc::new(Index::new());
        let completion_index = index.clone();
        let log_completion_cb = Arc::new(|cmd, offset| {
            panic!()
        });
        let log = Log::open(path, fs_thread, log_completion_cb).await?;

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
        self.log.append_write(batch, key, value);
    }

    fn delete(&self, batch: u64, key: &[u8]) {
        self.log.append_delete(batch, key);
    }

    async fn commit_batch(&self, batch: u64) -> Result<()> {
        self.log.commit_batch(batch);

        Ok(())
    }

    fn abort_batch(&self, batch: u64) {
        panic!()
    }
}

impl Store {
    async fn read(&self, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let offset = self.index.get_offset(view, key);
        if let Some(offset) = offset {
            Ok(Some(self.log.seek_read(offset, view, key).await?))
        } else {
            Ok(None)
        }
    }

    fn close_view(&self, view: u64) {
        panic!()
    }
}

impl Log {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>,
                  completion_cb: LogCompletionCallback) -> Result<Log> {
        panic!()
    }
}

impl Log {
    fn append_write(&self, batch: u64, key: &[u8], value: &[u8]) {
        let path = self.path.clone();
        let cmd = LogCommand::Write {
            batch,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        self.fs_thread.run(move |fs| {
            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let offset = log.seek(SeekFrom::End(0))?;
                cmd.write(&mut log)?;
                completion_cb(cmd, offset);
                Ok(())
            })() {
                let mut errors = errors.lock().expect("poison");
                let mut errors = errors.entry(batch).or_default();
                errors.push(e);
            }
        });
    }

    fn append_delete(&self, batch: u64, key: &[u8]) {
        let path = self.path.clone();
        let cmd = LogCommand::Delete {
            batch,
            key: key.to_vec(),
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        self.fs_thread.run(move |fs| {
            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let offset = log.seek(SeekFrom::End(0))?;
                cmd.write(&mut log)?;
                completion_cb(cmd, offset);
                Ok(())
            })() {
                let mut errors = errors.lock().expect("poison");
                let mut errors = errors.entry(batch).or_default();
                errors.push(e);
            }
        });
    }

    async fn commit_batch(&self, batch: u64) -> Result<()> {
        let path = self.path.clone();
        let cmd = LogCommand::Commit {
            batch,
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        self.fs_thread.run(move |fs| {
            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let offset = log.seek(SeekFrom::End(0))?;
                cmd.write(&mut log)?;
                log.flush()?;
                completion_cb(cmd, offset);
                Ok(())
            })() {
                let mut errors = errors.lock().expect("poison");
                let mut errors = errors.entry(batch).or_default();
                errors.push(e);
            }
        });

        let mut error_guard = self.errors.lock().expect("poison");
        let mut errors = error_guard.remove(&batch).unwrap_or_default();

        drop(error_guard);

        for error in errors {
            return Err(error);
        }

        Ok(())
    }
}

impl Log {
    async fn seek_read(&self, offset: u64, view: u64, key: &[u8]) -> Result<Vec<u8>> {
        panic!()
    }
}

impl Index {
    fn new() -> Index {
        Index { }
    }
}

impl Index {
    fn set_offset(&self, view: u64, key: &[u8], offset: u64) -> Option<u64> {
        panic!()
    }
}

impl Index {
    fn get_offset(&self, view: u64, key: &[u8]) -> Option<u64> {
        panic!()
    }
}
