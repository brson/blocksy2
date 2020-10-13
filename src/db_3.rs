use std::collections::VecDeque;
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
mod paths;

use fs_thread::FsThread;
use logcmd::LogCommand;

pub struct DbConfig {
    pub path: PathBuf,
    pub trees: Vec<String>,
}

pub struct Db {
    config: DbConfig,
    stores: BTreeMap<String, Store>,
    commit_log: CommitLog,
    next_batch: AtomicU64,
    next_view: AtomicU64,
}

struct Store {
    log: Log,
}

struct Log {
    file: LogFile,
    index: Arc<LogIndex>,
}

type LogCompletionCallback = Arc<dyn Fn(LogCommand, u64) + Send + Sync>;

struct LogFile {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
    completion_cb: LogCompletionCallback,
    errors: Arc<Mutex<BTreeMap<u64, Vec<Error>>>>,
}

struct LogIndex {
    committed: Arc<Mutex<BTreeMap<Vec<u8>, Vec<(u64, IndexEntry)>>>>,
}

enum IndexEntry {
    Filled(u64),
    Deleted,
}

struct CommitLog {
    path: Arc<PathBuf>,
    next_commit: Arc<AtomicU64>,
    fs_thread: Arc<FsThread>,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {
        let fs_thread = FsThread::start()?;
        let fs_thread = Arc::new(fs_thread);
        let mut stores = BTreeMap::new();

        for tree in &config.trees {
            let path = paths::tree_path(&config.path, tree)?;
            let store = Store::new(path, fs_thread.clone()).await?;
            stores.insert(tree.clone(), store);
        }

        let commit_log_path = paths::commit_log_path(&config.path)?;
        let commit_log = CommitLog::new(commit_log_path, fs_thread.clone()).await?;

        return Ok(Db {
            config,
            stores,
            commit_log,
            next_batch: AtomicU64::new(0),
            next_view: AtomicU64::new(0),
        });
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

        last_result?;

        Ok(())
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

        let log_path = paths::log_path(&path)?;
        let log = Log::open(path, fs_thread).await?;

        return Ok(Store {
            log,
        });
    }
}

impl Store {
    fn write(&self, batch: u64, key: &[u8], value: &[u8]) {
        self.log.write(batch, key, value);
    }

    fn delete(&self, batch: u64, key: &[u8]) {
        self.log.delete(batch, key);
    }

    async fn commit_batch(&self, batch: u64) -> Result<()> {
        self.log.commit_batch(batch).await?;

        Ok(())
    }

    fn abort_batch(&self, batch: u64) {
        self.log.abort_batch(batch);
    }
}

impl Store {
    async fn read(&self, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.log.read(view, key).await?)
    }

    fn close_view(&self, view: u64) {
        self.log.close_view(view)
    }
}

impl Log {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<Log> {
        let index = Arc::new(LogIndex::new());
        let completion_index = index.clone();
        let log_completion_cb = Arc::new(move |cmd, offset| {
            match cmd {
                LogCommand::Write { batch, key, .. } => {
                    completion_index.write_offset(batch, &key, offset);
                }
                LogCommand::Delete { batch, key } => {
                    completion_index.delete_offset(batch, &key, offset);
                }
                LogCommand::Commit { batch } => {
                    /* pass */
                }
            }
        });
        let file = LogFile::open(path, fs_thread, log_completion_cb).await?;

        Ok(Log {
            file,
            index,
        })
    }
}

impl Log {
    fn write(&self, batch: u64, key: &[u8], value: &[u8]) {
        self.file.write(batch, key, value);
    }

    fn delete(&self, batch: u64, key: &[u8]) {
        self.file.delete(batch, key);
    }

    async fn commit_batch(&self, batch: u64) -> Result<()> {
        self.file.commit_batch(batch).await?;
        self.index.commit_batch(batch);

        Ok(())
    }

    fn abort_batch(&self, batch: u64) {
        self.index.abort_batch(batch);
        self.file.abort_batch(batch);
    }
}

impl Log {
    async fn read(&self, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let offset = self.index.read_offset(view, key);
        if let Some(offset) = offset {
            Ok(Some(self.file.seek_read(offset, view, key).await?))
        } else {
            Ok(None)
        }
    }

    fn close_view(&self, view: u64) {
        self.index.close_view(view);
        self.file.close_view(view);
    }
}

impl LogFile {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>,
                  completion_cb: LogCompletionCallback) -> Result<LogFile> {
        let path = Arc::new(path);
        let errors = Arc::new(Mutex::new(BTreeMap::new()));

        Ok(LogFile {
            path,
            fs_thread,
            completion_cb,
            errors,
        })
    }
}

impl LogFile {
    fn write(&self, batch: u64, key: &[u8], value: &[u8]) {
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

    fn delete(&self, batch: u64, key: &[u8]) {
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
        let errors = self.fs_thread.run(move |fs| {
            let mut error_guard = self.errors.lock().expect("poison");
            let mut errors = error_guard.remove(&batch).unwrap_or_default();
            drop(error_guard);

            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let offset = log.seek(SeekFrom::End(0))?;
                cmd.write(&mut log)?;
                log.flush()?;
                completion_cb(cmd, offset);
                Ok(())
            })() {
                errors.push(e);
            }

            errors
        }).await;

        for error in errors {
            return Err(error);
        }

        Ok(())
    }

    fn abort_batch(&self, batch: u64) {
        let mut error_guard = self.errors.lock().expect("poison");
        let mut errors = error_guard.remove(&batch).unwrap_or_default();
    }
}

impl LogFile {
    async fn seek_read(&self, offset: u64, view: u64, key: &[u8]) -> Result<Vec<u8>> {
        let path = self.path.clone();
        let cmd = self.fs_thread.run(move |fs| -> Result<LogCommand> {
            let mut log = fs.open_read(&path)?;
            log.seek(SeekFrom::Start(offset))?;
            let cmd = LogCommand::read(&mut log)?;
            Ok(cmd)
        }).await?;

        let entry = match cmd {
            LogCommand::Write { value, .. } => {
                value
            }
            LogCommand::Delete { .. } => {
                panic!("unexpected log delete command");
            }
            LogCommand::Commit { .. } => {
                panic!("unexpected log commit command");
            }
        };

        Ok(entry)
    }

    fn close_view(&self, view: u64) {
        /* noop */
    }
}

impl LogIndex {
    fn new() -> LogIndex {
        LogIndex {
            committed: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl LogIndex {
    fn write_offset(&self, batch: u64, key: &[u8], offset: u64) {
        panic!()
    }

    fn delete_offset(&self, batch: u64, key: &[u8], offset: u64) {
        panic!()
    }

    fn commit_batch(&self, batch: u64) {
        panic!()
    }

    fn abort_batch(&self, batch: u64) {
        panic!()
    }
}

impl LogIndex {
    fn read_offset(&self, view: u64, key: &[u8]) -> Option<u64> {
        panic!()
    }

    fn close_view(&self, view: u64) {
        panic!()
    }
}

impl CommitLog {
    async fn new(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<CommitLog> {
        panic!()
    }
}
