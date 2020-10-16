use std::collections::VecDeque;
use std::io::{Seek, SeekFrom, Write};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::fs::{self, File};
use std::path::{PathBuf, Path};
use std::collections::BTreeMap;
use anyhow::{Result, Error};

mod fs_thread;
mod logcmd;
mod paths;

use fs_thread::FsThread;
use logcmd::{LogCommand, CommitLogCommand};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Batch(u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct View(u64);
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Commit(u64);
#[derive(Copy, Clone)]
pub struct CommitLimit(u64);

#[derive(Copy, Clone)]
struct Size(u64);
#[derive(Copy, Clone)]
struct Offset(u64);

type BatchCommitMap = Arc<Mutex<BTreeMap<Batch, Commit>>>;
type ViewCommitMap = Arc<Mutex<BTreeMap<View, Commit>>>;

pub struct DbConfig {
    pub data_dir: PathBuf,
    pub trees: Vec<String>,
}

pub struct Db {
    config: DbConfig,
    stores: BTreeMap<String, Store>,
    next_batch: AtomicU64,
    next_commit: AtomicU64,
    next_view: AtomicU64,
    view_commit_limit: Arc<AtomicU64>,
    commit_lock: Mutex<()>,
    commit_log: CommitLog,
    batch_commit_map: BatchCommitMap,
    view_commit_limit_map: ViewCommitMap,
}

struct Store {
    log: Log,
}

struct Log {
    file: LogFile,
    index: Arc<LogIndex>,
}

type LogCompletionCallback = Arc<dyn Fn(LogCommand, Offset) + Send + Sync>;

struct LogFile {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
    completion_cb: LogCompletionCallback,
    errors: Arc<Mutex<BTreeMap<Batch, Vec<Error>>>>,
}

struct LogIndex {
    committed: Arc<Mutex<BTreeMap<Vec<u8>, Vec<(Batch, IndexEntry)>>>>,
    uncommitted: Arc<Mutex<BTreeMap<Batch, Vec<(Vec<u8>, IndexEntry)>>>>,
    batch_commit_map: BatchCommitMap,
    view_commit_limit_map: ViewCommitMap,
}

enum IndexEntry {
    Filled(Offset),
    Deleted(Offset),
}

struct CommitLog {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {

        fs::create_dir_all(&config.data_dir)?;
        
        let fs_thread = FsThread::start()?;
        let fs_thread = Arc::new(fs_thread);

        let batch_commit_map = Arc::new(Mutex::new(BTreeMap::new()));
        let view_commit_limit_map = Arc::new(Mutex::new(BTreeMap::new()));

        let mut stores = BTreeMap::new();

        for tree in &config.trees {
            let tree_path_stem = paths::tree_path_stem(&config.data_dir, tree)?;
            let store = Store::new(tree_path_stem, fs_thread.clone(),
                                   batch_commit_map.clone(),
                                   view_commit_limit_map.clone()).await?;
            stores.insert(tree.clone(), store);
        }

        let commit_log_path = paths::commit_log_path(&config.data_dir)?;
        let commit_log = CommitLog::new(commit_log_path, fs_thread.clone()).await?;

        return Ok(Db {
            config,
            stores,
            next_batch: AtomicU64::new(0),
            next_commit: AtomicU64::new(0),
            next_view: AtomicU64::new(0),
            view_commit_limit: Arc::new(AtomicU64::new(0)),
            commit_lock: Mutex::new(()),
            commit_log,
            batch_commit_map,
            view_commit_limit_map,
        });
    }
}

impl Db {
    pub fn new_batch(&self) -> Batch {
        let next = self.next_batch.fetch_add(1, Ordering::Relaxed);
        assert_ne!(next, u64::max_value());
        Batch(next)
    }

    pub fn write(&self, tree: &str, batch: Batch, key: &[u8], value: &[u8]) {
        let store = self.stores.get(tree).expect("tree");
        store.write(batch, key, value);
    }

    pub fn delete(&self, tree: &str, batch: Batch, key: &[u8]) {
        let store = self.stores.get(tree).expect("tree");
        store.delete(batch, key);
    }

    pub async fn commit_batch(&self, batch: Batch) -> Result<()> {
        let mut last_result = Ok(());
        for store in self.stores.values() {
            if last_result.is_ok() {
                last_result = store.pre_commit_batch(batch).await;
            } else {
                store.abort_batch(batch);
            }
        }

        last_result?;

        let future = {
            let _commit_guard = self.commit_lock.lock().expect("poison");

            let commit = self.next_commit.fetch_add(1, Ordering::Relaxed);
            assert_ne!(commit, u64::max_value());
            let commit = Commit(commit);

            // This step promotes all log index caches for the batch
            // from uncommitted to committed. It must be done under lock
            // so that the index keeps batches in commit order.
            for store in self.stores.values() {
                store.commit_batch(batch);
            }

            // Write the final commit confirmation to disk at some point in the
            // future, preserving the order of commits, then run a completion
            // that publishes the commit to readers.
            let batch_commit_map = self.batch_commit_map.clone();
            let view_commit_limit = self.view_commit_limit.clone();
            self.commit_log.commit_batch(batch, commit, move || {
                let mut map = batch_commit_map.lock().expect("poison");
                assert!(!map.contains_key(&batch));
                map.insert(batch, commit);
                drop(map);

                let new_view_commit_limit = commit.0.checked_add(1).expect("view_commit_limit overflow");
                view_commit_limit.store(new_view_commit_limit, Ordering::Relaxed);
            })
        };

        future.await?;

        Ok(())
    }

    pub fn abort_batch(&self, batch: Batch) {
        for store in self.stores.values() {
            store.abort_batch(batch);
        }
    }
}

impl Db {
    pub fn new_view(&self) -> View {
        let view = self.next_view.fetch_add(1, Ordering::Relaxed);
        assert_ne!(view, u64::max_value());
        let view = View(view);
        let commit = self.view_commit_limit.load(Ordering::Relaxed);
        let commit = Commit(commit);
        {
            let mut map = self.view_commit_limit_map.lock().expect("poison");
            assert!(!map.contains_key(&view));
            map.insert(view, commit);
        }
        view
    }

    pub async fn read(&self, tree: &str, view: View, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let store = self.stores.get(tree).expect("tree");
        Ok(store.read(view, key).await?)
    }

    pub fn close_view(&self, view: View) {
        for store in self.stores.values() {
            store.close_view(view);
        }

        {
            let mut map = self.view_commit_limit_map.lock().expect("poison");
            let old = map.remove(&view);
            assert!(old.is_some());
        }
    }
}

impl Store {
    async fn new(tree_path_stem: PathBuf, fs_thread: Arc<FsThread>,
                 batch_commit_map: BatchCommitMap,
                 view_commit_limit_map: ViewCommitMap) -> Result<Store> {

        let path = paths::log_path(&tree_path_stem)?;
        let log = Log::open(path, fs_thread,
                            batch_commit_map,
                            view_commit_limit_map).await?;

        return Ok(Store {
            log,
        });
    }
}

impl Store {
    fn write(&self, batch: Batch, key: &[u8], value: &[u8]) {
        self.log.write(batch, key, value);
    }

    fn delete(&self, batch: Batch, key: &[u8]) {
        self.log.delete(batch, key);
    }

    async fn pre_commit_batch(&self, batch: Batch) -> Result<()> {
        self.log.pre_commit_batch(batch).await?;

        Ok(())
    }

    fn commit_batch(&self, batch: Batch) {
        self.log.commit_batch(batch);
    }

    fn abort_batch(&self, batch: Batch) {
        self.log.abort_batch(batch);
    }
}

impl Store {
    async fn read(&self, view: View, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.log.read(view, key).await?)
    }

    fn close_view(&self, view: View) {
        self.log.close_view(view)
    }
}

impl Log {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>,
                  batch_commit_map: BatchCommitMap,
                  view_commit_limit_map: ViewCommitMap) -> Result<Log> {
        let index = Arc::new(LogIndex::new(batch_commit_map, view_commit_limit_map));
        let completion_index = index.clone();
        let log_completion_cb = Arc::new(move |cmd, offset| {
            match cmd {
                LogCommand::Write { batch, key, .. } => {
                    completion_index.write_offset(Batch(batch), &key, offset);
                }
                LogCommand::Delete { batch, key } => {
                    completion_index.delete_offset(Batch(batch), &key, offset);
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
    fn write(&self, batch: Batch, key: &[u8], value: &[u8]) {
        self.file.write(batch, key, value);
    }

    fn delete(&self, batch: Batch, key: &[u8]) {
        self.file.delete(batch, key);
    }

    async fn pre_commit_batch(&self, batch: Batch) -> Result<()> {
        self.file.pre_commit_batch(batch).await?;
        self.index.pre_commit_batch(batch);

        Ok(())
    }

    fn commit_batch(&self, batch: Batch) {
        self.index.commit_batch(batch);
    }

    fn abort_batch(&self, batch: Batch) {
        self.index.abort_batch(batch);
        self.file.abort_batch(batch);
    }
}

impl Log {
    async fn read(&self, view: View, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let offset = self.index.read_offset(view, key);
        if let Some(offset) = offset {
            Ok(Some(self.file.seek_read(offset, view, key).await?))
        } else {
            Ok(None)
        }
    }

    fn close_view(&self, view: View) {
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
    fn write(&self, batch: Batch, key: &[u8], value: &[u8]) {
        let path = self.path.clone();
        let cmd = LogCommand::Write {
            batch: batch.0,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        self.fs_thread.run(move |fs| {
            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let offset = log.seek(SeekFrom::Current(0))?;
                let bytes = cmd.write(&mut log)?;
                let offset = log.seek(SeekFrom::Current(0))?;
                let offset = offset.checked_sub(bytes).expect("offset");
                completion_cb(cmd, Offset(offset));
                Ok(())
            })() {
                let mut errors = errors.lock().expect("poison");
                let mut errors = errors.entry(batch).or_default();
                errors.push(e);
            }
        });
    }

    fn delete(&self, batch: Batch, key: &[u8]) {
        let path = self.path.clone();
        let cmd = LogCommand::Delete {
            batch: batch.0,
            key: key.to_vec(),
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        self.fs_thread.run(move |fs| {
            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let bytes = cmd.write(&mut log)?;
                let offset = log.seek(SeekFrom::Current(0))?;
                let offset = offset.checked_sub(bytes).expect("offset");
                completion_cb(cmd, Offset(offset));
                Ok(())
            })() {
                let mut errors = errors.lock().expect("poison");
                let mut errors = errors.entry(batch).or_default();
                errors.push(e);
            }
        });
    }

    async fn pre_commit_batch(&self, batch: Batch) -> Result<()> {
        let path = self.path.clone();
        let cmd = LogCommand::Commit {
            batch: batch.0,
        };
        let errors = self.errors.clone();
        let completion_cb = self.completion_cb.clone();
        let errors = self.fs_thread.run(move |fs| {
            let mut error_guard = errors.lock().expect("poison");
            let mut errors = error_guard.remove(&batch).unwrap_or_default();
            drop(error_guard);

            if let Err(e) = (|| -> Result<()> {
                let mut log = fs.open_append(&path)?;
                let bytes = cmd.write(&mut log)?;
                let offset = log.seek(SeekFrom::Current(0))?;
                let offset = offset.checked_sub(bytes).expect("offset");
                log.flush()?;
                completion_cb(cmd, Offset(offset));
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

    fn abort_batch(&self, batch: Batch) {
        let errors = {
            let mut errors = self.errors.lock().expect("poison");
            errors.remove(&batch)
        };

        drop(errors);
    }
}

impl LogFile {
    async fn seek_read(&self, offset: Offset, view: View, key: &[u8]) -> Result<Vec<u8>> {
        let path = self.path.clone();
        let cmd = self.fs_thread.run(move |fs| -> Result<LogCommand> {
            let mut log = fs.open_read(&path)?;
            log.seek(SeekFrom::Start(offset.0))?;
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

    fn close_view(&self, view: View) {
        /* noop */
    }
}

impl LogIndex {
    fn new(batch_commit_map: BatchCommitMap, view_commit_limit_map: ViewCommitMap) -> LogIndex {
        LogIndex {
            committed: Arc::new(Mutex::new(BTreeMap::new())),
            uncommitted: Arc::new(Mutex::new(BTreeMap::new())),
            batch_commit_map,
            view_commit_limit_map,
        }
    }
}

impl LogIndex {
    fn write_offset(&self, batch: Batch, key: &[u8], offset: Offset) {
        let key = key.to_vec();
        let new_entry = IndexEntry::Filled(offset);
        let mut map = self.uncommitted.lock().expect("poison");
        let mut entries = map.entry(batch).or_default();
        entries.push((key, new_entry));
    }

    fn delete_offset(&self, batch: Batch, key: &[u8], offset: Offset) {
        let key = key.to_vec();
        let new_entry = IndexEntry::Deleted(offset);
        let mut map = self.uncommitted.lock().expect("poison");
        let mut entries = map.entry(batch).or_default();
        entries.push((key, new_entry));
    }

    fn pre_commit_batch(&self, batch: Batch) {
        /* noop */
    }

    fn commit_batch(&self, batch: Batch) {
        // Move index entries from uncommitted to committed. The caller will
        // ensure that this is done in the order batches are committed. This
        // will not have any effect on readers until the batch-commit map is
        // updated.

        let uncommitted = {
            let mut uncommitted = self.uncommitted.lock().expect("poison");
            uncommitted.remove(&batch).unwrap_or_default()
        };

        {
            let mut committed = self.committed.lock().expect("poison");
            for new in uncommitted {
                let mut kvlist = committed.entry(new.0).or_default();
                kvlist.push((batch, new.1));
            }
        }
    }

    fn abort_batch(&self, batch: Batch) {
        let uncommitted = {
            let mut uncommitted = self.uncommitted.lock().expect("poison");
            uncommitted.remove(&batch).unwrap_or_default()
        };

        drop(uncommitted);
    }
}

impl LogIndex {
    fn read_offset(&self, view: View, key: &[u8]) -> Option<Offset> {
        let view_commit_limit = {
            let mut map = self.view_commit_limit_map.lock().expect("poison");
            *map.get(&view).expect("view-commit")
        };

        let committed = self.committed.lock().expect("poison");
        let entries = committed.get(key);

        if let Some(entries) = entries {
            for entry in entries.iter().rev() {
                let batch = entry.0;
                let batch_commit = {
                    let batch_commit_map = self.batch_commit_map.lock().expect("poison");
                    batch_commit_map.get(&batch).copied()
                };
                if let Some(batch_commit) = batch_commit {
                    if batch_commit < view_commit_limit {
                        return match entry.1 {
                            IndexEntry::Filled(offset) => Some(offset),
                            IndexEntry::Deleted(_) => None,
                        }
                    }
                }
            }

            None
        } else {
            None
        }
    }

    fn close_view(&self, view: View) {
        /* noop */
    }
}

impl CommitLog {
    async fn new(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<CommitLog> {
        Ok(CommitLog {
            path: Arc::new(path),
            fs_thread,
        })
    }
}

impl CommitLog {
    async fn commit_batch<F>(&self, batch: Batch, commit: Commit,
                             completion_cb: F) -> Result<()>
    where F: FnOnce() + Send + 'static
    {
        let path = self.path.clone();
        let cmd = CommitLogCommand::Commit {
            commit: commit.0,
            batch: batch.0,
        };
        let future = self.fs_thread.run(move |fs| -> Result<()> {
            let mut log = fs.open_append(&path)?;
            cmd.write(&mut log)?;
            completion_cb();

            Ok(())
        });

        future.await?;

        Ok(())
    }
}
