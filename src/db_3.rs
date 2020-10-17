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
    logs: BTreeMap<String, Log>,
    next_batch: AtomicU64,
    next_commit: AtomicU64,
    next_view: AtomicU64,
    view_commit_limit: Arc<AtomicU64>,
    commit_lock: Mutex<()>,
    commit_log: CommitLog,
    batch_commit_map: BatchCommitMap,
    view_commit_limit_map: ViewCommitMap,
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

#[derive(Copy, Clone)]
enum IndexEntry {
    Filled(Offset),
    Deleted(Offset),
}

struct CommitLog {
    path: Arc<PathBuf>,
    fs_thread: Arc<FsThread>,
}

#[derive(Clone)]
struct ListNode(Arc<(Mutex<Option<ListNode>>, Mutex<Option<ListNode>>, Vec<u8>)>);

pub struct Cursor {
    view: View,
    first: Option<ListNode>,
    last: Option<ListNode>,
    curr: Option<ListNode>,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {

        fs::create_dir_all(&config.data_dir)?;
        
        let fs_thread = FsThread::start()?;
        let fs_thread = Arc::new(fs_thread);

        let commit_log_path = paths::commit_log_path(&config.data_dir)?;
        let (commit_log, batch_commit_map) =
            CommitLog::open(commit_log_path, fs_thread.clone()).await?;

        let commit_batch_map: BTreeMap<Commit, Batch> = batch_commit_map
            .lock().expect("poison")
            .iter().map(|(batch, commit)| (*commit, *batch))
            .collect();

        let view_commit_limit_map = Arc::new(Mutex::new(BTreeMap::new()));

        let mut logs = BTreeMap::new();

        let mut max_batch = None;

        for tree in &config.trees {
            let tree_path_stem = paths::tree_path_stem(&config.data_dir, tree)?;
            let log_path = paths::log_path(&tree_path_stem)?;
            let (log, log_max_batch) =
                Log::open(log_path, fs_thread.clone(),
                          batch_commit_map.clone(),
                          &commit_batch_map,
                          view_commit_limit_map.clone()).await?;
            if let Some(log_max_batch) = log_max_batch {
                max_batch = Some(u64::max(max_batch.unwrap_or(0), log_max_batch.0));
            }
            logs.insert(tree.clone(), log);
        }

        let next_batch = max_batch
            .map(|b| b.checked_add(1).expect("overflow"))
            .unwrap_or(0);
        let next_commit = batch_commit_map
            .lock().expect("poison")
            .values().max()
            .map(|c| c.0.checked_add(1).expect("overflow"))
            .unwrap_or(0);
        let next_view = 0;
        let view_commit_limit = next_commit;

        let next_batch = AtomicU64::new(next_batch);
        let next_commit = AtomicU64::new(next_commit);
        let next_view = AtomicU64::new(next_view);
        let view_commit_limit = Arc::new(AtomicU64::new(view_commit_limit));

        let commit_lock = Mutex::new(());

        return Ok(Db {
            config,
            logs,
            next_batch,
            next_commit,
            next_view,
            view_commit_limit,
            commit_lock,
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
        let log = self.logs.get(tree).expect("tree");
        log.write(batch, key, value);
    }

    pub fn delete(&self, tree: &str, batch: Batch, key: &[u8]) {
        let log = self.logs.get(tree).expect("tree");
        log.delete(batch, key);
    }

    pub async fn commit_batch(&self, batch: Batch) -> Result<()> {
        let mut last_result = Ok(());
        for log in self.logs.values() {
            if last_result.is_ok() {
                last_result = log.pre_commit_batch(batch).await;
            } else {
                log.abort_batch(batch);
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
            for log in self.logs.values() {
                log.commit_batch(batch);
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
        for log in self.logs.values() {
            log.abort_batch(batch);
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
        let log = self.logs.get(tree).expect("tree");
        Ok(log.read(view, key).await?)
    }

    pub fn close_view(&self, view: View) {
        let mut map = self.view_commit_limit_map.lock().expect("poison");
        let old = map.remove(&view);
        assert!(old.is_some());
    }
}

impl Db {
    pub fn cursor(&self, view: View, tree: &str) -> Cursor {
        let view = self.clone_view(view);
        let log = self.logs.get(tree).expect("tree");
        let (first, last) = log.cursor_list(view);
        let curr = None;

        Cursor {
            view,
            first,
            last,
            curr,
        }
    }

    pub fn close_cursor(&self, cursor: Cursor) {
        self.close_view(cursor.view);
        drop(cursor);
    }
}

impl Db {
    fn clone_view(&self, view: View) -> View {
        let new_view = self.next_view.fetch_add(1, Ordering::Relaxed);
        assert_ne!(new_view, u64::max_value());
        let new_view = View(new_view);
        {
            let mut map = self.view_commit_limit_map.lock().expect("poison");
            let commit = *map.get(&view).expect("view");
            assert!(!map.contains_key(&new_view));
            map.insert(new_view, commit);
        }
        new_view
    }
}

impl Log {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>,
                  batch_commit_map: BatchCommitMap,
                  commit_batch_map: &BTreeMap<Commit, Batch>,
                  view_commit_limit_map: ViewCommitMap) -> Result<(Log, Option<Batch>)> {
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
        let (file, max_batch) = LogFile::open(path, fs_thread, log_completion_cb).await?;

        for batch in commit_batch_map.values() {
            index.commit_batch(*batch);
        }

        let log = Log { file, index };

        Ok((log, max_batch))
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
}

impl Log {
    fn cursor_list(&self, view: View) -> (Option<ListNode>, Option<ListNode>) {
        self.index.cursor_list(view)
    }
}

impl LogFile {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>,
                  completion_cb: LogCompletionCallback) -> Result<(LogFile, Option<Batch>)> {
        let path = Arc::new(path);

        let path_clone = path.clone();
        let completion_cb_clone = completion_cb.clone();
        let max_batch = fs_thread.run(move |fs| -> Result<Option<Batch>> {
            let path = path_clone;
            let completion_cb = completion_cb_clone;

            let mut log = fs.open_read(&path)?;
            let end = log.seek(SeekFrom::End(0))?;

            log.seek(SeekFrom::Start(0))?;

            let mut max_batch = None;

            loop {
                let offset = log.seek(SeekFrom::Current(0))?;
                if offset == end {
                    break;
                }

                let cmd = LogCommand::read(&mut log)?;

                let batch = match &cmd {
                    LogCommand::Write { batch, .. } |
                    LogCommand::Delete { batch, .. } |
                    LogCommand::Commit { batch } => {
                        if let Some(max_batch_) = max_batch {
                            max_batch = Some(u64::max(max_batch_, *batch));
                        } else {
                            max_batch = Some(*batch);
                        }
                    }                    
                };

                completion_cb(cmd, Offset(offset));
            }

            Ok(max_batch.map(Batch))
        }).await?;

        let errors = Arc::new(Mutex::new(BTreeMap::new()));

        let file = LogFile {
            path,
            fs_thread,
            completion_cb,
            errors,
        };

        Ok((file, max_batch))
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
            self.latest_offset_in_view(entries, view_commit_limit)
        } else {
            None
        }
    }
}

impl LogIndex {
    fn cursor_list(&self, view: View) -> (Option<ListNode>, Option<ListNode>) {
        let view_commit_limit = {
            let mut map = self.view_commit_limit_map.lock().expect("poison");
            *map.get(&view).expect("view-commit")
        };

        let mut first = None;
        let mut curr = None;
        let map = self.committed.lock().expect("poison");
        for (key, entries) in map.iter() {
            let offset = self.latest_offset_in_view(entries, view_commit_limit);
            if offset.is_none() {
                continue;
            }

            let key = key.to_vec();
            let prev = curr.clone();
            curr = Some(ListNode(Arc::new((Mutex::new(prev.clone()), Mutex::new(None), key))));

            if let Some(prev) = prev {
                let mut prev_next = prev.0.1.lock().expect("poison");
                *prev_next = curr.clone();
            }

            if first.is_none() {
                first = curr.clone();
            }
        }

        let last = curr;

        (first, last)
    }
}

impl LogIndex {
    fn latest_offset_in_view(&self,
                             entries: &[(Batch, IndexEntry)],
                             view_commit_limit: Commit) -> Option<Offset> {
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
                    };
                }
            }
        }

        None
    }
}

impl CommitLog {
    async fn open(path: PathBuf, fs_thread: Arc<FsThread>) -> Result<(CommitLog, BatchCommitMap)> {
        let path = Arc::new(path);
        let batch_commit_map = Arc::new(Mutex::new(BTreeMap::new()));

        let path_clone = path.clone();
        let batch_commit_map_clone = batch_commit_map.clone();
        fs_thread.run(move |fs| -> Result<()> {
            let path = path_clone;
            let batch_commit_map = batch_commit_map_clone;

            let mut batch_commit_map = batch_commit_map.lock().expect("poison");

            let mut log = fs.open_read(&path)?;
            let end = log.seek(SeekFrom::End(0))?;

            log.seek(SeekFrom::Start(0))?;

            loop {
                let offset = log.seek(SeekFrom::Current(0))?;
                if offset == end {
                    break;
                }
                
                let cmd = CommitLogCommand::read(&mut log)?;
                match cmd {
                    CommitLogCommand::Commit { commit, batch } => {
                        assert!(!batch_commit_map.contains_key(&Batch(batch)));
                        batch_commit_map.insert(Batch(batch), Commit(commit));
                    }
                }
            }

            Ok(())
        }).await?;

        let commit_log = CommitLog {
            path,
            fs_thread,
        };

        Ok((commit_log, batch_commit_map))
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
        self.fs_thread.run(move |fs| -> Result<()> {
            let mut log = fs.open_append(&path)?;
            cmd.write(&mut log)?;
            completion_cb();

            Ok(())
        }).await?;

        Ok(())
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        panic!()
    }

    pub fn next(&self) -> Result<()> {
        panic!()
    }

    pub fn prev(&self) -> Result<()> {
        panic!()
    }

    pub fn key(&self) -> &[u8] {
        panic!()
    }

    pub fn seek_first(&self) -> Result<()> {
        panic!()
    }

    pub fn seek_last(&self) -> Result<()> {
        panic!()
    }

    pub fn seek_key(&self, key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        let mut node = self.curr.clone();

        while let Some(mut node_) = node {
            let mut prev = node_.0.0.lock().expect("poison");
            *prev = None;
            let mut next = node_.0.1.lock().expect("poison");
            node = next.clone();
            *next = None;
        }
    }
}
