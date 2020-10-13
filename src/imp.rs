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
        pub async fn open(config: DbConfig) -> Result<DbInner> { panic!() }
    }

    impl DbInner {
        pub fn new_batch(&self) -> u64 { panic!() }

        pub fn write(&self, tree: &str, batch: u64, key: &[u8], value: &[u8]) { panic!() }

        pub fn delete(&self, tree: &str, batch: u64, key: &[u8]) { panic!() }

        pub async fn commit_batch(&self, batch: u64) -> Result<()> { panic!() }

        pub fn abort_batch(&self, batch: u64) { panic!() }
    }

    impl DbInner {
        pub fn new_view(&self) -> u64 { panic!() }

        pub async fn read(&self, tree: &str, view: u64, key: &[u8]) -> Result<Option<Vec<u8>>> { panic!() }

        pub fn close_view(&self, view: u64) { panic!() }
    }
}

use inner::DbInner;

pub type DbConfig = inner::DbConfig;

#[derive(Clone)]
pub struct Db(Arc<DbInner>);

pub struct WriteBatch {
    db: Arc<DbInner>,
    batch: u64,
    destructed: bool,
}

pub struct ReadView {
    db: Arc<DbInner>,
    view: u64,
}

pub struct WriteTree<'batch> {
    batch: &'batch WriteBatch,
    tree: String,
}

pub struct ReadTree<'view> {
    view: &'view ReadView,
    tree: String,
}

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> {
        DbInner::open(config).await.map(Arc::new).map(Db)
    }

    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch {
            db: self.0.clone(),
            batch: self.0.new_batch(),
            destructed: false,
        }
    }

    pub fn read_view(&self) -> ReadView {
        ReadView {
            db: self.0.clone(),
            view: self.0.new_view(),
        }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        if !self.destructed {
            panic!("write batch not committed or aborted");
        }
    }
}

impl Drop for ReadView {
    fn drop(&mut self) {
        self.db.close_view(self.view);
    }
}

impl WriteBatch {
    pub fn tree<'batch>(&'batch self, tree: &str) -> WriteTree<'batch> {
        WriteTree {
            batch: self,
            tree: tree.to_string(),
        }
    }

    pub async fn commit(self) -> Result<()> {
        Ok(self.db.commit_batch(self.batch).await?)
    }

    pub fn abort(self) {
        self.db.abort_batch(self.batch)
    }
}

impl ReadView {
    pub fn tree<'view>(&'view self, tree: &str) -> ReadTree<'view> {
        ReadTree {
            view: self,
            tree: tree.to_string(),
        }
    }
}

impl<'batch> WriteTree<'batch> {
    pub fn write(&mut self, key: &[u8], value: &[u8]) { panic!() }

    pub fn delete(&mut self, key: &[u8]) { panic!() }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { panic!() }
}
