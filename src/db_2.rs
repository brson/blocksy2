use std::sync::Arc;
use anyhow::Result;

#[path = "db_3.rs"]
mod imp;

pub type DbConfig = imp::DbConfig;

#[derive(Clone)]
pub struct Db(Arc<imp::Db>);

pub struct WriteBatch {
    db: Arc<imp::Db>,
    batch: u64,
    destructed: bool,
}

pub struct ReadView {
    db: Arc<imp::Db>,
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
        imp::Db::open(config).await.map(Arc::new).map(Db)
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
    pub fn write(&self, key: &[u8], value: &[u8]) {
        self.batch.db.write(&self.tree, self.batch.batch, key, value)
    }

    pub fn delete(&self, key: &[u8]) {
        self.batch.db.delete(&self.tree, self.batch.batch, key)
    }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.view.db.read(&self.tree, self.view.view, key).await?)
    }
}
