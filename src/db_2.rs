use std::sync::Arc;
use anyhow::Result;
use log::warn;

#[path = "db_3.rs"]
mod imp;

pub type DbConfig = imp::DbConfig;

#[derive(Clone)]
pub struct Db(Arc<imp::Db>);

pub struct WriteBatch {
    db: Arc<imp::Db>,
    batch: imp::Batch,
    destructed: bool,
}

pub struct ReadView {
    db: Arc<imp::Db>,
    view: imp::View,
}

pub struct WriteTree<'batch> {
    batch: &'batch WriteBatch,
    tree: String,
}

pub struct ReadTree<'view> {
    view: &'view ReadView,
    tree: String,
}

pub struct Cursor {
    db: Arc<imp::Db>,
    cursor: imp::Cursor,
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
            warn!("write batch dropped without committing or aborting. aborting now");
            self.db.abort_batch(self.batch);
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

    pub fn cursor(&self) -> Cursor {
        Cursor {
            db: self.view.db.clone(),
            cursor: self.view.db.cursor(self.view.view, &self.tree),
        }
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        self.cursor.valid()
    }

    pub fn next(&self) -> Result<()> {
        self.cursor.next()
    }

    pub fn prev(&self) -> Result<()> {
        self.cursor.prev()
    }

    pub fn key_value(&self) -> (&[u8], &[u8]) {
        panic!()
    }

    pub fn seek_first(&self) -> Result<()> {
        self.cursor.seek_first()
    }

    pub fn seek_last(&self) -> Result<()> {
        self.cursor.seek_last()
    }

    pub fn seek_key(&self, key: &[u8]) -> Result<()> {
        self.cursor.seek_key(key)
    }
}
