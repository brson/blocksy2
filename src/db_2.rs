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
    view: imp::View,
    tree: String,
    cursor: imp::Cursor,
    value: Option<Vec<u8>>,
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
        let db = self.view.db.clone();
        let view = self.view.view;
        let tree = self.tree.clone();
        let cursor = self.view.db.cursor(view, &tree);
        let value = None;

        Cursor {
            db,
            view,
            tree,
            cursor,
            value,
        }
    }
}

impl Cursor {
    pub fn valid(&self) -> bool {
        self.cursor.valid()
    }

    pub async fn next(&mut self) -> Result<()> {
        self.value = None;
        self.cursor.next();
        panic!()
    }

    pub async fn prev(&mut self) -> Result<()> {
        self.value = None;
        self.cursor.prev();
        panic!()
    }

    pub fn key_value(&self) -> (&[u8], &[u8]) {
        panic!()
    }

    pub async fn seek_first(&mut self) -> Result<()> {
        self.value = None;
        self.cursor.seek_first();
        panic!()
    }

    pub async fn seek_last(&mut self) -> Result<()> {
        self.value = None;
        self.cursor.seek_last();
        panic!()
    }

    pub async fn seek_key(&mut self, key: &[u8]) -> Result<()> {
        self.value = None;
        self.cursor.seek_key(key);
        panic!()
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        self.db.close_view(self.view);
    }
}
