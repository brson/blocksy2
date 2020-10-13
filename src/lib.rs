#![allow(unused)]

use anyhow::Result;

#[path = "db_2.rs"]
mod imp;

pub type DbConfig = imp::DbConfig;

#[derive(Clone)]
pub struct Db(imp::Db);

pub struct WriteBatch(imp::WriteBatch);
pub struct ReadView(imp::ReadView);
pub struct WriteTree<'batch>(imp::WriteTree<'batch>);
pub struct ReadTree<'view>(imp::ReadTree<'view>);

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> { imp::Db::open(config).await.map(Db) }
    pub fn write_batch(&self) -> WriteBatch { WriteBatch(self.0.write_batch()) }
    pub fn read_view(&self) -> ReadView { ReadView(self.0.read_view()) }
}

impl WriteBatch {
    pub fn tree<'batch>(&'batch self, tree: &str) -> WriteTree<'batch> { WriteTree(self.0.tree(tree)) }
    pub async fn commit(self) -> Result<()> { self.0.commit().await }
    pub fn abort(self) { self.0.abort() }
}

impl ReadView {
    pub fn tree<'view>(&'view self, tree: &str) -> ReadTree<'view> { ReadTree(self.0.tree(tree)) }
}

impl<'batch> WriteTree<'batch> {
    pub fn write(&self, key: &[u8], value: &[u8]) { self.0.write(key, value) }
    pub fn delete(&self, key: &[u8]) { self.0.delete(key) }
}

impl<'view> ReadTree<'view> {
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> { self.0.read(key).await }
}
