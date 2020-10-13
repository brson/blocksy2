#![allow(unused)]

use anyhow::Result;

mod imp;

pub type DbConfig = imp::DbConfig;

#[derive(Clone)]
pub struct Db(imp::Db);

pub struct ReadView(imp::ReadView);
pub struct WriteBatch(imp::WriteBatch);
pub struct ReadTree(imp::ReadTree);
pub struct WriteTree(imp::WriteTree);

#[derive(Clone)]
pub struct IVec(imp::IVec);

impl Db {
    pub async fn open(config: DbConfig) -> Result<Db> { imp::Db::open(config).await.map(Db) }
    pub fn read_view(&self) -> ReadView { ReadView(self.0.read_view()) }
    pub fn write_batch(&self) -> WriteBatch { WriteBatch(self.0.write_batch()) }
}

impl ReadView {
    pub fn tree(&self) -> Result<ReadTree> { self.0.tree().map(ReadTree) }
}

impl WriteBatch {
    pub fn tree(&self) -> Result<WriteTree> { self.0.tree().map(WriteTree) }
    pub async fn commit(self) -> Result<()> { self.0.commit().await }
}

impl ReadTree {
    pub async fn get(&self, key: &[u8]) -> Result<Option<IVec>> { self.0.get(key).await.map(|o| o.map(IVec)) }
}

impl WriteTree {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> { self.0.insert(key, value) }
    pub fn remove(&mut self, key: &[u8]) -> Result<()> { self.0.remove(key) }
}
