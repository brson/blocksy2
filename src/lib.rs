#![allow(unused)]

use anyhow::Result;

mod imp;

pub type DbConfig = imp::DbConfig;

pub struct Db(imp::Db);

impl Db {
    pub fn open(config: DbConfig) -> Result<Db> { imp::Db::open(config).map(Db) }

    pub fn read_view(&self) -> ReadView { ReadView(self.0.read_view()) }

    pub fn write_batch(&self) -> WriteBatch { WriteBatch(self.0.write_batch()) }
}

pub struct ReadView(imp::ReadView);

impl ReadView {
    pub fn tree(&self) -> Result<ReadTree> { self.0.tree().map(ReadTree) }
}

pub struct WriteBatch(imp::WriteBatch);

impl WriteBatch {
    pub fn tree(&self) -> Result<WriteTree> { self.0.tree().map(WriteTree) }

    pub fn commit(self) -> Result<()> { self.0.commit() }
}

pub struct ReadTree(imp::ReadTree);

impl ReadTree {
    pub fn get(&self, key: &[u8]) -> Result<Option<IVec>> { self.0.get(key).map(|o| o.map(IVec)) }
}

pub struct WriteTree(imp::WriteTree);

impl WriteTree {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> { self.0.insert(key, value) }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> { self.0.remove(key) }
}

pub struct IVec(imp::IVec);
