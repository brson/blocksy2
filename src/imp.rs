use std::path::PathBuf;
use anyhow::Result;

pub struct DbConfig {
    path: PathBuf,
    trees: Vec<String>,
}

pub struct Db;

impl Db {
    pub fn open(config: DbConfig) -> Result<Db> { panic!() }

    pub fn read_view(&self) -> ReadView { panic!() }

    pub fn write_batch(&self) -> WriteBatch { panic!() }
}

pub struct ReadView;

impl ReadView {
    pub fn tree(&self) -> Result<ReadTree> { panic!() }
}

pub struct WriteBatch;

impl WriteBatch {
    pub fn tree(&self) -> Result<WriteTree> { panic!() }

    pub fn commit(self) -> Result<()> { panic!() }
}

pub struct ReadTree;

impl ReadTree {
    pub fn get(&self, key: &[u8]) -> Result<Option<IVec>> { panic!() }
}

pub struct WriteTree;

impl WriteTree {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> { panic!() }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> { panic!() }
}

pub struct IVec;
