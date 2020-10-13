#![allow(unused)]

use std::path::PathBuf;
use anyhow::Result;

struct DbConfig {
    path: PathBuf,
    trees: Vec<String>,
}

struct Db;

impl Db {
    fn open(config: DbConfig) -> Result<Db> { panic!() }

    fn read_view(&self) -> ReadView { panic!() }

    fn write_batch(&self) -> WriteBatch { panic!() }
}

struct ReadView;

impl ReadView {
    fn tree(&self) -> Result<&ReadTree> { panic!() }
}

struct WriteBatch;

impl WriteBatch {
    fn tree(&self) -> Result<&mut WriteTree> { panic!() }

    fn commit(self) -> Result<()> { panic!() }
}

struct ReadTree;

impl ReadTree {
    fn get(&self, key: &[u8]) -> Result<Option<IVec>> { panic!() }
}

struct WriteTree;

impl WriteTree {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> { panic!() }

    fn remove(&mut self, key: &[u8]) -> Result<()> { panic!() }
}

struct IVec;
