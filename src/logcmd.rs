use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
pub enum LogCommand {
    Write {
        batch: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        batch: u64,
        key: Vec<u8>,
    },
    Commit {
        batch: u64,
    }
}

impl LogCommand {
    pub fn write<W>(&self, writer: &mut W) -> Result<u64> where W: Write {
        panic!()
    }

    pub fn read<R>(reader: &mut R) -> Result<LogCommand> where R: Read {
        panic!()
    }
}

#[derive(Serialize, Deserialize)]
pub enum CommitLogCommand {
    Commit {
        commit: u64,
        batch: u64,
    }
}

impl CommitLogCommand {
    pub fn write<W>(&self, writer: &mut W) -> Result<u64> where W: Write {
        panic!()
    }

    pub fn read<R>(reader: &mut R) -> Result<CommitLogCommand> where R: Read {
        panic!()
    }
}
