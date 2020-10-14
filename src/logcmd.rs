use anyhow::{Result, bail};
use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use std::io::{Read, Write};
use std::convert::TryInto;

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
        let mut buf = serde_json::to_string_pretty(self)?;
        buf.push('\n');
        writer.write_all(buf.as_bytes())?;
        Ok(buf.len().try_into().expect("u64"))
    }

    pub fn read<R>(reader: &mut R) -> Result<LogCommand> where R: Read {
        let de = Deserializer::from_reader(reader).into_iter();
        for value in de {
            let value = value?;
            return Ok(value);
        }

        bail!("no command in stream");
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
        let buf = serde_json::to_string_pretty(self)?;
        writer.write_all(buf.as_bytes())?;
        Ok(buf.len().try_into().expect("u64"))
    }

    pub fn read<R>(reader: &mut R) -> Result<CommitLogCommand> where R: Read {
        let de = Deserializer::from_reader(reader).into_iter();
        for value in de {
            let value = value?;
            return Ok(value);
        }

        bail!("no command in stream");
    }
}
