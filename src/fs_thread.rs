use std::future::Future;
use std::path::Path;
use std::fs::File;
use anyhow::Result;

pub struct FsThread;

pub struct FsThreadContext;

impl Drop for FsThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FsThread {
    pub fn start() -> Result<FsThread> {
        panic!()
    }

    pub fn run<F, R>(&self, f: F) -> Box<dyn Future<Output = R> + Unpin>
    where F: FnOnce(&mut FsThreadContext) -> R + Send,
          R: Send,
    {
        panic!()
    }
}

impl FsThread {
    fn shutdown(&mut self) {
        panic!()
    }
}

impl FsThreadContext {
    pub fn open_append(&mut self, path: &Path) -> Result<&mut File> {
        panic!()
    }

    pub fn close(&mut self, path: &Path) {
        panic!()
    }
}
