use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::fs::File;
use anyhow::Result;
use std::thread::{self, JoinHandle};
use async_channel::{self, Sender, Receiver};
use futures::executor::block_on;

pub struct FsThread {
    handle: JoinHandle<()>,
    tx: Sender<Message>,
}

pub struct FsThreadContext {
    append_handles: BTreeMap<PathBuf, File>,
    read_handles: BTreeMap<PathBuf, File>,
}

enum Message {
    Run(Box<dyn FnOnce(&mut FsThreadContext) + Send>),
    Shutdown(Sender<()>),
}

impl Drop for FsThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl FsThread {
    pub fn start() -> Result<FsThread> {
        let (tx, rx) = async_channel::bounded(16);
        let handle = thread::spawn(move || {
            let mut context = FsThreadContext::new();
            loop {
                let msg = block_on(rx.recv()).expect("recv");
                match msg {
                    Message::Run(f) => {
                        f(&mut context);
                    },
                    Message::Shutdown(rsp_tx) => {
                        context.shutdown();
                        rsp_tx.try_send(()).expect("send");
                        break;
                    }
                }
            }
        });

        Ok(FsThread {
            handle, tx
        })
    }

    pub async fn run<F, R>(&self, f: F) -> R
    where F: FnOnce(&mut FsThreadContext) -> R + Send + 'static,
          R: Send + 'static,
    {
        let (rsp_tx, rsp_rx) = async_channel::bounded(1);

        let simple_f = move |ctx: &mut FsThreadContext| {
            let r = f(ctx);
            rsp_tx.try_send(r).expect("send");
        };

        self.tx.send(Message::Run(Box::new(simple_f))).await.expect("send");

        let r = rsp_rx.recv().await.expect("recv");

        r
    }
}

impl FsThread {
    fn shutdown(&mut self) {
        let (rsp_tx, rsp_rx) = async_channel::bounded(1);
        block_on(self.tx.send(Message::Shutdown(rsp_tx))).expect("send");
        block_on(rsp_rx.recv()).expect("recv");
    }
}

impl FsThreadContext {
    pub fn open_append(&mut self, path: &Path) -> Result<&mut File> {
        panic!()
    }

    pub fn open_read(&mut self, path: &Path) -> Result<&mut File> {
        panic!()
    }

    pub fn close(&mut self, path: &Path) {
        panic!()
    }
}

impl FsThreadContext {
    fn new() -> FsThreadContext {
        FsThreadContext {
            append_handles: BTreeMap::new(),
            read_handles: BTreeMap::new(),
        }
    }

    fn shutdown(&mut self) {
        panic!()
    }
}
