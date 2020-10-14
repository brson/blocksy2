use std::future::Future;
use std::path::Path;
use std::fs::File;
use anyhow::Result;
use std::thread::{self, JoinHandle};
use async_channel::{self, Sender, Receiver};
use futures::executor::block_on;

pub struct FsThread {
    handle: JoinHandle<()>,
    tx: Sender<Message>,
}

pub struct FsThreadContext;

enum Message {
    Run(Box<dyn FnOnce(&mut FsThreadContext) + Send>),
    Shutdown,
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
            let mut context = FsThreadContext;
            loop {
                let msg = block_on(rx.recv()).expect("recv");
                match msg {
                    Message::Run(f) => {
                        f(&mut context);
                    },
                    Message::Shutdown => {
                        context.shutdown();
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
        let (tx, rx) = async_channel::bounded(1);

        let simple_f = move |ctx: &mut FsThreadContext| {
            let r = f(ctx);
            tx.try_send(r).expect("send");
        };

        self.run_simple(simple_f).await;

        let r = rx.recv().await.expect("recv");

        r
    }
}

impl FsThread {
    async fn run_simple<F>(&self, f: F)
    where F: FnOnce(&mut FsThreadContext) + Send + 'static
    {
        self.tx.send(Message::Run(Box::new(f))).await.expect("send");
    }

    fn shutdown(&mut self) {
        panic!()
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
    fn shutdown(&mut self) {
        panic!()
    }
}
