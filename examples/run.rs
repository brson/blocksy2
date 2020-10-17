use std::fs;
use std::path::PathBuf;
use std::env;
use blocksy2::{Db, DbConfig};
use futures::executor::block_on;
use anyhow::Result;

fn main() -> Result<()> {
    let treename = "default";

    let config = DbConfig {
        data_dir: PathBuf::from("testdb"),
        trees: vec![treename.to_string()],
    };

    block_on(async {
        let args = env::args().collect::<Vec<_>>();
        let cmd = args.get(1).expect("cmd");
        match cmd.as_str() {
            "write" => {
                let key = args.get(2).expect("key");
                let value = args.get(3).expect("value");

                let db = Db::open(config).await?;

                let batch = db.write_batch();
                let tree = batch.tree(treename);
                tree.write(key.as_bytes(), value.as_bytes());
                drop(tree);
                batch.commit().await?;
            }
            "read" => {
                let key = args.get(2).expect("key");

                let db = Db::open(config).await?;

                let view = db.read_view();
                let tree = view.tree(treename);
                let value = tree.read(key.as_bytes()).await?;
                let value = value.unwrap_or_else(|| b"<none>".to_vec());
                let value = String::from_utf8(value).expect("value");

                println!("{}", value);
            }
            "delete-db" => {
                fs::remove_dir_all(&config.data_dir)?;
            }
            _ => {
                panic!("unknown command");
            }
        }

        Ok(())
    })
}
