use std::path::PathBuf;
use std::env;
use blocksy2::{Db, DbConfig};
use futures::executor::block_on;
use anyhow::Result;

fn main() -> Result<()> {
    block_on(async {
        let args = env::args().collect::<Vec<_>>();
        let key = args.get(1).expect("key");
        let value = args.get(2).expect("value");

        let treename = "default";

        let config = DbConfig {
            data_dir: PathBuf::from("testdb"),
            trees: vec![treename.to_string()],
        };
        let db = Db::open(config).await?;

        let batch = db.write_batch();
        let tree = batch.tree(treename);
        tree.write(key.as_bytes(), value.as_bytes());
        drop(tree);
        batch.commit().await?;

        let view = db.read_view();
        let tree = view.tree(treename);
        let value = tree.read(key.as_bytes()).await?.expect("value");
        let value = String::from_utf8(value).expect("value");
        drop(tree);
        drop(view);
        println!("value: {}", value);

        Ok(())
    })
}
