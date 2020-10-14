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

        let tree = "default";

        let config = DbConfig {
            data_dir: PathBuf::from("testdb"),
            trees: vec![tree.to_string()],
        };
        let db = Db::open(config).await?;
        let wb = db.write_batch();
        let tree = wb.tree(tree);
        tree.write(key.as_bytes(), value.as_bytes());
        drop(tree);
        wb.commit().await?;

        Ok(())
    })
}
