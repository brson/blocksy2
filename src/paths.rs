use anyhow::Result;
use std::path::{Path, PathBuf};

pub fn tree_path_stem(data_dir: &Path, tree: &str) -> Result<PathBuf> {
    Ok(data_dir.join(format!("t-{}", tree)))
}

pub fn log_path(tree_stem_path: &Path) -> Result<PathBuf> {
    let file_name = tree_stem_path.file_name().expect("tree stem");
    Ok(tree_stem_path.with_file_name(format!("{:?}-log", file_name)))
}

pub fn commit_log_path(data_dir: &Path) -> Result<PathBuf> {
    Ok(data_dir.join("commit-log"))
}
