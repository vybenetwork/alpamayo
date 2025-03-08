use {
    anyhow::Context,
    std::path::{Path, PathBuf},
    tokio_uring::fs::{File, OpenOptions},
};

pub async fn open(path: &PathBuf) -> anyhow::Result<(File, u64)> {
    create_dir_all(path).await?;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await
        .with_context(|| format!("failed to open file {path:?}"))?;

    let stx = file
        .statx()
        .await
        .with_context(|| format!("failed to get file size {path:?}"))?;

    Ok((file, stx.stx_size))
}

pub async fn create_dir_all(path: &Path) -> anyhow::Result<()> {
    if let Some(path) = path.parent() {
        tokio_uring::fs::create_dir_all(path)
            .await
            .with_context(|| format!("failed to create dirs on path {path:?}"))?;
    }
    Ok(())
}
