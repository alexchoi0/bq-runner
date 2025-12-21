use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct SqlFile {
    pub name: String,
    pub sql: String,
    pub path: PathBuf,
}

pub struct SqlLoader;

impl SqlLoader {
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        let pattern = path.as_ref().join("**/*.sql");
        let pattern_str = pattern.to_string_lossy();

        let sql_files: Vec<PathBuf> = glob(&pattern_str)
            .map_err(|e| Error::Loader(format!("Invalid glob pattern: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        sql_files
            .into_iter()
            .map(|sql_path| Self::load_file(&sql_path))
            .collect()
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<SqlFile> {
        let path = path.as_ref();

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Loader(format!("Invalid filename: {}", path.display())))?
            .to_string();

        let sql = fs::read_to_string(path)
            .map_err(|e| Error::Loader(format!("Failed to read {}: {}", path.display(), e)))?;

        Ok(SqlFile {
            name,
            sql,
            path: path.to_path_buf(),
        })
    }
}
