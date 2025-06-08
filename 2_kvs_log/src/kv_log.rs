use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use crate::models::{Result, Command};
use crate::serialize::{serialize};
use std::fs::{File, OpenOptions};

pub struct KvStore {
    // index: HashMap<String, String>,
    storage_dir: PathBuf,
    files: Vec<PathBuf>,
    active_file: PathBuf,
}

impl KvStore {
    pub fn new(path: &Path) -> Self {
        KvStore {
            storage_dir: path.to_path_buf(),
            files: Vec::new(),
            active_file: path.join("kv_1.log"),
        }
    }

    fn touch_file(file_path: &Path) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_path)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        let mut active_file = path.join("kv_1.log");
        let mut file_paths = Vec::new();

        // If the directory exists, read the existing storage files.
        if path.exists() {
            if !path.is_dir() {
                return Err(Box::from(format!("Path {} is not a directory", path.display())));
            }

            // Read all files in the directory and store their paths in sorted order.
            match std::fs::read_dir(path) {
                Ok(files) => {
                    for file_result in files {
                        if let Ok(file) = file_result {
                            file_paths.push(file.path());
                        }
                    }
                },
                Err(e) => {
                    return Err(Box::from(format!("Failed to read directory {}: {}", path.display(), e)));
                }
            }
            
            // If no files exist, create the first log file.
            file_paths.sort();
            if let Some(last_active_file) = file_paths.last() {
                active_file = last_active_file.clone();
            } else {
                Self::touch_file(&active_file)?;
            }

        // If the directory doesn't exist, create it.
        } else {
            match std::fs::create_dir_all(path) {
                Ok(()) => {},
                Err(e) => {
                    return Err(Box::from(format!("Failed to create directory {}: {}", path.display(), e)));
                }
            }
            Self::touch_file(&active_file)?;
        }
        
        Ok(
            KvStore {
                storage_dir: path.to_path_buf(),
                files: file_paths,
                active_file,
            }
        )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        OpenOptions::new()
            .append(true)
            .open(&self.active_file)
            .and_then(|mut file| {
                let command = Command::Set { key, value };
                let serialized_command = serialize(&command);
                let bytes_written = file.write(&serialized_command)?;
                if bytes_written != serialized_command.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other, 
                        format!("Unable to flush entire command, got {}/{} bytes written", bytes_written, serialized_command.len())
                    ));
                }
                file.sync_all()?;
                Ok(())
            })
            .map_err(|e| Box::from(format!("Failed to write to file {}: {}", self.active_file.display(), e)))
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(None)
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        Ok(())
    }
}
