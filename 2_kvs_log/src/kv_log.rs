use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use crate::models::{Result, Command};
use crate::serialize::{serialize, deserialize};
use std::fs::{OpenOptions, remove_file};
use std::io::BufReader;


pub struct KvStore {
    storage: HashMap<String, String>,
    storage_dir: PathBuf,
    files: Vec<PathBuf>,
    active_file: PathBuf,
}

impl KvStore {
    pub fn new(path: &Path) -> Self {
        KvStore {
            storage: HashMap::new(),
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

    fn get_default_log_file_path(path: &PathBuf) -> PathBuf {
        return path.join("kv_1.log");
    }

    fn restore_storage(files: &Vec<PathBuf>) -> Result<HashMap::<String, String>> {
        let mut index = HashMap::<String, String>::new();

        // Iterate through known storage files (expected to be sorted).
        for file_path in files {
            // Read each file using a buffered reader.
            let file = OpenOptions::new()
                .read(true)
                .open(file_path)?;
            let mut reader = BufReader::new(file);
            
            // Read commands one by one until the end. Restore the index on the fly.
            loop {
                let command = deserialize(&mut reader)?;
                match command {
                    Some(cmd) => {
                        match cmd {
                            Command::Set { key, value } => {
                                index.insert(key, value);
                            },
                            Command::Remove { key } => {
                                index.remove(&key);
                            },
                            _ => {},
                        }
                    },
                    None => break
                }
            }
        }

        Ok(index)
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        let mut active_file = Self::get_default_log_file_path(&path.to_path_buf());
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
            file_paths.sort();

            // If no files exist, create the first log file.
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

        let storage = Self::restore_storage(&file_paths)?;
        
        Ok(
            KvStore {
                storage: storage,
                storage_dir: path.to_path_buf(),
                files: file_paths,
                active_file,
            }
        )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.storage.insert(key.clone(), value.clone());

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
        match self.storage.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        for file_path in &self.files {
            remove_file(file_path)?;
        }
        self.files.clear();
        self.active_file = Self::get_default_log_file_path(&self.storage_dir);

        Ok(())
    }
}
