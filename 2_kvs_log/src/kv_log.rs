use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::fs::{remove_file, File, OpenOptions};
use std::io::BufReader;
use log;

use crate::models::{Result, Command};
use crate::serialize::{serialize, deserialize};


const MAX_SEGMENT_SIZE: u64 = 4_000_000;

pub struct KvStore {
    storage_index: HashMap<String, String>,
    storage_dir: PathBuf,
    files: Vec<PathBuf>,
    active_file: PathBuf,
}

impl KvStore {
    pub fn new(path: &Path) -> Self {
        KvStore {
            storage_index: HashMap::new(),
            storage_dir: path.to_path_buf(),
            files: Vec::new(),
            active_file: path.join("kv_1.log"),
        }
    }

    fn touch_file(file_path: &Path) -> std::result::Result<(), io::Error> {
        log::info!("File {} doesn't exist, creating", file_path.display());
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_path)?;
        file.sync_all()?;
        Ok(())
    }

    fn get_default_log_file_path(storage_path: &PathBuf) -> PathBuf {
        return storage_path.join("kv_1.log");
    }

    fn get_next_log_file_path(&self) -> PathBuf {
        // TODO handle external storage file changes
        return self.storage_dir.join(format!("kv_{}.log", self.files.len() + 1));
    }

    fn restore_index(files: &Vec<PathBuf>) -> Result<HashMap::<String, String>> {
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

    fn rotate_file(&mut self) -> std::result::Result<(), io::Error> {
        self.active_file = self.get_next_log_file_path();
        log::info!("Rotating log file to {}", self.active_file.display());
        self.files.push(self.active_file.clone());
        Self::touch_file(&self.active_file)
    }

    fn write(&mut self, cmd: Command) -> Result<()> {
        let serialized_command = serialize(&cmd);
        let command_size = serialized_command.len() as u64;
        if command_size > MAX_SEGMENT_SIZE {
            return Err(Box::from(format!("A single log entry size cannot exceed {}", MAX_SEGMENT_SIZE)));
        }
        
        if !self.active_file.exists() {
            Self::touch_file(&self.active_file)?;
            self.files.push(self.active_file.clone());
        }

        let mut data_is_written = false;
        while !data_is_written {
            OpenOptions::new()
                .append(true)
                .open(&self.active_file)
                .and_then(|mut file| {
                    let metadata = File::metadata(&file)?;
                    let file_size = metadata.len();
                    if file_size + command_size > MAX_SEGMENT_SIZE {
                        self.rotate_file()?;
                        return Ok(())
                    }

                    let bytes_written = io::Write::write(&mut file, &serialized_command)?;
                    if bytes_written != serialized_command.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Unable to flush entire command, got {}/{} bytes written",
                                bytes_written,
                                serialized_command.len(),
                            ),
                        ));
                    }
                    file.sync_data()?;
                    data_is_written = true;
                    
                    Ok(())
                })
                .map_err(
                    |e| Box::<dyn std::error::Error>::from(
                        format!("Failed to write to file {}: {}", self.active_file.display(), e)
                    )
                )?;
        }

        Ok(())
    }

    pub fn open(path: &Path) -> Result<KvStore> {
        log::info!("Reading {} to restore storage", path.display());
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

            // Use the latest known file as active.
            if let Some(last_active_file) = file_paths.last() {
                active_file = last_active_file.clone();
            }
            log::info!("{} files found, active record at {}", file_paths.len(), active_file.display());

        // If the directory doesn't exist, create it.
        } else {
            log::info!("{} directory doesn't exist, creating", path.display());
            match std::fs::create_dir_all(path) {
                Ok(()) => {},
                Err(e) => {
                    return Err(Box::from(format!("Failed to create directory {}: {}", path.display(), e)));
                }
            }
            Self::touch_file(&active_file)?;
        }

        let storage_index = Self::restore_index(&file_paths)?;
        log::info!("Storage index is restored with {} records", storage_index.len());
        
        Ok(
            KvStore {
                storage_index: storage_index,
                storage_dir: path.to_path_buf(),
                files: file_paths,
                active_file,
            }
        )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.storage_index.insert(key.clone(), value.clone());
        self.write(Command::Set { key: key, value: value })
    }

    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        match self.storage_index.remove(&key) {
            Some(value) => {
                self.write(Command::Remove { key: key })?;
                return Ok(Some(value));
            },
            None => {
                return Ok(None);
            },
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.storage_index.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    pub fn reset(&mut self) -> Result<()> {
        for file_path in &self.files {
            log::info!("Removing log file {}", file_path.display());
            remove_file(file_path)?;
        }
        self.files.clear();
        self.active_file = Self::get_default_log_file_path(&self.storage_dir);
        self.storage_index.clear();

        Ok(())
    }
}
