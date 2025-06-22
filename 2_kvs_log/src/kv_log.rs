use std::collections::HashMap;
use std::io::{self, Seek};
use std::path::{Path, PathBuf};
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::BufReader;
use std::ffi::OsStr;
use log;

use crate::models::{Result, Command};
use crate::serialize::{self, get_value_offset};


const MAX_SEGMENT_SIZE: u64 = 4_000_000;

/// A single value position index in the log storage.
pub struct KvStorePosition {
    file_idx: usize,
    file_offset: u64,
}

/// Key-value log-based storage.
pub struct KvStore {
    storage_index: HashMap<String, KvStorePosition>,
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

    /// Get default log file path.
    fn get_default_log_file_path(storage_path: &PathBuf) -> PathBuf {
        return storage_path.join("kv_1.log");
    }

    /// Get next active log file path based on the known file paths.
    fn get_next_log_file_path(&self) -> PathBuf {
        return self.storage_dir.join(format!("kv_{}.log", self.files.len() + 1));
    }

    fn get_tmp_file_path(&self, file_path: &PathBuf) -> PathBuf {
        let dir_path = file_path.parent().unwrap_or(&self.storage_dir);
        let file_name = file_path.file_name().unwrap_or(OsStr::new("kv.log")).to_string_lossy();
        return dir_path.join(format!("_tmp_{}", file_name));
    }

    /// Restore storage index by reading a sorted list of log files.
    fn restore_index(files: &Vec<PathBuf>) -> Result<HashMap::<String, KvStorePosition>> {
        let mut index = HashMap::<String, KvStorePosition>::new();

        // Iterate through known storage files (expected to be sorted).
        let mut file_idx = 0;
        for file_path in files {
            // Read each file using a buffered reader.
            let file = OpenOptions::new()
                .read(true)
                .open(file_path)?;
            let mut reader = BufReader::new(file);

            // Read commands one by one until the end. Restore the index on fly.
            loop {
                let mut file_offset = reader.stream_position()?;
                let command = serialize::deserialize(&mut reader)?;
                match command {
                    Some(cmd) => {
                        let value_offset_opt = serialize::get_value_offset(&cmd);
                        match cmd {
                            Command::Set { key, value: _} => {
                                file_offset += value_offset_opt.unwrap_or(0);
                                index.insert(key, KvStorePosition{ file_idx: file_idx, file_offset: file_offset });
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

            file_idx += 1;
        }

        Ok(index)
    }

    fn compact_log_file(&mut self) -> Result<()> {
        if self.files.len() == 0 {
            log::info!("No files to compact!");
            return Ok(());
        }

        let file_path = &self.active_file;
        let file_idx = self.files.len() - 1;
        log::info!("Compacting log file {}", file_path.display());
        let mut log_file_commands: Vec<Command> = Vec::new();

        let file = OpenOptions::new()
                .read(true)
                .open(file_path)?;
        let initial_file_size = File::metadata(&file)?.len();
        let mut reader = BufReader::new(&file);
        let mut is_compacted = false;

        // Read commands one by one until the end.
        // If the key doesn't exist in the current index - it can be deleted.
        // If the known key's position is higher than in the log - it can be deleted.
        loop {
            let file_offset = reader.stream_position()?;
            if let Some(command) = serialize::deserialize(&mut reader)? {
                let value_offset_opt = serialize::get_value_offset(&command);
                
                // If "set" is found in the current index and the position
                // matches the index position - this is the latest key value.
                // Otherwise the found command can be compacted (ingored).
                match command {
                    Command::Set { key, value} => {
                        match self.storage_index.get(&key) {
                            Some(position) => {
                                let value_offset = file_offset + value_offset_opt.unwrap_or(0);
                                if value_offset == position.file_offset {
                                    log_file_commands.push(Command::Set { key, value });
                                    continue;
                                }
                            },
                            None => {},
                        }
                    },
                    _ => {},
                }
                is_compacted = true;
            } else {
                break
            }
        }
        drop(reader);
        drop(file);

        // If no commands ingored - no compaction required.
        if !is_compacted {
            log::info!("No records to compact found in {}", file_path.display());
            return Ok(())
        }

        // If all records are compacted - just remove the file.
        if log_file_commands.is_empty() {
            log::info!("All records in {} are compacted. Deleting the log file.", file_path.display());
            remove_file(file_path)?;
            return Ok(())
        }
        
        // Write the compacted commands to a temporary file.
        // Update the position offsets in the current file,
        // as the compacted records are probably shifted within the file.
        let tmp_file_path = self.get_tmp_file_path(&file_path);
        log::info!("Writing compacted records from {} to {}", file_path.display(), tmp_file_path.display());
        let mut tmp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&tmp_file_path)?;
        let mut file_offset = 0u64;
        for cmd in log_file_commands {
            let serialized_command = serialize::serialize(&cmd);
            let bytes_written = io::Write::write(&mut tmp_file, &serialized_command)?;
            if bytes_written != serialized_command.len() {
                return Err(
                    Box::from(
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Unable to flush entire command, got {}/{} bytes written",
                                bytes_written,
                                serialized_command.len(),
                            ),
                        )
                    )
                );
            }
            file_offset += bytes_written as u64;
            let value_offset = get_value_offset(&cmd).unwrap_or(0);
            
            // Insert new positions. We expect to see only "set" commands here.
            // File index remains the same as we're just compactng a single file.
            match cmd {
                Command::Set { key, value: _ } => {
                    self.storage_index.insert(
                        key, KvStorePosition { file_idx: file_idx, file_offset: file_offset + value_offset }
                    );
                },
                _ => {},
            }
        }
        tmp_file.sync_all()?;
        let compacted_file_size = File::metadata(&tmp_file)?.len();
        drop(tmp_file);

        // Replace the original file with a new file.
        // TODO apply file transactions here.
        log::info!("Replacing {} with compacted {}", file_path.display(), tmp_file_path.display());
        remove_file(file_path)?;
        rename(tmp_file_path, file_path)?;

        log::info!("Log file {} compaction completed: {} -> {} bytes", file_path.display(), initial_file_size, compacted_file_size);

        Ok(())
    }

    /// Sets active file path to the next value.
    fn rotate_file(&mut self) -> Result<()> {
        self.compact_log_file()?;
        self.active_file = self.get_next_log_file_path();
        log::info!("Rotating log file to {}", self.active_file.display());
        self.files.push(self.active_file.clone());
        Ok(())
    }

    /// Writes a command to the log storage.
    /// If the command contains a value, it's position is returned.
    fn write(&mut self, cmd: Command) -> Result<Option<KvStorePosition>> {
        let serialized_command = serialize::serialize(&cmd);
        let command_size = serialized_command.len() as u64;
        if command_size > MAX_SEGMENT_SIZE {
            return Err(Box::from(format!("A single log entry size cannot exceed {}", MAX_SEGMENT_SIZE)));
        }

        let mut file_idx = self.files.len() - 1;
        let mut file_offset = 0u64;
        let mut data_is_written = false;
        while !data_is_written {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.active_file)?;
                
            // If the current active file exceeds max allowed size - try writing to the next file.
            let file_size = File::metadata(&file)?.len();
            if file_size + command_size > MAX_SEGMENT_SIZE {
                self.rotate_file()?;
                file_idx += 1;
                continue;
            }

            file_offset = file.seek(io::SeekFrom::End(0))?;
            let bytes_written = io::Write::write(&mut file, &serialized_command)?;
            if bytes_written != serialized_command.len() {
                return Err(
                    Box::from(
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "Unable to flush entire command, got {}/{} bytes written",
                                bytes_written,
                                serialized_command.len(),
                            ),
                        )
                    )
                );
            }
            file.sync_data()?;
            data_is_written = true;
        }

        match serialize::get_value_offset(&cmd) {
            Some(value_offset) => {
                Ok(Some(KvStorePosition { file_idx: file_idx, file_offset: file_offset + value_offset }))
            },
            None => Ok(None),
        }
    }

    /// Reads a value from the log files using the position.
    fn read_value(&self, position: &KvStorePosition) -> Result<String> {
        if position.file_idx >= self.files.len() {
            return Err(Box::from(format!("Bad position: missing file with idx={}", position.file_idx)))
        }

        let file_path = &self.files[position.file_idx];
        let file = OpenOptions::new().read(true).open(file_path)?;

        let mut reader = BufReader::new(file);
        reader.seek(io::SeekFrom::Start(position.file_offset))?;
        serialize::deserialize_str(&mut reader)
    }

    /// Opens a directory as a log-base key-value storage.
    pub fn open(path: &Path) -> Result<KvStore> {
        log::info!("Reading {} to restore storage", path.display());
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
                            if file.path().extension() == Some(OsStr::new("log")) {
                                file_paths.push(file.path());
                            }
                        }
                    }
                },
                Err(e) => {
                    return Err(Box::from(format!("Failed to read directory {}: {}", path.display(), e)));
                }
            }
            file_paths.sort();

        // If the directory doesn't exist, create it.
        } else {
            log::info!("{} directory doesn't exist, creating", path.display());
            match std::fs::create_dir_all(path) {
                Ok(()) => {},
                Err(e) => {
                    return Err(Box::from(format!("Failed to create directory {}: {}", path.display(), e)));
                }
            }
        }

        let storage_index = Self::restore_index(&file_paths)?;
        log::info!("Storage index is restored with {} records", storage_index.len());

        // Use the latest known file as active. If no files found - use default first file.
        let mut active_file = Self::get_default_log_file_path(&path.to_path_buf());
        if let Some(last_active_file) = file_paths.last() {
            active_file = last_active_file.clone();
            log::info!("{} files found, active record at {}", file_paths.len(), active_file.display());
        } else {
            file_paths.push(active_file.clone());
            log::info!("no files found, active record at {}", active_file.display());
        }

        Ok(
            KvStore {
                storage_index: storage_index,
                storage_dir: path.to_path_buf(),
                files: file_paths,
                active_file,
            }
        )
    }

    /// Set key `key` to value `value`.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let pos = self.write(Command::Set { key: key.clone(), value: value })?.unwrap();
        self.storage_index.insert(key, pos);
        Ok(())
    }

    /// Removes key `key` from the storage.
    /// Returns `true` if the key existed.
    pub fn remove(&mut self, key: String) -> Result<bool> {
        match self.storage_index.remove(&key) {
            Some(_) => {
                self.write(Command::Remove { key: key })?;
                return Ok(true);
            },
            None => {
                return Ok(false);
            },
        }
    }

    /// Gets value with the key `key`. Returns `None` if the key doesn't exist in the storage.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.storage_index.get(&key) {
            Some(position) => {
                let value = self.read_value(&position)?;
                Ok(Some(value))
            },
            None => Ok(None),
        }
    }

    /// Removes all records in the storage.
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
