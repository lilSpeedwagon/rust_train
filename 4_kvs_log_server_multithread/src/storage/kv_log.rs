use std::collections::{HashMap, HashSet};
use std::io::{self, Seek};
use std::path::{Path, PathBuf};
use std::fs::{remove_file, rename, File, OpenOptions};
use std::io::BufReader;
use log;
use dashmap;

use crate::models::{Result, Command};
use crate::serialize::{self, get_value_offset, ReadFromStream};
use crate::threads;
use crate::threads::base::ThreadPool;

const MAX_SEGMENT_SIZE: u64 = 4_000_000;
const DEFAULT_FILE_IDX: usize = 1;
const COMPACTION_POOL_SIZE: usize = 2;

/// Convert file index to the actual file path.
fn file_idx_to_path(storage_path: &Path, file_idx: usize) -> PathBuf {
    storage_path.join(format!("kv_{}.log", file_idx))
}

/// Convert file path to file index if some.
fn path_to_idx(file_path: &Path) -> Option<usize> {
    if let Some(file_stem) = file_path.file_stem() {
        if let Some(stem_str) = file_stem.to_str() {
            let parts = stem_str.split('_');
            if let Some(idx_str) = parts.last() {
                match usize::from_str_radix(idx_str, 10) {
                    Ok(idx) => return Some(idx),
                    Err(_) => return None,
                }
            }
        }
    }
    None
}

/// Get path for a temporary copy of a given file.
fn get_tmp_file_path(storage_path: &Path, file_path: &Path) -> Result<PathBuf> {
    if file_path.is_dir() {
        return Err(Box::from(format!("Path {} is a directory", file_path.display())));
    }

    let file_name_opt = file_path.file_name();
    if let None = file_name_opt {
        return Err(Box::from(format!("Path {} is not a valid filename", file_path.display())));
    }

    let file_name = file_name_opt.unwrap().to_string_lossy();
    Ok(storage_path.join(format!("_tmp_{}", file_name)))
}

/// A single value position index in the log storage.
struct KvStorePosition {
    file_idx: usize,
    file_offset: u64,
}

/// Internal storage data structure to be exclusively locked during writes.
struct KvLogStorageInternal {
    active_file_idx: usize,
}

impl Clone for KvLogStorageInternal {
    fn clone(&self) -> KvLogStorageInternal {
        KvLogStorageInternal {
            active_file_idx: self.active_file_idx,
        }
    }

    fn clone_from(&mut self, source: &KvLogStorageInternal) {
        *self = source.clone()
    }
}

/// Key-value log-based storage.
pub struct KvLogStorage {
    internal: std::sync::Arc<std::sync::Mutex<KvLogStorageInternal>>,
    index: std::sync::Arc<dashmap::DashMap<String, KvStorePosition>>,
    storage_dir: PathBuf,
    compaction_thread_pool: std::sync::Arc<std::sync::Mutex::<threads::shared::SharedThreadPool>>,
}

impl Clone for KvLogStorage {
    fn clone(&self) -> KvLogStorage {
        KvLogStorage {
            index: self.index.clone(),
            internal: self.internal.clone(),
            storage_dir: self.storage_dir.clone(),
            compaction_thread_pool: self.compaction_thread_pool.clone(),
        }
    }

    fn clone_from(&mut self, source: &KvLogStorage) {
        *self = source.clone()
    }
}

impl KvLogStorage {
    /// Opens a directory as a log-base key-value storage.
    pub fn open(path: &Path) -> Result<KvLogStorage> {
        log::info!("Reading {} to restore storage", path.display());
        let mut file_idxs = Vec::new();

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
                            if file.path().extension() == Some(std::ffi::OsStr::new("log")) {
                                if let Some(file_idx) = path_to_idx(&file.path()) {
                                    file_idxs.push(file_idx);
                                }
                                
                            }
                        }
                    }
                },
                Err(e) => {
                    return Err(Box::from(format!("Failed to read directory {}: {}", path.display(), e)));
                }
            }

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

        // Use the latest known file as active. If no files found - use default first file.
        file_idxs.sort();
        let active_file_idx = *file_idxs.last().unwrap_or(&DEFAULT_FILE_IDX);
        let file_path = file_idx_to_path(&path.to_path_buf(), active_file_idx);
        log::info!("{} files found, active record at {}", file_idxs.len(), file_path.display());

        let storage_index = Self::restore_index(path, &file_idxs)?;

        Ok(
            KvLogStorage {
                index: std::sync::Arc::new(storage_index),
                storage_dir: path.to_path_buf(),
                internal: std::sync::Arc::new(
                    std::sync::Mutex::new(
                        KvLogStorageInternal {
                            active_file_idx: active_file_idx,
                        },
                    )
                ),
                compaction_thread_pool: std::sync::Arc::new(
                    std::sync::Mutex::new(
                        threads::shared::SharedThreadPool::new(COMPACTION_POOL_SIZE)
                    )
                ),
            }
        )
    }

    /// Restore storage index by reading a sorted list of log files (by file indexes).
    fn restore_index(storage_dir: &Path, files_idxs: &Vec<usize>) -> Result<dashmap::DashMap::<String, KvStorePosition>> {
        // We build a regular hashmap first as we know this method should be called
        // in a single thread on a startup. Later we will transform this map to a thread-safe
        // dashmap implementation.
        let mut index = HashMap::<String, KvStorePosition>::new();

        // Iterate through known storage files (expected to be sorted).
        for file_idx in files_idxs {
            // Read each file using a buffered reader.
            let file_path = &file_idx_to_path(storage_dir, *file_idx);
            let file = OpenOptions::new()
                .read(true)
                .open(file_path)?;
            let mut reader = BufReader::new(file);
            let file_idx = path_to_idx(file_path)
                .ok_or_else(|| format!("Invalid file path: {}", file_path.display()))?;

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
        }

        log::info!("Storage index is restored with {} records", index.len());
        Ok(dashmap::DashMap::from_iter(index))
    }

    fn compact_log_file(
        storage_dir: PathBuf,
        write_mutex: std::sync::Arc::<std::sync::Mutex::<KvLogStorageInternal>>,
        index: std::sync::Arc::<dashmap::DashMap<String, KvStorePosition>>,
        log_file_idx: usize,
    ) -> Result<()> {
        let log_file_path = file_idx_to_path(&storage_dir, log_file_idx);
        log::info!("Compacting log file {}", log_file_path.display());

        let file = OpenOptions::new()
                .read(true)
                .open(&log_file_path)?;
        let initial_file_size = File::metadata(&file)?.len();
        let mut reader = BufReader::new(&file);

        // Read commands one by one until the end of the file.
        // The actual values stored in this file after compaction go to a hashmap.
        // The tombstones for keys from previous files go to a set of tombstones to keep in the file.
        let mut file_key_values = HashMap::<String, String>::new();
        let mut keys_to_remove = HashSet::<String>::new();
        let mut commands_count = 0;
        loop {
            if let Some(command) = serialize::deserialize(&mut reader)? {
                match command {
                    Command::Set { key, value} => {
                        keys_to_remove.remove(&key);
                        file_key_values.insert(key, value);
                        commands_count += 1;
                    },
                    Command::Remove { key } => {
                        file_key_values.remove(&key);
                        keys_to_remove.insert(key);
                        commands_count += 1;
                    }
                    _ => {},
                }
            } else {
                break
            }
        }
        drop(reader);
        drop(file);

        // If the amount of commands matches the expected number of compacted set/remove commands,
        // we can skip compaction.
        if commands_count == file_key_values.len() + keys_to_remove.len() {
            log::info!("No records to compact found in {}", log_file_path.display());
            return Ok(())
        }

        // If all records are compacted - just remove the file.
        if file_key_values.is_empty() && keys_to_remove.is_empty() {
            log::info!("All records in {} are compacted. Deleting the log file.", log_file_path.display());
            remove_file(log_file_path)?;
            return Ok(())
        }

        // Write the compacted commands to a temporary file.
        // Build a new index with new value positions,
        // as the compacted records are probably shifted within the file.
        
        // Create a temporary file to write the compacted commands and then swap it with the actual file.
        let tmp_file_path = get_tmp_file_path(&storage_dir, &log_file_path)?;
        log::info!("Writing compacted records from {} to {}", log_file_path.display(), tmp_file_path.display());
        if tmp_file_path.exists() {
            log::warn!(
                "Temporary file {} already exists. It might be a result of a previous failed compaction.",
                tmp_file_path.display(),
            );
            remove_file(&tmp_file_path)?;
        }
        let mut tmp_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&tmp_file_path)?;

        // Rebuild the index subset for the compacted file to update the value positions.
        // Later we can merge the updated index with the actual storage index.
        let mut file_index = HashMap::<String, KvStorePosition>::new();
        
        // Insert SET commands and update the index positions.
        let mut file_offset = 0u64;
        for (key, value) in file_key_values {
            let cmd = Command::Set{ key: key.clone(), value: value };
            let serialized_command = serialize::serialize(&cmd)?;
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

            let value_offset = get_value_offset(&cmd).unwrap_or(0);
            file_index.insert(
                key, KvStorePosition { file_idx: log_file_idx, file_offset: file_offset + value_offset }
            );
            file_offset += bytes_written as u64;
        }

        // Insert tombstones for keys from previous files.
        for key in keys_to_remove {
            let cmd = Command::Remove { key: key };
            let serialized_command = serialize::serialize(&cmd)?;
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
        }

        // Flush the written content and sync the metadata.
        tmp_file.sync_all()?;
        let compacted_file_size = File::metadata(&tmp_file)?.len();
        drop(tmp_file);

        // Acquire the storage write mutex to make actual changes in the storage files and index.
        let _mutex_guard = write_mutex.lock().unwrap_or_else(|e| e.into_inner());
        
        // Replace the original file with the compacted temp file.
        log::info!("Replacing {} with compacted {}", log_file_path.display(), tmp_file_path.display());
        rename(tmp_file_path, &log_file_path)?;

        // Update the storage index. If a key has a newer value, or doesn't exists, skip the key position update.
        for (key, new_position) in file_index {
            if let Some(existing_pos) = index.get(&key) {
                if existing_pos.file_idx == log_file_idx {
                    index.insert(key, new_position);
                }
            }
        }

        log::info!(
            "Log file {} compaction completed: {} -> {} bytes",
            log_file_path.display(), initial_file_size, compacted_file_size
        );
        Ok(())
    }

    /// Runs the compaction process in a new thread.
    /// The compaction threads are taken from a separate thread pool guarded with a mutex.
    /// As compaction process is relatively rare, it is not expected to cause mutex contention.
    fn run_compaction(&self, log_file_idx: usize) {
        let storage_dir = self.storage_dir.clone();
        let internal = self.internal.clone();
        let index = self.index.clone();
        let mut pool = self.compaction_thread_pool.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(err) = pool.spawn(Box::new(move || {
            Self::compact_log_file(storage_dir, internal, index, log_file_idx).ok();
        })) {
            log::error!("Cannot queue the compaction job for the log file with idx={}: {}", log_file_idx, err);
        }
    }

    /// Set active file path to the next value and compact the currect active file.
    fn rotate_file(&self, internal: &mut KvLogStorageInternal) -> Result<()> {
        let prev_idx = internal.active_file_idx;
        internal.active_file_idx += 1;
        let prev_file_path = file_idx_to_path(&self.storage_dir, prev_idx);
        let next_file_path = file_idx_to_path(&self.storage_dir, internal.active_file_idx);
        
        log::info!("Rotating log file {} to {}", prev_file_path.display(), next_file_path.display());
        
        self.run_compaction(prev_idx);

        Ok(())
    }

    /// Writes a command to the log storage.
    /// If the command contains a value, it's position is returned.
    fn write(&self, internal: &mut KvLogStorageInternal, cmd: Command) -> Result<Option<KvStorePosition>> {
        let serialized_command = serialize::serialize(&cmd)?;
        let command_size = serialized_command.len() as u64;
        if command_size > MAX_SEGMENT_SIZE {
            return Err(Box::from(format!("A single log entry size cannot exceed {}", MAX_SEGMENT_SIZE)));
        }

        let mut file_offset = 0u64;
        let mut data_is_written = false;
        while !data_is_written {
            let active_file_path = file_idx_to_path(&self.storage_dir, internal.active_file_idx);
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&active_file_path)?;

            // If the current active file exceeds max allowed size - try writing to the next file.
            let file_size = File::metadata(&file)?.len();
            if file_size + command_size > MAX_SEGMENT_SIZE {
                self.rotate_file(internal)?;
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
                Ok(
                    Some(
                        KvStorePosition {
                            file_idx: internal.active_file_idx,
                            file_offset: file_offset + value_offset
                        }
                    )
                )
            },
            None => Ok(None),
        }
    }

    /// Reads a value from the log files using the position.
    fn read_value(storage_path: &Path, position: &KvStorePosition) -> Result<String> {
        let file_path = file_idx_to_path(&storage_path, position.file_idx);
        let file = OpenOptions::new().read(true).open(file_path)?;

        let mut reader = BufReader::new(file);
        reader.seek(io::SeekFrom::Start(position.file_offset))?;
        
        match String::deserialize(&mut reader) {
            Ok(result) => Ok(result),
            Err(err) => Err(Box::new(err)),
        }
    }

    /// Set key `key` to value `value`.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut internal = match self.internal.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let cmd = Command::Set { key: key.clone(), value: value };
        let pos = self.write(&mut internal, cmd)?.unwrap();
        self.index.insert(key, pos);
        Ok(())
    }

    /// Removes key `key` from the storage.
    /// Returns `true` if the key existed.
    pub fn remove(&mut self, key: String) -> Result<bool> {
        let mut internal = match self.internal.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match self.index.remove(&key) {
            Some(_) => {
                self.write(&mut internal, Command::Remove { key: key })?;
                Ok(true)
            },
            None => Ok(false),
        }
    }

    /// Gets value with the key `key`. Returns `None` if the key doesn't exist in the storage.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(position) => {
                let value = Self::read_value(&self.storage_dir, &position)?;
                Ok(Some(value))
            },
            None => Ok(None),
        }
    }

    /// Removes all records in the storage.
    pub fn reset(&mut self) -> Result<()> {
        let mut internal = self.internal.lock().unwrap_or_else(|e| e.into_inner());
        for file_idx in 1..internal.active_file_idx + 1 {
            let file_path = file_idx_to_path(&self.storage_dir, file_idx);
            log::info!("Removing log file {}", file_path.display());

            if let Err(err) = remove_file(&file_path) {
                if err.kind() == std::io::ErrorKind::NotFound {
                    log::warn!("Cannot delete file {}. File doesn't exist", file_path.display());
                } else {
                    return Err(Box::new(err));
                }
            }
        }
        internal.active_file_idx = DEFAULT_FILE_IDX;
        self.index.clear();
        Ok(())
    }
}
