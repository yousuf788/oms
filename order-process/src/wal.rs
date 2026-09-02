use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReplicatedCommand {
    pub order_id: u64,
    pub symbol: String,
    pub side: String,
    pub qty: u32,
    pub status: String,
    pub filled_qty: u32,
    pub processed_by: String,
    pub term: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: ReplicatedCommand,
}

/// Encodes `entry` as `bincode` and appends it to `buf` behind a 4-byte
/// little-endian length prefix, so multiple records can be concatenated in
/// one file and read back without a text delimiter (binary data isn't safely
/// newline-delimited the way the old JSON-lines format was).
fn write_framed_entry(buf: &mut Vec<u8>, entry: &LogEntry) -> io::Result<()> {
    let encoded = bincode::serialize(entry)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded);
    Ok(())
}

/// Reads as many complete length-prefixed records as `bytes` contains. Stops
/// (without erroring) at a truncated trailing record — e.g. a length header
/// with no body yet, from a process killed mid-write — since the WAL's
/// durability model is OS-buffered writes, not fsync'd transactions.
fn read_framed_entries(bytes: &[u8]) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            break;
        }
        if let Ok(entry) = bincode::deserialize::<LogEntry>(&bytes[pos..pos + len]) {
            entries.push(entry);
        }
        pos += len;
    }
    entries
}

pub struct Wal {
    path: PathBuf,
    file: fs::File,
    entries: Vec<LogEntry>,
}

impl Wal {
    pub fn new(node_id: u8) -> io::Result<Self> {
        let base_dir = std::env::var("ORDER_PROCESS_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("logs"));
        fs::create_dir_all(&base_dir)?;
        let path = if std::env::var("ORDER_PROCESS_DATA_DIR").is_ok() {
            base_dir.join(format!("orders-processed-s2-{}.log", node_id))
        } else {
            base_dir.join("orders-processed.log")
        };
        let entries = Self::load_entries(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        if crate::config::verbose_raft() {
            println!(
                "[wal] opened {} ({} entries, last_index={})",
                path.display(),
                entries.len(),
                entries.last().map(|e| e.index).unwrap_or(0)
            );
        }
        Ok(Self { path, file, entries })
    }

    fn load_entries(path: &PathBuf) -> io::Result<Vec<LogEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(path)?;
        let mut entries = read_framed_entries(&bytes);
        entries.sort_by_key(|entry| entry.index);
        Ok(entries)
    }

    fn append_single_entry(&mut self, entry: &LogEntry) -> io::Result<()> {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, entry)?;
        self.file.write_all(&buf)?;
        Ok(())
    }

    fn rewrite_all(&mut self) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in &self.entries {
            write_framed_entry(&mut buf, entry)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(&buf)?;
        file.flush()?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|entry| entry.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|entry| entry.term).unwrap_or(0)
    }

    pub fn get_term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if let Some(e) = self.entries.get((index - 1) as usize) {
            if e.index == index {
                return Some(e.term);
            }
        }
        self.entries
            .binary_search_by_key(&index, |entry| entry.index)
            .ok()
            .map(|idx| self.entries[idx].term)
    }

    pub fn entries_from(&self, from_index: u64) -> Vec<LogEntry> {
        if from_index == 0 {
            return self.entries.clone();
        }
        let start = match self.entries.binary_search_by_key(&from_index, |e| e.index) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.entries[start..].to_vec()
    }

    pub fn entry_at(&self, index: u64) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }
        if let Some(e) = self.entries.get((index - 1) as usize) {
            if e.index == index {
                return Some(e.clone());
            }
        }
        self.entries
            .binary_search_by_key(&index, |entry| entry.index)
            .ok()
            .map(|idx| self.entries[idx].clone())
    }

    pub fn append_leader_entry(&mut self, term: u64, command: ReplicatedCommand) -> io::Result<LogEntry> {
        let entry = LogEntry {
            index: self.last_index() + 1,
            term,
            command,
        };
        self.append_single_entry(&entry)?;
        self.entries.push(entry.clone());
        Ok(entry)
    }

    pub fn append_leader_batch(
        &mut self,
        term: u64,
        commands: Vec<ReplicatedCommand>,
    ) -> io::Result<Vec<LogEntry>> {
        let mut entries = Vec::with_capacity(commands.len());
        let mut buf = Vec::with_capacity(commands.len() * 96);
        let mut last_idx = self.last_index();
        for command in commands {
            last_idx += 1;
            let entry = LogEntry {
                index: last_idx,
                term,
                command,
            };
            write_framed_entry(&mut buf, &entry)?;
            entries.push(entry);
        }
        self.file.write_all(&buf)?;
        self.entries.extend(entries.clone());
        Ok(entries)
    }

    pub fn append_entries_from_leader(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        incoming_entries: &[LogEntry],
    ) -> io::Result<Option<u64>> {
        if prev_log_index > self.last_index() {
            return Ok(None);
        }
        if self.get_term_at(prev_log_index) != Some(prev_log_term) {
            return Ok(None);
        }

        let mut truncated = false;
        for incoming in incoming_entries {
            if let Some(existing) = self.entry_at(incoming.index) {
                if existing.term != incoming.term {
                    self.entries.retain(|entry| entry.index < incoming.index);
                    self.entries.push(incoming.clone());
                    truncated = true;
                }
            } else {
                self.append_single_entry(incoming)?;
                self.entries.push(incoming.clone());
            }
        }

        if truncated {
            self.entries.sort_by_key(|entry| entry.index);
            self.rewrite_all()?;
        }
        Ok(Some(self.last_index()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(index: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            command: ReplicatedCommand {
                order_id: index,
                symbol: "BTC-USDT".to_string(),
                side: "BUY".to_string(),
                qty: 5,
                status: "FILLED".to_string(),
                filled_qty: 5,
                processed_by: "Nitin (S2-1)".to_string(),
                term: 1,
            },
        }
    }

    #[test]
    fn framed_entries_round_trip() {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, &sample_entry(7)).unwrap();
        write_framed_entry(&mut buf, &sample_entry(8)).unwrap();

        let decoded = read_framed_entries(&buf);

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].index, 7);
        assert_eq!(decoded[0].command.order_id, 7);
        assert_eq!(decoded[1].index, 8);
    }

    #[test]
    fn read_framed_entries_stops_at_truncated_trailing_record() {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, &sample_entry(1)).unwrap();
        buf.extend_from_slice(&999u32.to_le_bytes());

        let decoded = read_framed_entries(&buf);

        assert_eq!(decoded.len(), 1, "must not panic or return garbage for a truncated trailing record");
    }
}
