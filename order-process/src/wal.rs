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

pub struct Wal {
    path: PathBuf,
    entries: Vec<LogEntry>,
}

impl Wal {
    pub fn new(node_id: u8) -> io::Result<Self> {
        let base_dir = std::env::var("ORDER_PROCESS_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("order-process/data"));
        fs::create_dir_all(&base_dir)?;
        let path = base_dir.join(format!("wal-s2-{}.log", node_id));
        let entries = Self::load_entries(&path)?;
        if crate::config::verbose_raft() {
            println!(
                "[wal] opened {} ({} entries, last_index={})",
                path.display(),
                entries.len(),
                entries.last().map(|e| e.index).unwrap_or(0)
            );
        }
        Ok(Self { path, entries })
    }

    fn load_entries(path: &PathBuf) -> io::Result<Vec<LogEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let content = fs::read_to_string(path)?;
        for line in content.lines() {
            if let Ok(entry) = serde_json::from_str::<LogEntry>(line) {
                entries.push(entry);
            }
        }
        entries.sort_by_key(|entry| entry.index);
        Ok(entries)
    }

    fn persist(&self) -> io::Result<()> {
        let mut out = String::new();
        for entry in &self.entries {
            let line = serde_json::to_string(entry)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
            out.push_str(&line);
            out.push('\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(out.as_bytes())?;
        file.sync_all()?;
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
        self.entries
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    pub fn entries_from(&self, from_index: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.index >= from_index)
            .cloned()
            .collect()
    }

    pub fn entry_at(&self, index: u64) -> Option<LogEntry> {
        self.entries.iter().find(|entry| entry.index == index).cloned()
    }

    pub fn append_leader_entry(&mut self, term: u64, command: ReplicatedCommand) -> io::Result<LogEntry> {
        let entry = LogEntry {
            index: self.last_index() + 1,
            term,
            command,
        };
        self.entries.push(entry.clone());
        self.persist()?;
        Ok(entry)
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

        for incoming in incoming_entries {
            if let Some(existing) = self.entry_at(incoming.index) {
                if existing.term != incoming.term {
                    self.entries.retain(|entry| entry.index < incoming.index);
                    self.entries.push(incoming.clone());
                }
            } else {
                self.entries.push(incoming.clone());
            }
        }
        self.entries.sort_by_key(|entry| entry.index);
        self.persist()?;
        Ok(Some(self.last_index()))
    }
}
