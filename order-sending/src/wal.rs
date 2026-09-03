// order-sending WAL — durable, replayable record of every OrderWire this
// process has generated. Read back at startup so the order_id counter
// resumes past a restart instead of colliding with previously-sent ids, and
// consulted by the replay listener (replay.rs) to serve REPLAY_REQUEST
// ranges from order-process. Same length-prefixed bincode framing as
// order-process/src/wal.rs, for the same reason: binary data isn't safely
// newline-delimited.
//
// Durability model matches order-process's WAL: OS-buffered writes, not
// fsync'd transactions. A truncated trailing record (process killed
// mid-write) is dropped on the next startup scan rather than causing an error.

use crate::OrderWire;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

fn write_framed(buf: &mut Vec<u8>, order: &OrderWire) -> io::Result<()> {
    let encoded = bincode::serialize(order)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded);
    Ok(())
}

pub struct SenderWal {
    path: PathBuf,
    file: File,
    /// order_id -> byte offset of its length-prefix in the file, for O(1)
    /// replay lookups without scanning.
    index: HashMap<u64, u64>,
    next_offset: u64,
    last_order_id: u64,
}

impl SenderWal {
    pub fn open() -> io::Result<Self> {
        Self::open_in(Path::new("logs"))
    }

    pub fn open_in(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("orders-sent.wal");

        let raw = fs::read(&path).unwrap_or_default();
        let mut index = HashMap::new();
        let mut last_order_id = 0u64;
        let mut cursor = 0usize;
        while cursor + 4 <= raw.len() {
            let len = u32::from_le_bytes(raw[cursor..cursor + 4].try_into().unwrap()) as usize;
            if cursor + 4 + len > raw.len() {
                break; // truncated trailing record from a mid-write crash
            }
            let offset = cursor as u64;
            if let Ok(order) = bincode::deserialize::<OrderWire>(&raw[cursor + 4..cursor + 4 + len]) {
                index.insert(order.order_id, offset);
                last_order_id = last_order_id.max(order.order_id);
            }
            cursor += 4 + len;
        }

        let next_offset = cursor as u64;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        println!(
            "[wal] opened {} ({} entries, last_order_id={})",
            path.display(),
            index.len(),
            last_order_id
        );

        Ok(Self { path, file, index, next_offset, last_order_id })
    }

    /// Highest order_id durably recorded — the sender resumes its counter
    /// from `last_order_id() + 1` after a restart.
    pub fn last_order_id(&self) -> u64 {
        self.last_order_id
    }

    /// Appends every order in `orders` with a single `write_all` syscall —
    /// the background WAL-writer thread accumulates a batch (by size or a
    /// short idle timeout, same 64KB/50ms pattern used elsewhere in this
    /// codebase) before calling this, so WAL durability never costs a
    /// syscall per order in the 300k/sec hot path.
    pub fn append_batch(&mut self, orders: &[OrderWire]) -> io::Result<()> {
        if orders.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(orders.len() * 32);
        let mut offset = self.next_offset;
        for order in orders {
            let start = buf.len();
            write_framed(&mut buf, order)?;
            self.index.insert(order.order_id, offset);
            offset += (buf.len() - start) as u64;
            self.last_order_id = self.last_order_id.max(order.order_id);
        }
        self.file.write_all(&buf)?;
        self.next_offset = offset;
        Ok(())
    }

    /// Re-read one order's record from disk by seeking directly to its
    /// indexed offset — O(1), no scan. Not on the hot path (only called
    /// while serving a replay request), so a fresh read handle per call is
    /// an acceptable simplicity/perf trade-off.
    pub fn get(&self, order_id: u64) -> Option<OrderWire> {
        let offset = *self.index.get(&order_id)?;
        let mut file = File::open(&self.path).ok()?;
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf).ok()?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        file.read_exact(&mut payload).ok()?;
        bincode::deserialize(&payload).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(order_id: u64) -> OrderWire {
        OrderWire { order_id, symbol: 0, side: true, qty: 5, ts_ms: 1000 }
    }

    fn tempdir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "order-sending-wal-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_then_get_round_trips() {
        let dir = tempdir("roundtrip");
        let mut wal = SenderWal::open_in(&dir).unwrap();
        wal.append_batch(&[sample(1)]).unwrap();
        wal.append_batch(&[sample(2)]).unwrap();
        wal.append_batch(&[sample(3)]).unwrap();

        assert_eq!(wal.last_order_id(), 3);
        let got = wal.get(2).unwrap();
        assert_eq!(got.order_id, 2);
        assert_eq!(got.qty, 5);
        assert!(wal.get(999).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_resumes_last_order_id() {
        let dir = tempdir("reopen");
        {
            let mut wal = SenderWal::open_in(&dir).unwrap();
            wal.append_batch(&[sample(1)]).unwrap();
            wal.append_batch(&[sample(2)]).unwrap();
        }
        let wal = SenderWal::open_in(&dir).unwrap();
        assert_eq!(wal.last_order_id(), 2);
        assert_eq!(wal.get(1).unwrap().order_id, 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
