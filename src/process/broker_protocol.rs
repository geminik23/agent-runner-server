//! Wire constants for the private supervised CLI input broker.

pub(super) const HEADER_BYTES: usize = 12;
pub(super) const STATUS_BYTES: usize = 9;
pub(super) const FINISH: u32 = u32::MAX;
pub(super) const MAX_FRAME: u32 = 16 * 1024 * 1024;
pub(super) const READY: u8 = 1;
pub(super) const WRITTEN: u8 = 2;
pub(super) const CLOSED: u8 = 3;
pub(super) const CHILD_EXITED: u8 = 4;
pub(super) const MAGIC: u64 = u64::from_be_bytes(*b"ARSBR001");
