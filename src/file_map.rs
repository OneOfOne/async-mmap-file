use std::{collections::HashMap, sync::Mutex};

use crate::{MmapFile, Result};

pub struct FileMap {
	files: Mutex<HashMap<String, MmapFile>>,
}

impl FileMap {
	/// Creates a new `FileMap`.
	pub fn new() -> Self {
		Self {
			files: Default::default(),
		}
	}

	/// Retrieves a file from the map, or opens, and inserts it if not present.
	///
	/// # Arguments
	///
	/// * `path` - A string slice that holds the path of the file.
	///
	/// # Returns
	///
	/// * `Result<MmapFile>` - The memory-mapped file or an error.
	pub async fn get(&self, path: &str) -> Result<MmapFile> {
		let mut m = self.files.lock().unwrap();
		let path = path.to_owned();
		match m.get(&path) {
			Some(f) => Ok(f.clone()),
			None => {
				let f = MmapFile::open(&path).await?;
				m.insert(path, f.clone());
				Ok(f)
			}
		}
	}

	/// Deletes a file from the map.
	///
	/// # Arguments
	///
	/// * `path` - A string slice that holds the path of the file.
	pub fn delete(&self, path: &str) {
		let mut m = self.files.lock().unwrap();
		m.remove(path);
	}
}
