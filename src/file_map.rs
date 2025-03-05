use std::{
	collections::HashMap,
	io::{Error, ErrorKind},
	ops::{Deref, DerefMut},
	sync::Mutex,
};

use tokio::{fs::File, task::yield_now};

use crate::{MmapFile, Result};

const MULTIPLE_READERS: &str = "file is being read";
const MULTIPLE_WRITERS: &str = "multiple writers aren't allowed";

/// A map of memory-mapped files.
///
/// Only allows one file handle per path.
///
#[derive(Default, Debug)]
pub struct FileMap {
	files: Mutex<HashMap<String, MmapFile>>,
	writers: Mutex<HashMap<String, bool>>,
}

impl FileMap {
	///
	/// * `FileMap` - A new instance of `FileMap`.
	///
	/// # Example
	///
	/// ```
	/// let file_map = FileMap::new();
	/// ```
	pub fn new() -> Self {
		Self::default()
	}

	///
	/// * `path` - A string slice that holds the path of the file to be retrieved.
	///
	/// # Returns
	///
	/// * `Result<MmapFile>` - On success, returns the memory-mapped file. On failure, returns an error.
	///
	/// # Errors
	///
	/// This function will return an error if the file cannot be opened.
	///
	/// # Panics
	///
	/// This function will panic if the mutex is poisoned.
	///
	/// # Example
	///
	/// ```ignore
	/// let file_map = FileMap::new();
	/// let mmap_file = file_map.get("/path/to/file").await?;
	/// ```
	pub async fn get(&self, path: &str) -> Result<MmapFile> {
		let path = path.to_owned();
		{
			let m = self.writers.lock().unwrap();
			if m.contains_key(&path) {
				return Err(Error::new(ErrorKind::Other, "file is being written"));
			}
		}
		{
			let m = self.writers.lock().unwrap();
			if m.contains_key(&path) {
				return Err(Error::new(ErrorKind::Other, "file is being written"));
			}
		}
		let mut m = self.files.lock().unwrap();
		match m.get(&path) {
			Some(f) => Ok(f.clone()),
			None => {
				let f = MmapFile::open(&path).await?;
				m.insert(path, f.clone());
				Ok(f)
			}
		}
	}

	/// Attempts to acquire a writer for the specified file path.
	///
	/// This method will continuously try to acquire a writer for the file at the given path.
	/// If the file is currently being written by another writer, it will yield and retry until
	/// it succeeds or encounters an error other than `ErrorKind::Other`.
	///
	/// # Arguments
	///
	/// * `path` - A string slice that holds the path of the file to be written.
	/// * `append` - A boolean indicating whether to append to the file if it exists.
	///
	/// # Returns
	///
	/// * `Result<Writer<'_>>` - On success, returns a `Writer` for the file. On failure, returns an error.
	///
	/// # Errors
	///
	/// This function will return an error if it fails to acquire a writer for reasons other than
	/// the file being currently written by another writer.
	///
	/// # Example
	///
	/// ```ignore
	/// let file_map = FileMap::new();
	/// let writer = file_map.writer("/path/to/file", false).await?;
	/// ```
	///
	/// # Panics
	///
	/// This function will panic if the mutex is poisoned.
	pub async fn writer(&self, path: &str, append: bool) -> Result<Writer<'_>> {
		loop {
			match self.try_writer(path, append).await {
				Ok(w) => return Ok(w),
				Err(err) if err.kind() == ErrorKind::Other => {
					yield_now().await;
				}
				Err(err) => return Err(err),
			}
		}
	}

	pub async fn try_writer(&self, path: &str, append: bool) -> Result<Writer<'_>> {
		let path = path.to_owned();
		{
			let mut wm = self.writers.lock().unwrap();
			match wm.get(&path) {
				Some(_) => return Err(Error::new(ErrorKind::Other, MULTIPLE_WRITERS)),
				None => {
					let mut fm = self.files.lock().unwrap();
					match fm.get(&path) {
						Some(f) if f.reader_count() > 1 => {
							return Err(Error::new(ErrorKind::Other, MULTIPLE_READERS));
						}
						Some(_) => {
							fm.remove(&path);
						}
						None => {}
					}

					wm.insert(path.clone(), true);
				}
			}
		}

		let f = File::options()
			.write(true)
			.append(append)
			.create(true)
			.open(&path)
			.await;

		match f {
			Ok(f) => Ok(Writer { fm: self, path, f }),
			Err(err) => {
				let mut wm = self.writers.lock().unwrap();
				wm.remove(&path);
				Err(err)
			}
		}
	}

	///
	/// * `path` - A string slice that holds the path of the file to be deleted.
	///
	/// This method will remove the file associated with the given path from the map.
	/// If the file does not exist in the map, the method will do nothing.
	///
	/// # Panics
	///
	/// This function will panic if the mutex is poisoned.
	///
	/// # Example
	///
	/// ```ignore
	/// let file_map = FileMap::new();
	/// let mmap_file = file_map.get("/path/to/file").await?;
	/// file_map.remove("/path/to/file");
	/// ```
	pub fn remove(&self, path: &str) {
		let mut m = self.files.lock().unwrap();
		m.remove(path);
	}

	pub async fn remove_blocking(&self, path: &str) -> Option<MmapFile> {
		let f = {
			let mut m = self.files.lock().unwrap();
			m.remove(path)
		};

		match f {
			Some(f) => {
				while f.reader_count() > 1 {
					yield_now().await;
				}
				return Some(f);
			}
			None => None,
		}
	}
}

pub struct Writer<'a> {
	fm: &'a FileMap,
	path: String,
	f: File,
}

impl Deref for Writer<'_> {
	type Target = File;

	fn deref(&self) -> &Self::Target {
		&self.f
	}
}

impl DerefMut for Writer<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.f
	}
}

impl Drop for Writer<'_> {
	fn drop(&mut self) {
		self.fm.writers.lock().unwrap().remove(&self.path);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::fs::remove_file;

	#[tokio::test]
	async fn test_file_map() {
		let file_map = FileMap::new();
		assert!(file_map.try_writer("/tmp/y", false).await.is_ok());
		let f = file_map.get("/tmp/y").await.expect("reader failed");
		assert!(file_map.try_writer("/tmp/y", false).await.is_err());
		drop(f);
		let w = file_map.try_writer("/tmp/y", false).await.expect("writer failed");
		assert!(file_map.get("/tmp/y").await.is_err());
		drop(w);
		file_map.get("/tmp/y").await.expect("reader failed");
		remove_file("/tmp/y").await.expect("delete failed");
	}
}
