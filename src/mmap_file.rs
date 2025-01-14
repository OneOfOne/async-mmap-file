use crate::Result;
use memmap2::Mmap;
use std::{
	fs::File as StdFile,
	io::{Error, ErrorKind, SeekFrom},
	ops::Deref,
	path::Path,
	pin::Pin,
	sync::{Arc, LazyLock},
	task::{Context, Poll},
};
use tokio::{
	fs::File as TokioFile,
	io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf},
	task::spawn_blocking,
};

static PAGE_SIZE: LazyLock<usize> = LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE).min(4096) } as usize);

/// A memory-mapped read-only file implementing AsyncRead / AsyncSeek
///
/// SAFETY:
///
/// The file must be locked before reading from it.
///
/// If the file is modified on disk, the universe may or may not implode.
#[derive(Clone, Debug)]
pub struct MmapFile {
	f: Arc<TokioFile>,
	m: Arc<Mmap>,
	offset: usize,
}

impl MmapFile {
	/// Opens a memory-mapped file asynchronously.
	///
	/// # Arguments
	///
	/// * `p` - A path to the file to be opened.
	///
	/// # Returns
	///
	/// A `Result` containing the `MmapFile` instance if successful, or an error if not.
	pub async fn open(p: impl AsRef<Path>) -> Result<Self> {
		let p = p.as_ref().to_owned();
		let (f, m) = spawn_blocking(async move || -> Result<(StdFile, Mmap)> {
			let f = StdFile::open(p)?;
			let m = unsafe { memmap2::MmapOptions::new().populate().map_copy_read_only(&f)? };
			Ok((f, m))
		})
		.await?
		.await?;

		Ok(Self {
			f: TokioFile::from_std(f).into(),
			m: m.into(),
			offset: 0,
		})
	}

	/// Reads data into the provided buffer starting at the specified offset.
	///
	/// # Arguments
	///
	/// * `buf` - The buffer to read data into.
	/// * `offset` - The offset from which to start reading.
	///
	/// # Returns
	///
	/// A `Result` containing the number of bytes read on success.
	pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
		let cur_offset = self.offset;
		self.offset = offset as usize;
		let res = self.read(buf).await;
		self.offset = cur_offset;
		res
	}

	/// Writes the contents of the memory-mapped file to the given writer.
	///
	/// # Arguments
	///
	/// * `writer` - An asynchronous writer implementing `AsyncWrite` and `Unpin`.
	///
	/// # Returns
	///
	/// A `Result` containing the total number of bytes written on success.
	///
	/// # Errors
	///
	/// This function will return an error if reading from the memory-mapped file
	/// or writing to the writer fails.
	pub async fn write_to(&mut self, mut w: impl AsyncWrite + Unpin) -> Result<usize> {
		let mut buf = vec![0; *PAGE_SIZE];
		let mut total = 0;
		loop {
			let n = self.read(&mut buf).await?;
			if n == 0 {
				break;
			}
			w.write_all(&buf[..n]).await?;
			total = total + n;
		}
		self.seek(SeekFrom::Start(0)).await?;
		Ok(total)
	}

	pub fn reader_count(&self) -> usize {
		Arc::strong_count(&self.f)
	}
}

impl AsyncRead for MmapFile {
	fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
		let m = &self.m;
		let len = buf.remaining().min(m.len() - self.offset).min(*PAGE_SIZE);
		buf.put_slice(&m[self.offset..self.offset + len]);
		self.offset += len;
		Poll::Ready(Ok(()))
	}
}

impl AsyncSeek for MmapFile {
	fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> Result<()> {
		let m = &self.m;
		match position {
			SeekFrom::Start(offset) => {
				if offset > m.len() as u64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = offset as usize;
			}
			SeekFrom::End(offset) => {
				let pos = m.len() as i64 + offset;
				if pos < 0 || pos > m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = pos as usize;
			}
			SeekFrom::Current(offset) => {
				let pos = self.offset as i64 + offset;
				if pos < 0 || pos > m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = pos as usize;
			}
		}
		Ok(())
	}

	fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<u64>> {
		let offset = self.offset as u64;
		Poll::Ready(Ok(offset))
	}
}

impl Deref for MmapFile {
	type Target = TokioFile;

	fn deref(&self) -> &Self::Target {
		&self.f
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::{
		fs::{File, remove_file},
		io::AsyncReadExt,
	};

	#[tokio::test]
	async fn test_mmap() -> Result<()> {
		const SIZE: usize = 10 * 1024 * 1024;
		let path = "/tmp/x";
		{
			let mut f = File::create(&path).await.expect("create failed");
			let buf = vec!['@' as u8; SIZE];
			f.write_all(&buf).await.expect("write all failed");
			f.flush().await.expect("flush failed");
		}
		let mut f = MmapFile::open(&path).await.expect("open failed");
		let mut buf = String::with_capacity(SIZE);
		let n1 = f.read_to_string(&mut buf).await.expect("read to string failed");
		assert_eq!(n1, SIZE);
		remove_file(&path).await.expect("remove file failed");
		Ok(())
	}
}
