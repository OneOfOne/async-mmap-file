use crate::Result;
use futures::FutureExt;
use memmap2::Mmap;
use std::{
	fs::File as StdFile,
	future::AsyncDrop,
	io::{Error, ErrorKind, SeekFrom},
	ops::{Deref, DerefMut},
	path::Path,
	pin::Pin,
	sync::{Arc, LazyLock},
	task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio::{fs::File as TokioFile, io::AsyncWrite};

static PAGE_SIZE: LazyLock<usize> = LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize);

#[derive(Clone, Default)]
pub struct MmapOpenOptions {
	opts: tokio::fs::OpenOptions,
	read: Option<bool>,
	write: Option<bool>,
}

impl MmapOpenOptions {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn read(&mut self, read: bool) -> &mut Self {
		self.opts.read(read);
		self.read = Some(read);
		self
	}

	pub fn write(&mut self, write: bool) -> &mut Self {
		self.opts.write(write);
		self.write = Some(write);
		self
	}

	pub fn create(&mut self, create: bool) -> &mut Self {
		self.opts.create(create);
		self.write = Some(create);
		self
	}

	pub fn create_new(&mut self, create_new: bool) -> &mut Self {
		self.opts.create_new(create_new);
		self.write = Some(create_new);
		self
	}

	pub fn truncate(&mut self, truncate: bool) -> &mut Self {
		self.opts.truncate(truncate);
		self
	}

	pub fn mode(&mut self, mode: u32) -> &mut Self {
		self.opts.mode(mode);
		self
	}

	pub async fn open(&self, fp: impl AsRef<Path>) -> Result<MmapFile> {
		let f = self.opts.open(fp).await?;
		MmapFile::from_file(f)
	}
}

// impl Deref for MmapOpenOptions {
// 	type Target = tokio::fs::OpenOptions;
//
// 	fn deref(&self) -> &Self::Target {
// 		&self.opts
// 	}
// }
//
// impl DerefMut for MmapOpenOptions {
// 	fn deref_mut(&mut self) -> &mut Self::Target {
// 		&mut self.opts
// 	}
// }

#[derive(Clone)]
pub struct MmapFile {
	f: Arc<TokioFile>,
	m: Arc<Mmap>,
	write: bool,
	offset: usize,
}

impl MmapFile {
	pub fn options() -> MmapOpenOptions {
		MmapOpenOptions::new()
	}

	pub fn from_file(f: TokioFile) -> Result<Self> {
		// this errors if the file isn't open as read, need to figure it out
		let m = unsafe { memmap2::MmapOptions::new().populate().map(&f)? };
		Ok(Self {
			f: f.into(),
			m: m.into(),
			write: false,
			offset: 0,
		})
	}

	pub fn from_std(f: StdFile) -> Result<Self> {
		Self::from_file(TokioFile::from_std(f))
	}

	pub async fn open<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = TokioFile::open(p).await?;
		Self::from_file(f)
	}

	pub fn file_mut(&mut self) -> Option<&mut TokioFile> {
		Arc::get_mut(&mut self.f)
	}

	pub fn mmap_raw(&self) -> &Mmap {
		&self.m
	}

	pub fn reload_mmap(&mut self) -> Result<()> {
		let m = unsafe { memmap2::MmapOptions::new().populate().map_copy_read_only(&self.f)? };
		self.m = m.into();
		Ok(())
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

	/// Emulates Writes data to the file at the specified offset
	/// by seeking to offset then seeking back to the prev offset.
	///
	/// # Arguments
	///
	/// * `buf` - A mutable slice of bytes to write to the file.
	/// * `offset` - The offset at which to start writing.
	///
	/// # Returns
	///
	/// A `Result` containing the number of bytes written, or an error if the write operation fails.
	///
	/// # Errors
	///
	/// Returns an `Error` if the file is read-only or if the seek or write operations fail.
	pub async fn write_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
		if !self.write {
			return Err(Error::new(ErrorKind::PermissionDenied, "read-only file"));
		}
		let cur_offset = self.offset as u64;
		self.seek(SeekFrom::Start(offset)).await?;
		let res = self.write(buf).await;
		self.seek(SeekFrom::Start(cur_offset)).await?;
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
}

impl AsyncRead for MmapFile {
	fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
		let mut len = buf.remaining().min(self.m.len() - self.offset);
		if len > *PAGE_SIZE {
			len = *PAGE_SIZE;
		}
		buf.put_slice(&self.m[self.offset..self.offset + len]);
		self.offset += len;
		Poll::Ready(Ok(()))
	}
}

impl AsyncSeek for MmapFile {
	fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> Result<()> {
		match position {
			SeekFrom::Start(offset) => {
				if offset > self.m.len() as u64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = offset as usize;
			}
			SeekFrom::End(offset) => {
				let pos = self.m.len() as i64 + offset;
				if pos < 0 || pos > self.m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = pos as usize;
			}
			SeekFrom::Current(offset) => {
				let pos = self.offset as i64 + offset;
				if pos < 0 || pos > self.m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.offset = pos as usize;
			}
		}
		Ok(())
	}

	fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64>> {
		let offset = self.offset as u64;
		if self.write {
			if let Some(f) = self.file_mut() {
				let mut fut = Box::pin(f.seek(SeekFrom::Start(offset)));
				return fut.poll_unpin(cx);
			}
		}
		Poll::Ready(Ok(offset))
	}
}

impl AsyncWrite for MmapFile {
	fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
		if !self.write {
			return Poll::Ready(Err(Error::new(ErrorKind::PermissionDenied, "read-only file")));
		}

		let f = self.file_mut();
		if f.is_none() {
			return Poll::Ready(Err(Error::new(ErrorKind::Other, "multiple writers aren't allowed")));
		}

		let f = unsafe { f.unwrap_unchecked() };
		let mut fut = Box::pin(f.write(buf));
		fut.poll_unpin(cx)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
		if !self.write {
			return Poll::Ready(Err(Error::new(ErrorKind::PermissionDenied, "read-only file")));
		}
		let f = self.file_mut();
		if f.is_none() {
			return Poll::Ready(Err(Error::new(ErrorKind::Other, "multiple writers aren't allowed")));
		}

		let f = unsafe { f.unwrap_unchecked() };
		let mut fut = Box::pin(f.flush());

		match fut.poll_unpin(cx) {
			Poll::Ready(Ok(_)) => Poll::Ready(self.reload_mmap()),
			Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
			Poll::Pending => Poll::Pending,
		}
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
		if !self.write {
			return Poll::Ready(Err(Error::new(ErrorKind::PermissionDenied, "read-only file")));
		}
		let f = self.file_mut();
		if f.is_none() {
			return Poll::Ready(Err(Error::new(ErrorKind::Other, "multiple writers aren't allowed")));
		}
		let f = unsafe { f.unwrap_unchecked() };
		let mut fut = Box::pin(f.shutdown());
		fut.poll_unpin(cx)
	}
}

impl Deref for MmapFile {
	type Target = TokioFile;

	fn deref(&self) -> &Self::Target {
		self.f.as_ref()
	}
}

impl AsyncDrop for MmapFile {
	type Dropper<'a> = impl Future<Output = ()> + 'a;

	fn async_drop(mut self: Pin<&mut Self>) -> Self::Dropper<'_> {
		println!("dropping");
		async move {
			if Arc::strong_count(&self.f) == 1 && self.write {
				let f = self.file_mut().unwrap();
				f.shutdown().await.expect("shutdown failed");
			}
		}
	}
}

// unsafe impl Send for MmapFile {}
#[cfg(test)]
mod tests {
	use std::future::async_drop;

	use super::*;
	use tokio::{fs::remove_file, io::AsyncReadExt};

	#[tokio::test]
	async fn test_mmap() -> Result<()> {
		const SIZE: usize = 10 * 1024 * 1024;
		let path = "/tmp/x";
		let mut f = MmapFile::options()
			.write(true)
			.create(true)
			.truncate(true)
			.open(&path)
			.await
			.expect("create failed");
		let buf = vec!['@' as u8; SIZE];
		f.write_all(&buf).await.expect("write all failed");
		async_drop(f).await;
		let mut f = MmapFile::open(&path).await.expect("open failed");
		let mut buf = String::with_capacity(SIZE);
		let n1 = f.read_to_string(&mut buf).await.expect("read to string failed");
		assert_eq!(n1, SIZE);

		remove_file(&path).await.expect("remove file failed");
		Ok(())
	}
}
