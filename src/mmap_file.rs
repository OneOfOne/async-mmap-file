use crate::{Result, cvt};
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
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, ReadBuf};

static PAGE_SIZE: LazyLock<usize> =
	LazyLock::new(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize);

#[derive(Clone)]
pub struct MmapFile {
	f: Arc<TokioFile>,
	m: Arc<Mmap>,
	pos: usize,
}

impl MmapFile {
	pub fn from_file(f: TokioFile) -> Result<Self> {
		let m = unsafe {
			memmap2::MmapOptions::new()
				.populate()
				.map_copy_read_only(&f)?
		};
		Ok(Self {
			f: f.into(),
			m: m.into(),
			pos: 0,
		})
	}

	pub fn from_std(f: StdFile) -> Result<Self> {
		Self::from_file(TokioFile::from_std(f))
	}

	pub async fn open<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = TokioFile::open(p).await?;
		Self::from_file(f)
	}

	pub fn open_sync<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = StdFile::open(p)?;
		Self::from_std(f)
	}

	pub fn file_raw(&mut self) -> Option<&mut TokioFile> {
		Arc::get_mut(&mut self.f)
	}

	pub fn mmap_raw(&self) -> &Mmap {
		&self.m
	}

	pub async fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
		let oldpos = self.pos;
		self.pos = offset as usize;
		let res = self.read(buf).await;
		self.pos = oldpos;
		res
	}
}

impl AsyncRead for MmapFile {
	fn poll_read(
		mut self: Pin<&mut Self>,
		_cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<Result<()>> {
		let mut len = buf.remaining().min(self.m.len() - self.pos);
		if len > *PAGE_SIZE {
			len = *PAGE_SIZE;
		}
		buf.put_slice(&self.m[self.pos..self.pos + len]);
		self.pos += len;
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
				self.pos = offset as usize;
			}
			SeekFrom::End(offset) => {
				let pos = self.m.len() as i64 + offset;
				if pos < 0 || pos > self.m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.pos = pos as usize;
			}
			SeekFrom::Current(offset) => {
				let pos = self.pos as i64 + offset;
				if pos < 0 || pos > self.m.len() as i64 {
					return Err(Error::new(ErrorKind::InvalidInput, "invalid position"));
				}
				self.pos = pos as usize;
			}
		}
		Ok(())
	}

	fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<u64>> {
		Poll::Ready(Ok(self.pos as u64))
	}
}

impl Deref for MmapFile {
	type Target = TokioFile;

	fn deref(&self) -> &Self::Target {
		&self.f
	}
}

unsafe impl Send for MmapFile {}
#[cfg(test)]
mod tests {
	use super::*;
	use tokio::io::AsyncReadExt;

	#[tokio::test]
	async fn test_mmap() -> Result<()> {
		let mut f = MmapFile::open("/tmp/x").await?;
		let mut buf = vec![];
		let n = f.read_to_end(&mut buf).await?;
		let f2 = f.clone();
		drop(f2);
		println!("len {:?}", n);
		drop(f);
		Ok(())
	}
}
