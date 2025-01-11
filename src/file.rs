use crate::{Result, locked_file::cvt};
use std::fs::File as StdFile;
use std::{
	fs::Metadata,
	io::{ErrorKind, Seek, SeekFrom},
	os::fd::{AsRawFd, RawFd},
	path::Path,
	sync::Arc,
	task::Poll,
};
use tokio::io::AsyncRead;
use tokio::{
	fs::File as TFile,
	io::{AsyncSeek, AsyncWrite},
};
#[derive(Clone, Debug)]
pub struct File {
	f: Option<Arc<StdFile>>,
	offset: usize,
}

impl File {
	pub fn from_std(mut f: StdFile) -> Result<Self> {
		let pos = f.seek(SeekFrom::Current(0))?;
		Ok(Self {
			f: Some(Arc::new(f)),
			offset: pos as usize,
		})
	}

	pub async fn from_file(f: TFile) -> Result<Self> {
		Self::from_std(f.into_std().await)
	}

	pub async fn create<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = StdFile::create(p)?;
		Ok({
			Self {
				f: Some(Arc::new(f)),
				offset: 0,
			}
		})
	}

	pub async fn open<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = StdFile::open(p)?;
		Ok({
			Self {
				f: Some(Arc::new(f)),
				offset: 0,
			}
		})
	}

	pub fn metadata(&self) -> Result<Metadata> {
		let f = self.f.as_ref().unwrap();
		f.metadata()
	}

	pub(crate) fn fd(&self) -> i32 {
		let f = self.f.as_ref().unwrap();
		f.as_raw_fd()
	}

	pub fn file_mut(&mut self) -> &mut Arc<StdFile> {
		self.f.as_mut().unwrap()
	}
}

impl Drop for File {
	fn drop(&mut self) {
		let f = self.f.take().unwrap();
		if Arc::strong_count(&f) == 1 {
			let f = Arc::into_inner(f).unwrap();
			drop(f);
		}
	}
}

impl AsRawFd for File {
	fn as_raw_fd(&self) -> RawFd {
		self.fd()
	}
}

impl AsyncRead for File {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let b = buf.initialize_unfilled();
		let ret = unsafe {
			cvt(libc::pread64(
				self.fd(),
				b.as_mut_ptr() as *mut libc::c_void,
				b.len().min(8192),
				self.offset as i64,
			))
		};

		match ret {
			Ok(n) => {
				self.offset += n;
				unsafe { buf.assume_init(n) };
				buf.set_filled(self.offset);
				Poll::Ready(Ok(()))
			}
			Err(err) if err.kind() != ErrorKind::WouldBlock => {
				return Poll::Ready(Err(err));
			}
			_ => Poll::Pending,
		}
	}
}

impl AsyncSeek for File {
	fn start_seek(mut self: std::pin::Pin<&mut Self>, position: SeekFrom) -> Result<()> {
		match position {
			SeekFrom::Start(offset) => {
				self.offset = offset as usize;
			}
			SeekFrom::End(offset) => {
				let metadata = self.metadata()?;
				let pos = metadata.len() as usize + offset as usize;
				self.offset = pos;
			}
			SeekFrom::Current(offset) => {
				let pos = self.offset as i64 + offset;
				if pos < 0 {
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						"invalid seek to a negative or zero position",
					));
				}
				self.offset = pos as usize;
			}
		}
		Ok(())
	}

	fn poll_complete(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::io::Result<u64>> {
		let pos = self.offset as u64;
		self.file_mut().seek(SeekFrom::Start(pos))?;
		Poll::Ready(Ok(0))
	}
}

impl AsyncWrite for File {
	fn poll_write(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> Poll<std::result::Result<usize, std::io::Error>> {
		let ret = unsafe {
			cvt(libc::pwrite(
				self.fd(),
				buf.as_ptr() as *const libc::c_void,
				buf.len(),
				self.offset as i64,
			))
		};
		match ret {
			Ok(n) => {
				self.offset += n;
				Poll::Ready(Ok(n))
			}
			Err(err) => {
				if let Some(err) = err.raw_os_error() {
					if err.eq(&libc::EAGAIN) || err.eq(&libc::EINTR) {
						return Poll::Pending;
					}
				}

				return Poll::Ready(Err(err));
			}
		}
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::result::Result<(), std::io::Error>> {
		unsafe { cvt(libc::fsync(self.fd()) as isize) }?;
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> Poll<std::result::Result<(), std::io::Error>> {
		Poll::Ready(Ok(()))
	}
}

unsafe impl Send for File {}
unsafe impl Sync for File {}
#[cfg(test)]
mod aio_tests {
	use super::*;
	use test::Bencher;
	use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

	#[tokio::test]
	async fn write_read() -> Result<()> {
		// write().await?;
		read().await?;
		Ok(())
	}

	async fn write() -> Result<()> {
		let mut f = File::create(&"/tmp/x").await?;
		let buf = "abcd\n".repeat(1_000_000);
		let _ = f.write(buf.as_bytes()).await?;

		Ok(())
	}

	async fn read() -> Result<()> {
		let f = File::open("/tmp/x").await?;
		{
			let mut f = f.clone();
			let mut buf = vec![];
			let n = f.read_to_end(&mut buf).await?;
			_ = n;
			drop(f);
		}
		{
			let mut f = f.clone();
			let mut buf = vec![];
			let n = f.read_to_end(&mut buf).await?;
			_ = n;
			drop(f);
		}

		Ok(())
	}

	fn file_read(b: &mut Bencher) {
		let r = Arc::new(Box::leak(Box::new(
			tokio::runtime::Builder::new_multi_thread()
				.worker_threads(1)
				.enable_all()
				.build()
				.unwrap(),
		)));
		let f = File::from_std(StdFile::open("/tmp/x").unwrap()).unwrap();
		b.iter(|| {
			let mut f = f.clone();
			r.block_on(async move {
				let mut buf = vec![];
				let n = f.read_to_end(&mut buf).await.unwrap();
				assert_eq!(n, 70_000_000);
				f.seek(SeekFrom::Start(0)).await.unwrap();
			})
		});
	}

	fn tokio_read(b: &mut Bencher) {
		let r = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(1)
			.enable_all()
			.build()
			.unwrap();
		b.iter(|| {
			r.block_on(async {
				let mut f = TFile::open("/tmp/x").await.unwrap();
				let mut buf = vec![];
				let n = f.read_to_end(&mut buf).await.unwrap();
				assert_eq!(n, 70_000_000);
			})
		});
	}
}
