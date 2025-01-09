use std::{
	fs::Metadata,
	io::{ErrorKind, SeekFrom},
	os::fd::AsFd,
	path::Path,
	sync::Arc,
	thread::{self},
};

use crate::Result;
use nix::{
	errno::Errno,
	sys::{aio::*, signal::SigevNotify},
};
use std::fs::File as StdFile;
use tokio::task::yield_now;
use tokio::{fs::File as TFile, io::AsyncSeekExt};
#[derive(Clone)]
pub struct File {
	f: Arc<TFile>,
	write_offset: usize,
	read_offset: usize,
}

impl File {
	pub async fn from_file(mut f: TFile) -> Result<Self> {
		let pos = f.seek(SeekFrom::Current(0)).await?;
		Ok(Self {
			f: Arc::new(f),
			write_offset: pos as usize,
			read_offset: pos as usize,
		})
	}
	pub async fn create<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = TFile::create(p).await?;
		Ok({
			Self {
				f: f.into(),
				write_offset: 0,
				read_offset: 0,
			}
		})
	}

	pub async fn open<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = TFile::open(p).await?;
		Ok({
			Self {
				f: f.into(),
				write_offset: 0,
				read_offset: 0,
			}
		})
	}

	pub fn open_std<P: AsRef<Path>>(p: P) -> Result<Self> {
		let f = StdFile::open(p)?;
		Ok({
			Self {
				f: Arc::new(TFile::from_std(f)),
				write_offset: 0,
				read_offset: 0,
			}
		})
	}

	pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
		let mut aiow = Box::pin(AioWrite::new(
			self.f.as_fd(),
			self.write_offset as i64,
			buf,
			0,
			SigevNotify::SigevNone,
		));
		aiow.as_mut().submit()?;
		while aiow.as_mut().error() == Err(Errno::EINPROGRESS) {
			yield_now().await;
		}
		let n = aiow.as_mut().aio_return()?;
		self.write_offset += n;
		Ok(n)
	}

	pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		let mut aiow = Box::pin(AioRead::new(
			self.f.as_fd(),
			self.read_offset as i64,
			buf,
			0,
			SigevNotify::SigevNone,
		));
		aiow.as_mut().submit()?;
		while aiow.as_mut().error() == Err(Errno::EINPROGRESS) {
			yield_now().await;
		}
		let n = aiow.as_mut().aio_return()?;
		self.read_offset += n;
		Ok(n)
	}

	pub async fn metadata(&self) -> Result<Metadata> {
		self.f.metadata().await
	}

	pub async fn size(&self) -> Result<usize> {
		self.metadata().await.map(|m| m.len() as usize)
	}

	pub async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
		let size = self.size().await?;
		buf.resize(size, 0);
		let n = self.read(buf).await?;
		if n != size {
			return Err(ErrorKind::UnexpectedEof.into());
		}
		Ok(n)
	}
	pub async fn seek(&mut self, pos: SeekFrom) {
		self.write_offset = 0
	}
}

impl Drop for File {
	fn drop(&mut self) {
		let mut aiof = Box::pin(AioFsync::new(
			self.f.as_fd(),
			AioFsyncMode::O_SYNC,
			0,
			SigevNotify::SigevNone,
		));
		aiof.as_mut().submit().expect("aio_fsync failed early");
		while aiof.as_mut().error() == Err(Errno::EINPROGRESS) {
			thread::yield_now();
		}
		aiof.as_mut().aio_return().expect("aio_fsync failed late");
	}
}

#[cfg(test)]
mod aio_tests {
	use super::*;
	use test::Bencher;
	use tokio::io::AsyncReadExt;

	#[tokio::test]
	async fn write_read() -> Result<()> {
		write().await?;
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
		let mut f = File::open(&"/tmp/x").await?;
		let mut buf = vec![];
		let n = f.read_to_end(&mut buf).await?;
		_ = n;
		Ok(())
	}

	#[bench]
	fn file_read(b: &mut Bencher) {
		let r = Arc::new(Box::leak(Box::new(
			tokio::runtime::Builder::new_multi_thread()
				.worker_threads(1)
				.enable_all()
				.build()
				.unwrap(),
		)));
		let mut f = File::open_std(&"/tmp/x").unwrap();
		let r2 = r.clone();
		b.iter(|| {
			let mut f = f.clone();
			r.block_on(async move {
				let mut buf = vec![];
				let n = f.read_to_end(&mut buf).await.unwrap();
				_ = n;
			})
		});
	}

	#[bench]
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
				_ = n;
			})
		});
	}
}
