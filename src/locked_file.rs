use std::{
	io::{Error, ErrorKind},
	ops::{Deref, DerefMut},
	path::PathBuf,
	sync::Arc,
	task::Poll,
};

use nix::errno::{Errno};
use tokio::{
	fs::File,
	io::{AsyncRead, AsyncWrite},
	sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock},
};

use crate::Result;

pub struct LockedFileWrite {
	f: Box<dyn AsyncWrite + Unpin>,
	lock: OwnedRwLockWriteGuard<i32>,
}

impl LockedFileWrite {
	pub(crate) async fn new(lock: Arc<RwLock<i32>>, fp: PathBuf) -> Result<Self> {
		let lock = lock.write_owned().await;
		let f = File::create(fp).await?;
		Ok(Self {
			f: Box::new(f),
			lock,
		})
	}
	pub(crate) async fn new_writer<T: AsyncWrite + Unpin + 'static>(
		lock: Arc<RwLock<i32>>,
		f: T,
	) -> Result<Self> {
		let lock = lock.write_owned().await;
		Ok(Self {
			f: Box::new(f),
			lock,
		})
	}
}

impl Deref for LockedFileWrite {
	type Target = Box<dyn AsyncWrite + Unpin>;

	fn deref(&self) -> &Self::Target {
		&self.f
	}
}

impl DerefMut for LockedFileWrite {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.f
	}
}

impl Drop for LockedFileWrite {
	fn drop(&mut self) {
		_ = self.f;
		_ = self.lock;
	}
}

pub struct LockedFileRead {
	lock: OwnedRwLockReadGuard<i32>,
	size: usize,
	index: usize,
}

impl LockedFileRead {
	pub(crate) async fn new(lock: Arc<RwLock<i32>>) -> Result<Self> {
		let lock = lock.read_owned().await;
		let size = 50 * 1024;
		Ok(Self {
			lock,
			size,
			index: 0,
		})
	}
}
pub fn cvt(t: isize) -> Result<usize> {
	if t == -1 {
		let err = Error::from_raw_os_error(Errno::last_raw());
		Err(err)
	} else {
		Ok(t as usize)
	}
}

impl AsyncRead for LockedFileRead {
	fn poll_read(
		mut self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		unsafe {
			let b = &mut buf.initialize_unfilled();
			let ret = cvt(libc::pread64(
				*self.lock,
				b.as_mut_ptr() as *mut libc::c_void,
				b.len(),
				self.index as i64,
			));

			match ret {
				Ok(n) => {
					self.index += n;
					buf.assume_init(n);
					buf.set_filled(self.index);
					Poll::Ready(Ok(()))
				}
				Err(err) if err.kind() != ErrorKind::WouldBlock => {
					return Poll::Ready(Err(err));
				}
				_ => Poll::Pending,
			}
		}
	}
}

impl Drop for LockedFileRead {
	fn drop(&mut self) {
		_ = self.lock;
	}
}
