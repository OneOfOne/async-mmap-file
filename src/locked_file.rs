use std::{
	ops::{Deref, DerefMut},
	path::PathBuf,
	sync::Arc,
};

use tokio::{
	fs::File,
	io::{AsyncRead, AsyncWrite, AsyncWriteExt},
	sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock},
};

use crate::Result;

pub struct LockedFileWrite {
	f: Box<dyn AsyncWrite + Unpin>,
	lock: OwnedRwLockWriteGuard<()>,
}

impl LockedFileWrite {
	pub(crate) async fn new(lock: Arc<RwLock<()>>, fp: PathBuf) -> Result<Self> {
		let lock = lock.write_owned().await;
		let f = File::create(fp).await?;
		Ok(Self {
			f: Box::new(f),
			lock,
		})
	}
	pub(crate) async fn new_writer<T: AsyncWrite + Unpin + 'static>(
		lock: Arc<RwLock<()>>,
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
	f: Box<dyn AsyncRead + Unpin>,
	lock: OwnedRwLockReadGuard<()>,
}

impl LockedFileRead {
	pub(crate) async fn new(lock: Arc<RwLock<()>>, fp: PathBuf) -> Result<Self> {
		let lock = lock.read_owned().await;
		let f = File::open(fp).await?;
		Ok(Self {
			f: Box::new(f),
			lock,
		})
	}
}

impl Deref for LockedFileRead {
	type Target = Box<dyn AsyncRead + Unpin>;

	fn deref(&self) -> &Self::Target {
		&self.f
	}
}

impl DerefMut for LockedFileRead {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.f
	}
}

impl Drop for LockedFileRead {
	fn drop(&mut self) {
		_ = self.f;
		_ = self.lock;
	}
}
