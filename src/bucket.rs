/*
type Bucket interface {
	Append(key string, r io.Reader, middlewares ...mw.Middleware) (err error)
	AppendFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error)
	Bucket(names ...string) Bucket
	Buckets(rev bool) (out []string)
	CreateBucket(names ...string) (Bucket, error)
	Delete(key string) (err error)
	DeleteBucket(name string) (err error)
	ForEach(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error
	ForEachReverse(fn func(key string, value io.Reader) error, middlewares ...mw.Middleware) error
	Get(key string, middlewares ...mw.Middleware) (_ io.ReadCloser, err error)
	GetAndDelete(key string, fn func(r io.Reader) error, middlewares ...mw.Middleware) (err error)
	GetAndRename(key string, nBkt Bucket, nKey string, overwrite bool, fn ReaderFn, mws ...mw.Middleware) (err error)
	Group(mws ...mw.Middleware) Bucket
	Keys(reverse bool) (out []string)
	Name() string
	NextID() *big.Int
	Path() string
	Put(key string, r io.Reader, middlewares ...mw.Middleware) (err error)
	PutFunc(key string, fn func(w io.Writer) error, middlewares ...mw.Middleware) (err error)
	PutTimed(key string, r io.Reader, expireAfter time.Duration, middlewares ...mw.Middleware) (err error)
	PutTimedFunc(key string, fn func(w io.Writer) error, expireAfter time.Duration, middlewares ...mw.Middleware) (err error)
	Export(w io.Writer) (err error)
	Stat(key string) (fi os.FileInfo, err error)
	SetExtraData(fileKey, key string, val string) error
	GetExtraData(fileKey, key string) (out string)
	ExtraData(fileKey string) (out map[string]string)
	AllExtraData() (out map[string]map[string]string)
}
*/

use std::{
	collections::{HashMap, hash_map::Entry},
	path::PathBuf,
	sync::Arc,
};

use tokio::{
	fs::File,
	io::{AsyncWriteExt, BufReader, BufWriter},
	sync::RwLock,
};

use crate::{
	Result,
	locked_file::{LockedFileRead, LockedFileWrite},
};

pub trait BucketT: Sized + Send {
	async fn bucket(&self, name: &str) -> Option<Self>;
}

type FileLock = Arc<RwLock<()>>;

#[derive(Clone)]
pub struct Bucket {
	path: PathBuf,
	files: Arc<RwLock<HashMap<String, FileLock>>>,
	buckets: Arc<RwLock<HashMap<String, Self>>>,
}
impl BucketT for Bucket {
	async fn bucket(&self, name: &str) -> Option<Self> {
		let lock = self.buckets.read().await;
		lock.get(name).cloned()
	}
}
impl Bucket {
	pub fn new(path: PathBuf) -> Result<Self> {
		std::fs::create_dir_all(&path)?;
		Ok(Self {
			path,
			files: Default::default(),
			buckets: Default::default(),
		})
	}

	pub async fn bucket(&self, name: &str) -> Option<Self> {
		let lock = self.buckets.read().await;
		lock.get(name).cloned()
	}

	pub async fn bucket_or_create(&self, name: &str) -> Result<Self> {
		let mut lock = self.buckets.write().await;
		let bucket = match lock.entry(name.to_owned()) {
			Entry::Occupied(o) => o.into_mut(),
			Entry::Vacant(v) => v.insert(Bucket::new(self.path.join(name))?),
		};
		Ok(bucket.clone())
	}

	pub async fn buffered_read(
		&self,
		name: &str,
		reader: impl AsyncFn(&mut BufReader<File>) -> Result<()>,
	) -> Result<()> {
		let lock = {
			let files = self.files.read().await;
			let lock = files.get(name);
			if lock.is_none() {
				return Err(std::io::Error::new(
					std::io::ErrorKind::NotFound,
					"file not found",
				));
			}
			unsafe { lock.unwrap_unchecked().clone() }
		};

		let _lock = lock.read().await;
		let path = self.path.join(name);
		let f = File::open(path).await.unwrap();
		let mut f = BufReader::new(f);
		reader(&mut f).await
	}

	pub async fn buffered_write(
		&self,
		name: &str,
		reader: impl AsyncFn(&mut BufWriter<File>) -> Result<()>,
	) -> Result<()> {
		let lock = {
			let mut files = self.files.write().await;
			let lock = files
				.entry(name.to_owned())
				.or_insert_with(|| Default::default());
			lock.clone()
		};

		let _lock = lock.write().await;
		let path = self.path.join(name);
		let f = File::create(path).await?;
		let mut f = BufWriter::new(f);
		reader(&mut f).await?;
		f.flush().await
	}

	pub async fn write_file(&self, name: &str) -> Result<LockedFileWrite> {
		let lock = {
			let mut files = self.files.write().await;
			let lock = files
				.entry(name.to_owned())
				.or_insert_with(|| Default::default());
			lock.clone()
		};
		let path = self.path.join(name);
		LockedFileWrite::new(lock, path).await
	}

	pub async fn read_file(&self, name: &str) -> Result<LockedFileRead> {
		let lock = {
			let files = self.files.read().await;
			let lock = files.get(name);
			if lock.is_none() {
				return Err(std::io::Error::new(
					std::io::ErrorKind::NotFound,
					"file not found",
				));
			}
			unsafe { lock.unwrap_unchecked().clone() }
		};

		let path = self.path.join(name);
		LockedFileRead::new(lock, path).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};

	#[tokio::test]
	async fn test_bucket() -> Result<()> {
		let bb = Bucket::new(PathBuf::from("/tmp/b")).expect("bucket");
		let b = bb.bucket_or_create("test1").await?;
		tokio::spawn(async move {
			b.buffered_write("test", async |w| {
				w.write_all(b"hello").await?;
				Ok(())
			})
			.await
			.unwrap();
			b.buffered_read("test", async |r| {
				let mut s = String::new();
				r.read_to_string(&mut s).await?;
				println!("{}", s);
				Ok(())
			})
			.await
			.unwrap();
		});
		let b = bb.bucket_or_create("test2").await?;
		{
			let mut lf = b.write_file("test").await?;
			lf.write_all(b"world").await?;
		}

		let mut lf = b.read_file("test").await?;
		let mut s = String::new();
		lf.read_to_string(&mut s).await?;
		assert_eq!(s, "world");
		Ok(())
	}
}
