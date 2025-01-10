use std::io::SeekFrom;

use criterion::async_executor::FuturesExecutor;
use criterion::*;
use fsdb::file::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
#[macro_use]
extern crate criterion;

fn bench_files(c: &mut Criterion) {
	let r = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(1)
		.enable_all()
		.build()
		.unwrap();
	c.bench_function("File", |b| {
		let f = File::from_std(std::fs::File::open("/tmp/x").unwrap()).unwrap();
		b.to_async(&r).iter(|| async {
			let mut f = f.clone();
			let mut buf = vec![];
			let n = f.read_to_end(&mut buf).await.unwrap();
			assert_eq!(n, 70_000_000);
		})
	});
	c.bench_function("Tokio file", |b| {
		b.to_async(&r).iter(|| async {
			let mut f = tokio::fs::File::open("/tmp/x").await.unwrap();
			let mut buf = vec![];
			let n = f.read_to_end(&mut buf).await.unwrap();
			assert_eq!(n, 70_000_000);
		})
	});
}

criterion_group!(benches, bench_files);
criterion_main!(benches);
