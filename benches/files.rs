use criterion::*;
use fsdb::mmap_file::MmapFile;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::io::AsyncReadExt;
extern crate criterion;

fn bench_files(c: &mut Criterion) {
	let r = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(16)
		.enable_all()
		.build()
		.unwrap();

	c.bench_function("MmapFile", |b| {
		let f = MmapFile::open_sync("/tmp/x").unwrap();
		b.to_async(&r).iter(|| async {
			let mut futs = FuturesUnordered::new();
			for _ in 0..50 {
				let mut f = f.clone();
				futs.push(r.spawn(async move {
					let mut buf = vec![];
					_ = f.read_to_end(&mut buf).await.unwrap();
				}));
			}
			while let Some(result) = futs.next().await {
				_ = result
			}
		})
	});

	c.bench_function("Tokio file", |b| {
		b.to_async(&r).iter(|| async {
			let mut futs = FuturesUnordered::new();
			for _ in 0..50 {
				futs.push(r.spawn(async move {
					let mut f = tokio::fs::File::open("/tmp/x").await.unwrap();
					let mut buf = vec![];
					_ = f.read_to_end(&mut buf).await.unwrap();
				}));
			}
			while let Some(result) = futs.next().await {
				_ = result
			}
		})
	});
}

criterion_group!(benches, bench_files);
criterion_main!(benches);
