use async_mmap_file::FileMap;
use criterion::*;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::{
	fs::File,
	io::{AsyncReadExt, AsyncWriteExt},
};
extern crate criterion;

fn file_benches(c: &mut Criterion) {
	const SIZE: usize = 100 * 1024 * 1024;
	let r = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(16)
		.enable_all()
		.build()
		.unwrap();

	r.block_on(async {
		let path = "/tmp/x";
		let mut f = File::create(&path).await.expect("create failed");
		let buf = vec!['@' as u8; SIZE];
		f.write_all(&buf).await.expect("write all failed");
		f.flush().await.expect("flush failed");
	});

	c.bench_function("MmapFile", |b| {
		let fm = FileMap::new();
		b.to_async(&r).iter(|| async {
			let f = fm.get("/tmp/x").await.unwrap();
			let mut futs = FuturesUnordered::new();
			for _ in 0..50 {
				let f = f.clone();
				futs.push(r.spawn(async move {
					let mut f = f.clone();
					let mut buf = vec![];
					let n = f.read_to_end(&mut buf).await.unwrap();
					assert_eq!(n, SIZE);
				}));
			}

			while let Some(_) = futs.next().await {}
		})
	});

	c.bench_function("Tokio file", |b| {
		b.to_async(&r).iter(|| async {
			let mut futs = FuturesUnordered::new();
			for _ in 0..50 {
				futs.push(r.spawn(async move {
					let mut f = tokio::fs::File::open("/tmp/x").await.unwrap();
					let mut buf = vec![];
					let n = f.read_to_end(&mut buf).await.unwrap();
					assert_eq!(n, SIZE);
				}));
			}
			while let Some(_) = futs.next().await {}
		})
	});
}

criterion_group!(benches, file_benches);
criterion_main!(benches);
