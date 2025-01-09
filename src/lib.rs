#![feature(test, async_drop, impl_trait_in_assoc_type)]
extern crate test;
pub mod bucket;
pub mod file;
pub mod locked_file;

type Result<T> = std::io::Result<T>;

pub fn add(left: u64, right: u64) -> u64 {
	left + right
}

#[cfg(test)]
mod tests {
	use std::{fs::File, io::Read, os::fd::AsRawFd};

	use test::Bencher;

	use super::*;

	#[bench]
	fn it_works(b: &mut Bencher) {
		let mut f = File::open("/tmp/x").unwrap();
		let fd = f.as_raw_fd();
		unsafe {
			let flags = libc::fcntl(fd, libc::F_GETFL, 0);
			println!("flags: {}", flags);
			let flags = libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
			println!("flags: {}", flags);
		}
		let mut s = String::new();
		println!("{:?}", f.read_to_string(&mut s));
		println!("{:?}", f.read_to_string(&mut s));
		let result = add(2, 2);
		assert_eq!(result, 4);
	}
}
