#![feature(test, async_drop, impl_trait_in_assoc_type, let_chains)]

use std::io::Error;

use nix::errno::Errno;
extern crate test;
pub mod bucket;
pub mod file;
pub mod locked_file;
pub mod mmap_file;

type Result<T> = std::io::Result<T>;
pub fn cvt(t: isize) -> Result<usize> {
	if t == -1 {
		let err = Error::from_raw_os_error(Errno::last_raw());
		Err(err)
	} else {
		Ok(t as usize)
	}
}
