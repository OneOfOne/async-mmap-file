#![feature(file_lock)]
mod mmap_file;
pub use mmap_file::*;

mod file_map;
pub use file_map::*;

pub type Result<T> = std::io::Result<T>;
