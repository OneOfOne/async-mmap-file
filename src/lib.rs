#![feature(test, async_drop, impl_trait_in_assoc_type, let_chains)]
mod mmap_file;
pub use mmap_file::*;

pub type Result<T> = std::io::Result<T>;
