// Lock-free SPSC ring with no Arc: one writer, one reader (which also releases).
#![allow(dead_code)]

pub mod reader;
pub mod ring;
pub mod writer;

#[cfg(test)]
mod tests;
