// Lock-free SPSC-gated ring: single writer, single releaser, copy-out readers.
#![allow(dead_code)]

pub mod releaser;
pub mod ring;
pub mod writer;

#[cfg(test)]
mod tests;
