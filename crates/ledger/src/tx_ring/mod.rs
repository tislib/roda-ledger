// Lock-free SPSC-gated ring; not yet wired into the pipeline.
#![allow(dead_code)]

pub(crate) mod releaser;
pub(crate) mod ring;
pub(crate) mod writer;

#[cfg(test)]
mod tests;
