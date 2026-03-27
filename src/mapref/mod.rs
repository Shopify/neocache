//! Map reference types returned by lock-holding accessors.
//!
//! These types hold a per-shard `RwLock` guard and provide scoped access to
//! keys and values. Do not hold them across `.await` points.

pub mod entry;
pub mod multiple;
pub mod one;
