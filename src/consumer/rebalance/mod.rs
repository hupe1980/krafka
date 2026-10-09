//! Applying assignment changes on the poll path, and the listener the
//! application observes them through.

mod apply;
mod listener;

pub(super) use listener::ErasedRebalanceListener;
pub use listener::{ConsumerRebalanceListener, NoOpRebalanceListener};
