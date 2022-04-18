use std::sync::RwLockReadGuard;

mod channel;
pub(crate) use channel::*;

mod message;
pub(crate) use message::*;

mod worker;
pub(crate) use worker::*;

mod aggregate;
pub use aggregate::*;

mod combine;
pub use combine::*;

mod context;
pub use context::*;

mod master;
pub use master::*;

mod vertex;
pub use vertex::*;

pub type EdgeParserFn<E> = dyn Fn(&String) -> Option<(i64, i64, E)> + Send + Sync;

pub type VertexParserFn<V> = dyn Fn(&String) -> Option<(i64, V)> + Send + Sync;

pub type ComputeFn<V, E, M> =
    dyn Fn(&mut Vertex<V, E, M>, &RwLockReadGuard<Context<V, E, M>>) + Send + Sync;
