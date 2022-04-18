use crate::Vertex;

use std::any::Any;

pub type AggVal = dyn Any + Send + Sync;

pub trait Aggregate<V, E, M>: Send + Sync {
    fn report(&self, v: &Vertex<V, E, M>) -> Box<AggVal>;
    fn aggregate(&self, a: Box<AggVal>, b: Box<AggVal>) -> Box<AggVal>;
}
