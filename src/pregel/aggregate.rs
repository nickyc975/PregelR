use super::vertex::Vertex;

use std::any::Any;

pub type AggVal = Box<dyn Any + Send + Sync>;

pub trait Aggregate<V, E, M>: Send + Sync
where
    V: Send,
    E: Send,
    M: Send + Clone,
{
    fn report(&self, v: &Vertex<V, E, M>) -> AggVal;
    fn aggregate(&self, a: AggVal, b: AggVal) -> AggVal;
}
