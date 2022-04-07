use super::Vertex;

use std::any::Any;
use std::sync::Arc;

pub type AggVal = Arc<dyn Any + Send + Sync>;

pub trait Aggregate<V, E, M>: Send + Sync
where
    V: Send,
    E: Send,
    M: Send + Clone,
{
    fn report(&self, v: &Vertex<V, E, M>) -> AggVal;
    fn aggregate(&self, a: AggVal, b: AggVal) -> AggVal;
}
