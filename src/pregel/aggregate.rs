use super::vertex::Vertex;
use std::any::Any;

pub trait Aggregate<V, E, M>
where
    M: Clone,
{
    fn report(&self, v: &Vertex<V, E, M>) -> Box<dyn Any>;
    fn aggregate(&self, a: Box<dyn Any>, b: Box<dyn Any>) -> Box<dyn Any>;
}
