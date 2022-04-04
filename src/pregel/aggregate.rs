use super::vertex::Vertex;

pub trait Aggregate<V, E, M>
where
V: Send + Sync,
E: Send + Sync,
M: Send + Sync + Clone,
{
    fn report(&self, v: &Vertex<V, E, M>) -> Box<dyn Send + Sync>;
    fn aggregate(&self, a: Box<dyn Send + Sync>, b: Box<dyn Send + Sync>) -> Box<dyn Send + Sync>;
}
