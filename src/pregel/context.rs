use super::aggregate::Aggregate;
use super::combine::Combine;
use super::state::State;
use super::vertex::Vertex;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

pub struct Context<'a, V, E, M>
where
    V: Send + Sync,
    E: Send + Sync,
    M: Send + Sync + Clone,
{
    pub state: State,
    pub superstep: i64,
    pub num_edges: i64,
    pub num_vertices: i64,

    pub work_path: &'a Path,

    pub edge_parser: Option<&'a dyn Fn(&String) -> (i64, i64, E)>,
    pub vertex_parser: Option<&'a dyn Fn(&String) -> (i64, V)>,
    pub compute: &'a dyn Fn(&mut Vertex<V, E, M>),

    pub combiner: Option<&'a dyn Combine<M>>,
    pub aggregators: HashMap<String, &'a dyn Aggregate<V, E, M>>,
    pub aggregated_values: RefCell<HashMap<String, Box<dyn Send + Sync>>>,
}

unsafe impl<'a, V, E, M> Send for Context<'a, V, E, M>
where
    V: Send + Sync,
    E: Send + Sync,
    M: Send + Sync + Clone,
{
}
unsafe impl<'a, V, E, M> Sync for Context<'a, V, E, M>
where
    V: Send + Sync,
    E: Send + Sync,
    M: Send + Sync + Clone,
{
}

impl<'a, V, E, M> Context<'a, V, E, M>
where
    V: Send + Sync,
    E: Send + Sync,
    M: Send + Sync + Clone,
{
    pub fn new(compute: &'a dyn Fn(&mut Vertex<V, E, M>), work_path: &'a Path) -> Self {
        Context {
            compute,
            work_path,
            state: State::INITIALIZED,
            superstep: 0,
            num_edges: 0,
            num_vertices: 0,
            edge_parser: None,
            vertex_parser: None,
            combiner: None,
            aggregators: HashMap::new(),
            aggregated_values: RefCell::new(HashMap::new()),
        }
    }
}
