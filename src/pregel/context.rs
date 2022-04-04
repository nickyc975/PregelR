use super::aggregate::Aggregate;
use super::combine::Combine;
use super::state::State;
use super::vertex::Vertex;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Context<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
    pub state: State,
    pub superstep: i64,
    pub num_edges: i64,
    pub num_vertices: i64,

    pub work_path: String,

    pub edge_parser: Option<Box<dyn Fn(&String) -> (i64, i64, E)>>,
    pub vertex_parser: Option<Box<dyn Fn(&String) -> (i64, V)>>,

    pub compute: Box<dyn Fn(&mut Vertex<V, E, M>)>,

    pub combiner: Option<Box<dyn Combine<M>>>,
    pub aggregators: HashMap<String, Box<dyn Aggregate<V, E, M>>>,
    pub aggregated_values: RefCell<HashMap<String, Box<dyn Send + Sync>>>,
}

unsafe impl<V, E, M> Send for Context<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
}

unsafe impl<V, E, M> Sync for Context<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
}

impl<V, E, M> Context<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
    pub fn new(compute: Box<dyn Fn(&mut Vertex<V, E, M>)>, work_path: String) -> Self {
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
