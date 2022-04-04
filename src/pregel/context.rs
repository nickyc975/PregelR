use super::aggregate::Aggregate;
use super::combine::Combine;
use super::state::State;
use super::vertex::Vertex;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

pub struct Context<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub state: State,
    pub superstep: i64,
    pub num_edges: i64,
    pub num_vertices: i64,

    pub work_path: PathBuf,

    pub edge_parser: Option<Box<dyn Fn(&String) -> (i64, i64, E) + Send + Sync>>,
    pub vertex_parser: Option<Box<dyn Fn(&String) -> (i64, V) + Send + Sync>>,

    pub compute: Box<dyn Fn(&mut Vertex<V, E, M>) + Send + Sync>,

    pub combiner: Option<Box<dyn Combine<M>>>,
    pub aggregators: HashMap<String, Box<dyn Aggregate<V, E, M>>>,
    pub aggregated_values: RwLock<HashMap<String, Box<dyn Send + Sync>>>,
}

impl<V, E, M> Context<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(
        compute: Box<dyn Fn(&mut Vertex<V, E, M>) + Send + Sync>,
        work_path: PathBuf,
    ) -> Self {
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
            aggregated_values: RwLock::new(HashMap::new()),
        }
    }
}
