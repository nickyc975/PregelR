use super::aggregate::Aggregate;
use super::combine::Combine;
use super::state::State;
use super::vertex::Vertex;
use std::any::Any;
use std::collections::HashMap;
use std::path::Path;

pub struct Context<V, E, M>
where
    M: Clone,
{
    pub state: State,
    pub superstep: i64,
    pub num_edges: i64,
    pub num_vertices: i64,

    pub work_path: Box<Path>,
    pub edges_path: Option<Box<Path>>,
    pub vertices_path: Option<Box<Path>>,

    pub edge_parser: Option<Box<dyn Fn(String) -> (i64, i64, E)>>,
    pub vertex_parser: Option<Box<dyn Fn(String) -> (i64, V)>>,
    pub compute: Box<dyn Fn(&mut Vertex<V, E, M>)>,

    pub combiner: Option<Box<dyn Combine<M>>>,
    pub aggregators: HashMap<String, Box<dyn Aggregate<V, E, M>>>,
    pub aggregated_values: HashMap<String, Box<dyn Any>>,
}

impl<V, E, M> Context<V, E, M>
where
    M: Clone,
{
    pub fn new(compute: Box<dyn Fn(&mut Vertex<V, E, M>)>, work_path: Box<Path>) -> Self {
        Context {
            compute,
            work_path,
            state: State::INITIALIZED,
            superstep: 0,
            num_edges: 0,
            num_vertices: 0,
            edges_path: None,
            vertices_path: None,
            edge_parser: None,
            vertex_parser: None,
            combiner: None,
            aggregators: HashMap::new(),
            aggregated_values: HashMap::new(),
        }
    }
}
