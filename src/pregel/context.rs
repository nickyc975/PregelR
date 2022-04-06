use super::aggregate::{AggVal, Aggregate};
use super::combine::Combine;
use super::state::State;
use super::vertex::Vertex;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct Context<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub(crate) state: State,
    pub(crate) superstep: i64,
    pub(crate) num_edges: i64,
    pub(crate) num_vertices: i64,

    pub(crate) work_path: PathBuf,

    pub(crate) edge_parser: Option<Box<dyn Fn(&String) -> (i64, i64, E) + Send + Sync>>,
    pub(crate) vertex_parser: Option<Box<dyn Fn(&String) -> (i64, V) + Send + Sync>>,

    pub(crate) compute: Box<dyn Fn(&mut Vertex<V, E, M>) + Send + Sync>,

    pub(crate) combiner: Option<Box<dyn Combine<M>>>,
    pub(crate) aggregators: HashMap<String, Box<dyn Aggregate<V, E, M>>>,
    pub(crate) aggregated_values: RwLock<HashMap<String, AggVal>>,
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

    pub fn state(&self) -> State {
        self.state
    }

    pub fn superstep(&self) -> i64 {
        self.superstep
    }

    pub fn num_edges(&self) -> i64 {
        self.num_edges
    }

    pub fn num_vertices(&self) -> i64 {
        self.num_vertices
    }

    pub fn get_aggregated_value<T: 'static + Send + Sync>(&self, name: &String) -> Option<Arc<T>> {
        match self.aggregated_values.read() {
            Ok(aggregated_values) => match aggregated_values.get(name) {
                Some(value_box) => match value_box.clone().downcast::<T>() {
                    Ok(value) => Some(value),
                    Err(_) => None,
                },
                None => None,
            },
            Err(_) => None,
        }
    }
}
