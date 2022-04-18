use crate::Combine;
use crate::ComputeFn;
use crate::EdgeParserFn;
use crate::VertexParserFn;
use crate::{AggVal, Aggregate};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub enum Operation {
    Load,
    Compute,
}

pub struct Context<V, E, M> {
    pub(crate) operation: Operation,
    pub(crate) superstep: i64,
    pub(crate) num_edges: i64,
    pub(crate) num_vertices: i64,

    pub(crate) work_path: PathBuf,

    pub(crate) edge_parser: Option<Box<EdgeParserFn<E>>>,
    pub(crate) vertex_parser: Option<Box<VertexParserFn<V>>>,

    pub(crate) compute: Box<ComputeFn<V, E, M>>,

    pub(crate) combiner: Option<Box<dyn Combine<M>>>,
    pub(crate) aggregators: HashMap<String, Box<dyn Aggregate<V, E, M>>>,
    pub(crate) aggregated_values: HashMap<String, Arc<AggVal>>,
}

impl<V, E, M> Context<V, E, M> {
    pub fn new(compute: Box<ComputeFn<V, E, M>>, work_path: PathBuf) -> Self {
        Context {
            compute,
            work_path,
            operation: Operation::Load,
            superstep: 0,
            num_edges: 0,
            num_vertices: 0,
            edge_parser: None,
            vertex_parser: None,
            combiner: None,
            aggregators: HashMap::new(),
            aggregated_values: HashMap::new(),
        }
    }

    pub fn operation(&self) -> Operation {
        self.operation
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
        match self.aggregated_values.get(name) {
            Some(value_box) => match value_box.clone().downcast::<T>() {
                Ok(value) => Some(value),
                Err(_) => None,
            },
            None => None,
        }
    }
}
