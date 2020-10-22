use super::worker::Worker;
use super::state::State;
use super::vertex::Vertex;
use super::combine::Combine;
use super::aggregate::Aggregate;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;

pub struct Master<V, E, M> where M: Clone {
    state: State,
    superstep: i64,
    nworkers: i64,
    n_active_workers: i64,
    workers: HashMap<i64, Worker<V, E, M>>,
    work_path: Box<Path>,
    edges_path: Box<Path>,
    vertices_path: Box<Path>,
    edge_parser: Box<dyn Fn(String) -> (i64, i64, E)>,
    vertex_parser: Box<dyn Fn(String) -> (i64, V)>,
    compute: Box<dyn Fn(&mut Vertex<V, E, M>)>,
    combiner: Option<Box<dyn Combine<M>>>,
    aggregators: Vec::<Rc<dyn Aggregate<V>>>,
}