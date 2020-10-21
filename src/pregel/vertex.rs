use super::context::Context;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::Arc;

struct Vertex<V, E, M> {
    id: i64,
    value: V,
    context: Arc<dyn Context<V, E, M>>,
    outer_edges: HashMap<i64, (i64, i64, E)>,
    odd_recv_queue: LinkedList<M>,
    even_recv_queue: LinkedList<M>,
}
