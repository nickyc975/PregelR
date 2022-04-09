use super::message::Message;

use std::cell::RefCell;
use std::collections::hash_map::{self, HashMap};

pub struct Vertex<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    id: i64,
    pub value: Option<V>,
    active: bool,
    removed: bool,
    outer_edges: HashMap<i64, (i64, i64, E)>,
    pub(crate) recv_queue: RefCell<Vec<M>>,
    pub(crate) send_queue: RefCell<Vec<Message<M>>>,
}

impl<V, E, M> Vertex<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(id: i64) -> Self {
        Vertex {
            id,
            value: None,
            active: true,
            removed: false,
            outer_edges: HashMap::new(),
            recv_queue: RefCell::new(Vec::new()),
            send_queue: RefCell::new(Vec::new()),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn activate(&mut self) {
        self.active = true;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }

    pub fn remove(&mut self) {
        self.removed = true;
    }

    pub fn removed(&self) -> bool {
        self.removed
    }

    pub fn add_outer_edge(&mut self, edge: (i64, i64, E)) {
        if edge.0 == self.id {
            self.outer_edges.insert(edge.1, edge);
        }
    }

    pub fn remove_outer_edge(&mut self, target: i64) {
        self.outer_edges.remove(&target);
    }

    pub fn has_outer_edge_to(&self, target: i64) -> bool {
        self.outer_edges.contains_key(&target)
    }

    pub fn get_outer_edge_to(&self, target: i64) -> Option<&(i64, i64, E)> {
        self.outer_edges.get(&target)
    }

    pub fn get_outer_edges(&self) -> hash_map::Values<i64, (i64, i64, E)> {
        self.outer_edges.values()
    }

    pub fn send_message_to(&self, receiver: i64, value: M) {
        let message = Message::new(value, self.id, receiver);
        self.send_queue.borrow_mut().push(message);
    }

    pub fn send_message(&self, value: M) {
        for target in self.outer_edges.keys() {
            self.send_message_to(*target, value.clone());
        }
    }

    pub fn has_messages(&self) -> bool {
        !self.recv_queue.borrow().is_empty()
    }

    pub fn read_message(&mut self) -> Option<M> {
        self.recv_queue.borrow_mut().pop()
    }
}
