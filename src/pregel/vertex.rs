use super::context::Context;
use super::message::Message;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::RwLockReadGuard;

pub struct Vertex<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub id: i64,
    pub value: Option<V>,
    active: bool,
    outer_edges: HashMap<i64, (i64, i64, E)>,
    odd_recv_queue: LinkedList<M>,
    even_recv_queue: LinkedList<M>,
    pub(crate) send_queue: RefCell<LinkedList<Message<M>>>,
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
            outer_edges: HashMap::new(),
            odd_recv_queue: LinkedList::new(),
            even_recv_queue: LinkedList::new(),
            send_queue: RefCell::new(LinkedList::new()),
        }
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

    pub fn get_outer_edges(&self) -> &HashMap<i64, (i64, i64, E)> {
        &self.outer_edges
    }

    pub fn send_message_to(&self, receiver: i64, value: M) {
        let message = Message::new(value, self.id, receiver);
        self.send_queue.borrow_mut().push_back(message);
    }

    pub fn send_message(&self, value: M) {
        for target in self.outer_edges.keys() {
            self.send_message_to(*target, value.clone());
        }
    }

    pub fn has_messages(&self, context: &RwLockReadGuard<Context<V, E, M>>) -> bool {
        if context.superstep % 2 == 0 {
            !self.odd_recv_queue.is_empty()
        } else {
            !self.even_recv_queue.is_empty()
        }
    }

    pub fn read_message(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) -> Option<M> {
        if context.superstep % 2 == 0 {
            self.odd_recv_queue.pop_front()
        } else {
            self.even_recv_queue.pop_front()
        }
    }

    pub fn receive_message(&mut self, context: &RwLockReadGuard<Context<V, E, M>>, message: M) {
        if context.superstep % 2 == 0 {
            self.even_recv_queue.push_back(message);
        } else {
            self.odd_recv_queue.push_back(message);
        }
    }

    pub fn has_next_step_message(&self, context: &RwLockReadGuard<Context<V, E, M>>) -> bool {
        if context.superstep % 2 == 0 {
            !self.even_recv_queue.is_empty()
        } else {
            !self.odd_recv_queue.is_empty()
        }
    }

    pub fn read_next_step_message(
        &mut self,
        context: &RwLockReadGuard<Context<V, E, M>>,
    ) -> Option<M> {
        if context.superstep % 2 == 0 {
            self.even_recv_queue.pop_front()
        } else {
            self.odd_recv_queue.pop_front()
        }
    }
}
