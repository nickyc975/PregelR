use super::context::Context;
use super::message::Message;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::Arc;

pub struct Vertex<V, E, M>
where
    M: Clone,
{
    pub id: i64,
    pub value: V,
    context: Arc<RefCell<dyn Context<V, E, M>>>,
    outer_edges: HashMap<i64, (i64, i64, E)>,
    odd_recv_queue: LinkedList<M>,
    even_recv_queue: LinkedList<M>,
}

impl<V, E, M> Vertex<V, E, M>
where
    M: Clone,
{
    pub fn new(id: i64, value: V, context: Arc<RefCell<dyn Context<V, E, M>>>) -> Self {
        Vertex {
            id,
            value,
            context,
            outer_edges: HashMap::new(),
            odd_recv_queue: LinkedList::new(),
            even_recv_queue: LinkedList::new(),
        }
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
        match self.outer_edges.get(&target) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn get_outer_edge_to(&self, target: i64) -> Option<&(i64, i64, E)> {
        self.outer_edges.get(&target)
    }

    pub fn get_outer_edges(&self) -> &HashMap<i64, (i64, i64, E)> {
        &self.outer_edges
    }

    pub fn send_message_to(&self, receiver: i64, value: M) {
        let message = Message::new(value, self.id, receiver, self.context.borrow().superstep());
        self.context.borrow_mut().send_message(message);
    }

    pub fn send_message(&self, value: M) {
        for target in self.outer_edges.keys() {
            self.send_message_to(*target, value.clone());
        }
    }

    pub fn has_messages(&self) -> bool {
        if self.context.borrow().superstep() & 2 == 0 {
            !self.odd_recv_queue.is_empty()
        } else {
            !self.even_recv_queue.is_empty()
        }
    }

    pub fn read_message(&mut self) -> M {
        if self.context.borrow().superstep() & 2 == 0 {
            self.odd_recv_queue.pop_front().unwrap()
        } else {
            self.even_recv_queue.pop_front().unwrap()
        }
    }

    pub fn vote_to_halt(&self) {
        self.context.borrow_mut().mark_as_done(self.id);
    }

    pub fn receive_message(&mut self, message: M) {
        if self.context.borrow().superstep() & 2 == 0 {
            self.odd_recv_queue.push_back(message);
        } else {
            self.even_recv_queue.push_back(message);
        }
    }

    pub fn has_next_step_message(&self) -> bool {
        if self.context.borrow().superstep() & 2 == 0 {
            !self.even_recv_queue.is_empty()
        } else {
            !self.odd_recv_queue.is_empty()
        }
    }

    pub fn read_next_step_message(&mut self) -> M {
        if self.context.borrow().superstep() & 2 == 0 {
            self.even_recv_queue.pop_front().unwrap()
        } else {
            self.odd_recv_queue.pop_front().unwrap()
        }
    }
}
