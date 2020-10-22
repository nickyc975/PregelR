use super::aggregate::Aggregate;
use super::combine::Combine;
use super::context::Context;
use super::message::Message;
use super::state::State;
use super::vertex::Vertex;
use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::path::Path;
use std::sync::{Arc, Mutex};

static SEND_THRESHOLD: i32 = 10;

pub struct Worker<'a, V, E, M>
where
    M: Clone,
{
    pub id: i64,
    n_active_vertices: i64,
    pub time_cost: i64,
    pub n_msg_sent: RefCell<i64>,
    pub n_msg_recv: i64,
    vertices: HashMap<i64, Vertex<V, E, M>>,
    edges_path: &'a Box<Path>,
    vertices_path: &'a Box<Path>,
    context: Arc<Mutex<dyn Context<V, E, M>>>,
    edge_parser: &'a Box<dyn Fn(String) -> (i64, i64, E)>,
    vertex_parser: &'a Box<dyn Fn(String) -> (i64, V)>,
    compute: &'a Box<dyn Fn(&mut Vertex<V, E, M>)>,
    send_queues: RefCell<HashMap<i64, LinkedList<Message<M>>>>,
    combiner: &'a Option<Box<dyn Combine<M>>>,
    pub aggregators: Vec<Box<dyn Aggregate<V>>>,
}

impl<'a, V, E, M> Worker<'a, V, E, M>
where
    M: Clone,
{
    pub fn new(
        id: i64,
        context: Arc<Mutex<dyn Context<V, E, M>>>,
        edges_path: &'a Box<Path>,
        vertices_path: &'a Box<Path>,
        edge_parser: &'a Box<dyn Fn(String) -> (i64, i64, E)>,
        vertex_parser: &'a Box<dyn Fn(String) -> (i64, V)>,
        compute: &'a Box<dyn Fn(&mut Vertex<V, E, M>)>,
        combiner: &'a Option<Box<dyn Combine<M>>>,
    ) -> Self {
        Worker {
            id,
            context,
            n_active_vertices: 0,
            time_cost: 0,
            n_msg_sent: RefCell::new(0),
            n_msg_recv: 0,
            vertices: HashMap::new(),
            edges_path,
            vertices_path,
            edge_parser,
            vertex_parser,
            compute,
            combiner,
            send_queues: RefCell::new(HashMap::new()),
            aggregators: Vec::new(),
        }
    }

    pub fn local_n_vertices(&self) -> i64 {
        self.vertices.len() as i64
    }

    pub fn local_n_edges(&self) -> i64 {
        self.vertices
            .values()
            .map(|v| v.get_outer_edges().len())
            .sum::<usize>() as i64
    }

    fn send_messages(&self, queue: &mut LinkedList<Message<M>>) {
        let context = &mut self.context.lock().unwrap();
        while !queue.is_empty() {
            context.send_message(queue.pop_front().unwrap());
            *self.n_msg_sent.borrow_mut() += 1;
        }
    }
}

impl<'a, V, E, M> Context<V, E, M> for Worker<'a, V, E, M>
where
    M: Clone,
{
    fn state(&self) -> State {
        self.context.lock().unwrap().state()
    }

    fn superstep(&self) -> i64 {
        self.context.lock().unwrap().superstep()
    }

    fn num_edges(&self) -> i64 {
        self.context.lock().unwrap().num_edges()
    }

    fn num_vertices(&self) -> i64 {
        self.context.lock().unwrap().num_edges()
    }

    fn add_vertex(&mut self, id: i64) {
        self.context.lock().unwrap().add_vertex(id)
    }

    fn mark_as_done(&mut self, id: i64) {
        self.n_active_vertices -= 1;
    }

    fn send_message(&mut self, mut message: Message<M>) {
        let receiver = message.receiver;
        let mut queues = self.send_queues.borrow_mut();
        let queue_op = match queues.get_mut(&receiver) {
            Some(queue) => Some(queue),
            None => {
                queues.insert(receiver, LinkedList::new());
                queues.get_mut(&receiver)
            }
        };

        let queue = queue_op.unwrap();
        let initial_op = queue.pop_front();
        message.value = match (self.combiner, &initial_op) {
            (Some(combine), Some(initial)) => combine.combine(&message.value, &initial.value),
            _ => message.value,
        };
        queue.push_back(message);
        if queue.len() > SEND_THRESHOLD as usize {
            self.send_messages(queue);
        }
    }
}
