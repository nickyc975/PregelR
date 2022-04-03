use super::aggregate::Aggregate;
use super::combine::Combine;
use super::context::Context;
use super::message::{ChannelMessage, Message};
use super::state::State;
use super::vertex::Vertex;
use std::collections::{HashMap, LinkedList};

use std::any::Any;
use std::cell::RefCell;
use std::fs;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::time::Instant;

static SEND_THRESHOLD: i32 = 10;

pub struct Worker<'a, V, E, M>
where
    M: Clone,
{
    pub id: i64,
    pub time_cost: u128,
    pub n_msg_sent: i64,
    pub n_msg_recv: i64,
    n_active_vertices: i64,
    context: &'a Context<V, E, M>,
    vertices: Mutex<HashMap<i64, Vertex<'a, V, E, M>>>,
    sender: Sender<ChannelMessage<M>>,
    aggregated_values: RefCell<HashMap<String, Box<dyn Any>>>,
}

impl<'a, V, E, M> Worker<'a, V, E, M>
where
    M: Clone,
{
    pub fn new(id: i64, context: &'a Context<V, E, M>, sender: Sender<ChannelMessage<M>>) -> Self {
        Worker {
            id,
            context,
            sender,
            time_cost: 0,
            n_msg_sent: 0,
            n_msg_recv: 0,
            n_active_vertices: 0,
            vertices: Mutex::new(HashMap::new()),
            aggregated_values: RefCell::new(HashMap::new()),
        }
    }

    pub fn local_n_vertices(&self) -> i64 {
        match self.vertices.lock() {
            Ok(vertices) => vertices.len() as i64,
            Err(_) => 0,
        }
    }

    pub fn local_n_edges(&self) -> i64 {
        match self.vertices.lock() {
            Ok(vertices) => {
                let mut sum = 0_i64;
                for v in vertices.values() {
                    sum += v.get_outer_edges().len() as i64;
                }
                drop(vertices);
                sum
            }
            Err(_) => 0,
        }
    }

    pub fn receive_message(&mut self, message: Message<M>) {
        match self.vertices.lock() {
            Ok(mut vertices) => match vertices.get_mut(&message.receiver) {
                Some(vertex) => {
                    let value = match (
                        self.context.combiner.as_ref(),
                        vertex.read_next_step_message(),
                    ) {
                        (Some(combiner), Some(init)) => combiner.combine(init, message.value),
                        _ => message.value,
                    };
                    vertex.receive_message(value);
                    self.n_msg_recv += 1;
                }
                None => (),
            },
            Err(_) => (),
        }
    }

    pub fn add_vertex(&mut self, id: i64) {
        match self.vertices.lock() {
            Ok(mut vertices) => {
                vertices.entry(id).or_insert(Vertex::new(id, self.context));
            }
            Err(_) => (),
        }
    }

    pub fn run(&mut self) {
        let now = Instant::now();
        match self.context.state {
            State::INITIALIZED => self.load(),
            State::LOADED => self.clean(),
            State::CLEANED => self.compute(),
            State::COMPUTED => self.clean(),
        }
        self.time_cost = now.elapsed().as_millis();
    }

    pub fn report(&self, name: &String) -> Option<Box<dyn Any>> {
        self.aggregated_values.borrow_mut().remove(name)
    }

    fn clean(&mut self) {
        self.aggregated_values.borrow_mut().clear();
        self.n_msg_recv = 0;
        self.n_msg_sent = 0;
    }

    fn load_edges(&mut self) {}

    fn load_vertices(&mut self) {}

    fn load(&mut self) {
        match self.context.edge_parser.as_ref() {
            Some(_) => self.load_edges(),
            None => (),
        }

        match self.context.vertex_parser.as_ref() {
            Some(_) => self.load_vertices(),
            None => (),
        }
    }

    fn aggregate(&self, name: &String, vertex: &Vertex<V, E, M>) {
        match self.context.aggregators.get(name) {
            Some(aggregator) => {
                let new_val = aggregator.report(vertex);
                let mut values_mut = self.aggregated_values.borrow_mut();
                let (name, value) = match values_mut.remove_entry(name) {
                    Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                    None => (name.clone(), new_val),
                };
                values_mut.insert(name, value);
            }
            None => (),
        };
    }

    fn send_messages_of(&self, vertex: &Vertex<V, E, M>) {
        let send_queue = vertex.send_queue.borrow_mut();
        // let receiver = message.receiver;
        // let mut queues = self.send_queues.borrow_mut();
        // let queue_op = match queues.get_mut(&receiver) {
        //     Some(queue) => Some(queue),
        //     None => {
        //         queues.insert(receiver, LinkedList::new());
        //         queues.get_mut(&receiver)
        //     }
        // };

        // let queue = queue_op.unwrap();
        // let initial_op = queue.pop_front();
        // message.value = match (self.combiner, &initial_op) {
        //     (Some(combine), Some(initial)) => combine.combine(&message.value, &initial.value),
        //     _ => message.value,
        // };
        // queue.push_back(message);
        // if queue.len() > SEND_THRESHOLD as usize {
        //     self.send_messages(queue);
        // }
    }

    fn compute(&mut self) {
        match self.vertices.lock() {
            Ok(mut vertices) => {
                self.n_active_vertices = vertices.len() as i64;
                for vertex in vertices.values_mut() {
                    (self.context.compute)(vertex);

                    for name in self.context.aggregators.keys() {
                        self.aggregate(name, vertex);
                    }

                    self.send_messages_of(vertex);
                    self.n_active_vertices -= if vertex.active() { 0 } else { 1 };
                }
            }
            Err(_) => (),
        }
    }
}
