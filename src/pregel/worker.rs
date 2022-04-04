use super::context::Context;
use super::message::{ChannelMessage, Message};
use super::state::State;
use super::vertex::Vertex;

use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
use std::time::Instant;

pub struct Worker<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub id: i64,
    pub time_cost: RefCell<u128>,
    pub n_msg_sent: RefCell<i64>,
    pub n_msg_recv: RefCell<i64>,
    pub n_active_vertices: RefCell<i64>,
    pub edges_path: Option<PathBuf>,
    pub vertices_path: Option<PathBuf>,
    context: Arc<RwLock<Context<V, E, M>>>,
    vertices: Mutex<HashMap<i64, Vertex<V, E, M>>>,
    sender: Sender<ChannelMessage<M>>,
    aggregated_values: RefCell<HashMap<String, Box<dyn Send + Sync>>>,
    send_queues: RefCell<HashMap<i64, LinkedList<Message<M>>>>,
}

impl<V, E, M> Worker<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(
        id: i64,
        context: Arc<RwLock<Context<V, E, M>>>,
        sender: Sender<ChannelMessage<M>>,
    ) -> Self {
        Worker {
            id,
            context,
            sender,
            edges_path: None,
            vertices_path: None,
            time_cost: RefCell::new(0),
            n_msg_sent: RefCell::new(0),
            n_msg_recv: RefCell::new(0),
            n_active_vertices: RefCell::new(0),
            vertices: Mutex::new(HashMap::new()),
            aggregated_values: RefCell::new(HashMap::new()),
            send_queues: RefCell::new(HashMap::new()),
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
        if let Ok(mut vertices) = self.vertices.lock() {
            if let Some(vertex) = vertices.get_mut(&message.receiver) {
                let value = match (
                    self.context.read().unwrap().combiner.as_ref(),
                    vertex.read_next_step_message(),
                ) {
                    (Some(combiner), Some(init)) => combiner.combine(init, message.value),
                    _ => message.value,
                };
                vertex.receive_message(value);
                *self.n_msg_recv.borrow_mut() += 1;
            }
        }
    }

    pub fn add_vertex(&mut self, id: i64) {
        match self.vertices.lock() {
            Ok(mut vertices) => {
                vertices
                    .entry(id)
                    .or_insert(Vertex::new(id, Arc::clone(&self.context)));
            }
            Err(_) => (),
        }
    }

    pub fn run(&self) {
        let now = Instant::now();
        match self.context.read().unwrap().state {
            State::INITIALIZED => self.load(),
            State::LOADED => self.clean(),
            State::CLEANED => self.compute(),
            State::COMPUTED => self.communicate(),
            State::COMMUNICATED => self.clean(),
        }
        *self.time_cost.borrow_mut() = now.elapsed().as_millis();
        self.sender.send(ChannelMessage::Hlt).unwrap();
    }

    pub fn report(&self, name: &String) -> Option<Box<dyn Send + Sync>> {
        self.aggregated_values.borrow_mut().remove(name)
    }

    fn clean(&self) {
        self.aggregated_values.borrow_mut().clear();
        *self.n_msg_recv.borrow_mut() = 0;
        *self.n_msg_sent.borrow_mut() = 0;
    }

    fn load_edges(&self) {
        match (
            self.edges_path.as_ref(),
            self.context.read().unwrap().edge_parser.as_ref(),
        ) {
            (Some(path), Some(parser)) => {
                let file = match File::open(path) {
                    Ok(file) => file,
                    Err(err) => panic!("Failed to open edges file: {}", err),
                };

                for line in io::BufReader::new(file).lines() {
                    if let Ok(line) = line {
                        let (source, target, edge) = parser(&line);
                        match self.vertices.lock() {
                            Ok(mut vertices) => {
                                let vertex = vertices
                                    .entry(source)
                                    .or_insert(Vertex::new(source, Arc::clone(&self.context)));

                                if !vertex.has_outer_edge_to(target) {
                                    vertex.add_outer_edge((source, target, edge));
                                } else {
                                    eprintln!(
                                        "Warning: duplicate edge from {} to %{}!",
                                        source, target
                                    );
                                }

                                match self.sender.send(ChannelMessage::Vtx(target)) {
                                    _ => (),
                                }
                            }
                            Err(_) => (),
                        }
                    }
                }
            }
            _ => (),
        }
    }

    fn load_vertices(&self) {
        match (
            self.vertices_path.as_ref(),
            self.context.read().unwrap().vertex_parser.as_ref(),
        ) {
            (Some(path), Some(parser)) => {
                let file = match File::open(path) {
                    Ok(file) => file,
                    Err(err) => panic!("Failed to open vertices file: {}", err),
                };

                for line in io::BufReader::new(file).lines() {
                    if let Ok(line) = line {
                        let (id, value) = parser(&line);
                        match self.vertices.lock() {
                            Ok(mut vertices) => {
                                let vertex = vertices
                                    .entry(id)
                                    .or_insert(Vertex::new(id, Arc::clone(&self.context)));
                                vertex.value = Some(value);
                            }
                            Err(_) => (),
                        }
                    }
                }
            }
            _ => (),
        }
    }

    fn load(&self) {
        self.load_edges();
        self.load_vertices();
    }

    fn aggregate(&self, name: &String, vertex: &Vertex<V, E, M>) {
        match self.context.read().unwrap().aggregators.get(name) {
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
        let mut send_queue = vertex.send_queue.borrow_mut();
        while let Some(mut message) = send_queue.pop_front() {
            let receiver = message.receiver;
            let mut queues = self.send_queues.borrow_mut();
            let queue = queues.entry(receiver).or_insert(LinkedList::new());

            message.value = match (
                self.context.read().unwrap().combiner.as_ref(),
                queue.pop_front(),
            ) {
                (Some(combine), Some(initial)) => combine.combine(message.value, initial.value),
                _ => message.value,
            };

            queue.push_back(message);
        }
    }

    fn compute(&self) {
        match self.vertices.lock() {
            Ok(mut vertices) => {
                *self.n_active_vertices.borrow_mut() = vertices.len() as i64;
                for vertex in vertices.values_mut() {
                    (self.context.read().unwrap().compute)(vertex);

                    for name in self.context.read().unwrap().aggregators.keys() {
                        self.aggregate(name, vertex);
                    }

                    self.send_messages_of(vertex);
                    *self.n_active_vertices.borrow_mut() -= if vertex.active() { 0 } else { 1 };
                }
            }
            Err(_) => (),
        }
    }

    fn communicate(&self) {
        let mut send_queues = self.send_queues.borrow_mut();
        for (_, mut send_queue) in send_queues.drain() {
            while let Some(message) = send_queue.pop_front() {
                match self.sender.send(ChannelMessage::Msg(message)) {
                    _ => (),
                }
            }
        }
    }
}
