use super::channel::{Channel, ChannelMessage};
use super::message::Message;
use super::state::State;
use super::AggVal;
use super::Context;
use super::Vertex;

use std::collections::{HashMap, LinkedList};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::RwLockReadGuard;
use std::time::Instant;

pub struct Worker<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub id: i64,
    pub time_cost: u128,
    pub n_msg_sent: i64,
    pub n_msg_recv: i64,
    pub n_active_vertices: i64,
    pub edges_path: Option<PathBuf>,
    pub vertices_path: Option<PathBuf>,
    pub vertices: HashMap<i64, Vertex<V, E, M>>,

    channel: Channel<M>,
    send_queues: HashMap<i64, LinkedList<Message<M>>>,
    aggregated_values: HashMap<String, AggVal>,
}

impl<V, E, M> Worker<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(id: i64, channel: Channel<M>) -> Self {
        Worker {
            id,
            time_cost: 0,
            n_msg_sent: 0,
            n_msg_recv: 0,
            n_active_vertices: 0,
            edges_path: None,
            vertices_path: None,
            vertices: HashMap::new(),

            channel,
            send_queues: HashMap::new(),
            aggregated_values: HashMap::new(),
        }
    }

    pub fn local_n_vertices(&self) -> i64 {
        self.vertices.len() as i64
    }

    pub fn local_n_edges(&self) -> i64 {
        let mut sum = 0_i64;
        for v in self.vertices.values() {
            sum += v.get_outer_edges().len() as i64;
        }
        sum
    }

    fn load_edges(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        if self.edges_path.is_none() || context.edge_parser.is_none() {
            return;
        }

        let path = self.edges_path.as_ref().unwrap();
        let parser = context.edge_parser.as_ref().unwrap();

        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!(
                "Failed to open edges file at {}: {}",
                path.to_string_lossy(),
                err
            ),
        };

        for line in io::BufReader::new(file).lines() {
            if let Ok(line) = line {
                if let Some((source, target, edge)) = parser(&line) {
                    let vertex = self.vertices.entry(source).or_insert(Vertex::new(source));

                    if !vertex.has_outer_edge_to(target) {
                        vertex.add_outer_edge((source, target, edge));
                    } else {
                        eprintln!("Warning: duplicate edge from {} to %{}!", source, target);
                    }

                    self.channel.send(ChannelMessage::Vtx(target)).unwrap();
                }
            }
        }
    }

    fn load_vertices(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        if self.vertices_path.is_none() || context.vertex_parser.is_none() {
            return;
        }

        let path = self.vertices_path.as_ref().unwrap();
        let parser = context.vertex_parser.as_ref().unwrap();

        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!(
                "Failed to open vertices file at {}: {}",
                path.to_string_lossy(),
                err
            ),
        };

        for line in io::BufReader::new(file).lines() {
            if let Ok(line) = line {
                if let Some((id, value)) = parser(&line) {
                    let vertex = self.vertices.entry(id).or_insert(Vertex::new(id));
                    vertex.value = Some(value);
                }
            }
        }
    }

    fn load(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        self.load_edges(context);
        self.load_vertices(context);
        self.n_active_vertices = self.vertices.len() as i64;
    }

    fn clean(&mut self) {
        self.time_cost = 0;
        self.n_msg_recv = 0;
        self.n_msg_sent = 0;
        self.aggregated_values.clear();
    }

    fn send_messages(&mut self) {
        for (_, mut send_queue) in self.send_queues.drain() {
            while let Some(message) = send_queue.pop_front() {
                match self.channel.send(ChannelMessage::Msg(message)) {
                    Ok(_) => self.n_msg_sent += 1,
                    Err(e) => eprintln!("Message sent failed: {}", e),
                }
            }
        }
    }

    fn compute(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let combiner_op = context.combiner.as_ref();
        self.n_active_vertices = self.vertices.len() as i64;

        for vertex in self.vertices.values_mut() {
            // Initiate vertex activation status.
            vertex.activate();

            (context.compute)(vertex, context);

            for (name, aggregator) in &context.aggregators {
                let new_val = aggregator.report(vertex);
                let (name, value) = match self.aggregated_values.remove_entry(name) {
                    Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                    None => (name.clone(), new_val),
                };
                self.aggregated_values.insert(name, value);
            }

            // Collect vertex's pending messages for later sending.
            let mut send_queue = vertex.send_queue.borrow_mut();
            while let Some(mut message) = send_queue.pop_front() {
                let receiver = message.receiver;
                let queue = self
                    .send_queues
                    .entry(receiver)
                    .or_insert(LinkedList::new());

                message.value = match (combiner_op, queue.pop_front()) {
                    (Some(combiner), Some(initial)) => {
                        combiner.combine(message.value, initial.value)
                    }
                    _ => message.value,
                };

                queue.push_back(message);
            }

            self.n_active_vertices -= if vertex.active() { 0 } else { 1 };
        }

        self.send_messages();
    }

    fn process_messages(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let combiner_op = context.combiner.as_ref();

        for message in &self.channel {
            match message {
                ChannelMessage::Msg(msg) => {
                    if let Some(vertex) = self.vertices.get_mut(&msg.receiver) {
                        let mut recv_queue = vertex.recv_queue.borrow_mut();
                        let value = match (combiner_op, recv_queue.pop_front()) {
                            (Some(combiner), Some(init)) => combiner.combine(init, msg.value),
                            _ => msg.value,
                        };
                        recv_queue.push_back(value);
                        self.n_msg_recv += 1;
                    }
                }
                ChannelMessage::Vtx(id) => {
                    self.vertices.entry(id).or_insert(Vertex::new(id));
                }
                ChannelMessage::Hlt => unreachable!(),
            }
        }
    }

    pub fn run(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let now = Instant::now();

        match context.state {
            State::INITIALIZED => self.load(context),
            State::LOADED => self.clean(),
            State::CLEANED => self.compute(context),
            State::COMPUTED => self.clean(),
        }

        match self.channel.send(ChannelMessage::Hlt) {
            Ok(_) => self.process_messages(context),
            Err(e) => panic!("Send message failed: {}", e),
        }

        self.time_cost = now.elapsed().as_millis();
    }

    pub fn report(&self, name: &String) -> Option<AggVal> {
        match self.aggregated_values.get(name) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }
}
