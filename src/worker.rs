use crate::AggVal;
use crate::Message;
use crate::Vertex;
use crate::{Channel, ChannelMessage};
use crate::{Context, Operation};

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::RwLockReadGuard;
use std::time::Instant;

pub struct Worker<V, E, M> {
    pub id: i64,
    pub time_cost: u128,
    pub n_msg_sent: i64,
    pub n_msg_recv: i64,
    pub n_active_vertices: i64,
    pub edges_path: Option<PathBuf>,
    pub vertices_path: Option<PathBuf>,
    pub vertices: HashMap<i64, Vertex<V, E, M>>,

    channel: Channel<M>,
    send_queues: HashMap<i64, Vec<Message<M>>>,
    aggregated_values: RefCell<HashMap<String, Box<AggVal>>>,
}

impl<V, E, M> Worker<V, E, M> {
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
            aggregated_values: RefCell::new(HashMap::new()),
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

        for line in io::BufReader::new(file).lines().flatten() {
            if let Some((source, target, edge)) = parser(&line) {
                let vertex = self
                    .vertices
                    .entry(source)
                    .or_insert_with(|| Vertex::new(source));

                if !vertex.has_outer_edge_to(target) {
                    vertex.add_outer_edge((source, target, edge));
                } else {
                    eprintln!("Warning: duplicate edge from {} to %{}!", source, target);
                }

                self.channel.send(ChannelMessage::Vertex(target));
            }
        }

        // Flush vertex messages.
        self.channel.flush();
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

        for line in io::BufReader::new(file).lines().flatten() {
            if let Some((id, value)) = parser(&line) {
                let vertex = self.vertices.entry(id).or_insert_with(|| Vertex::new(id));
                vertex.value = Some(value);
            }
        }
    }

    fn load(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        self.load_edges(context);
        self.load_vertices(context);
        self.n_active_vertices = self.vertices.len() as i64;
    }

    fn compute(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let mut removed = Vec::new();
        let combiner_op = context.combiner.as_ref();
        let mut aggregated_values = self.aggregated_values.borrow_mut();

        aggregated_values.clear();
        self.n_active_vertices = self.vertices.len() as i64;
        for vertex in self.vertices.values_mut() {
            // Initiate vertex activation status.
            vertex.activate();

            // Do computation.
            (context.compute)(vertex, context);

            // Aggregate values from the vertex.
            for (name, aggregator) in &context.aggregators {
                let new_val = aggregator.report(vertex);
                let (name, value) = match aggregated_values.remove_entry(name) {
                    Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                    None => (name.clone(), new_val),
                };
                aggregated_values.insert(name, value);
            }

            // Collect vertex's pending messages for later sending.
            for mut message in vertex.send_queue.borrow_mut().drain(..) {
                let queue = self
                    .send_queues
                    .entry(message.receiver)
                    .or_insert_with(Vec::new);

                message.value = match (combiner_op, queue.pop()) {
                    (Some(combiner), Some(initial)) => {
                        combiner.combine(message.value, initial.value)
                    }
                    _ => message.value,
                };

                queue.push(message);
            }

            if vertex.removed() {
                // The vertex is removed, add it to the removed list and decrease the
                // number of active vertices.
                removed.push(vertex.id());
                self.n_active_vertices -= 1;
            } else if !vertex.active() {
                // The vertex is inactive, decrease the number of active vertices.
                self.n_active_vertices -= 1;
            }
        }

        // Remove vertices.
        for id in &removed {
            self.vertices.remove(id);
        }

        // Send messages to other workers.
        self.n_msg_sent = 0;
        for send_queue in self.send_queues.values_mut() {
            self.n_msg_sent += send_queue.len() as i64;
            for message in send_queue.drain(..) {
                self.channel.send(ChannelMessage::Message(message));
            }
        }

        // Flush messages.
        self.channel.flush();
    }

    fn process_messages(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let combiner_op = context.combiner.as_ref();

        self.n_msg_recv = 0;
        for content in &self.channel {
            match content {
                ChannelMessage::Message(message) => {
                    let receiver_id = message.receiver;
                    let vertex = self
                        .vertices
                        .entry(receiver_id)
                        .or_insert_with(|| Vertex::new(receiver_id));

                    let mut recv_queue = vertex.recv_queue.borrow_mut();
                    let value = match (combiner_op, recv_queue.pop()) {
                        (Some(combiner), Some(init)) => combiner.combine(init, message.value),
                        _ => message.value,
                    };

                    recv_queue.push(value);
                    self.n_msg_recv += 1;
                }
                ChannelMessage::Vertex(id) => {
                    self.vertices.entry(id).or_insert_with(|| Vertex::new(id));
                }
            }
        }
    }

    pub fn run(&mut self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let now = Instant::now();

        match context.operation() {
            Operation::Load => self.load(context),
            Operation::Compute => self.compute(context),
        }
        self.process_messages(context);

        self.time_cost = now.elapsed().as_millis();
    }

    pub fn report(&self, name: &String) -> Option<Box<AggVal>> {
        self.aggregated_values.borrow_mut().remove(name)
    }
}
