use super::channel::{Channel, ChannelMessage};
use super::message::Message;
use super::state::State;
use super::AggVal;
use super::Context;
use super::Vertex;

use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard};
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
    pub vertices: RefCell<HashMap<i64, Vertex<V, E, M>>>,

    channel: Channel<M>,
    context: Arc<RwLock<Context<V, E, M>>>,
    aggregated_values: RefCell<HashMap<String, AggVal>>,
    send_queues: RefCell<HashMap<i64, LinkedList<Message<M>>>>,
}

impl<V, E, M> Worker<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(id: i64, channel: Channel<M>, context: Arc<RwLock<Context<V, E, M>>>) -> Self {
        Worker {
            id,
            channel,
            context,
            edges_path: None,
            vertices_path: None,
            time_cost: RefCell::new(0),
            n_msg_sent: RefCell::new(0),
            n_msg_recv: RefCell::new(0),
            n_active_vertices: RefCell::new(0),
            vertices: RefCell::new(HashMap::new()),
            aggregated_values: RefCell::new(HashMap::new()),
            send_queues: RefCell::new(HashMap::new()),
        }
    }

    pub fn local_n_vertices(&self) -> i64 {
        self.vertices.borrow().len() as i64
    }

    pub fn local_n_edges(&self) -> i64 {
        let mut sum = 0_i64;
        for v in self.vertices.borrow().values() {
            sum += v.get_outer_edges().len() as i64;
        }
        sum
    }

    fn load_edges(
        &self,
        path: &PathBuf,
        parser: &(dyn Fn(&String) -> (i64, i64, E) + Send + Sync),
    ) {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!(
                "Failed to open edges file at {}: {}",
                path.to_string_lossy(),
                err
            ),
        };

        let mut vertices = self.vertices.borrow_mut();
        for line in io::BufReader::new(file).lines() {
            if let Ok(line) = line {
                let (source, target, edge) = parser(&line);

                let vertex = vertices.entry(source).or_insert(Vertex::new(source));

                if !vertex.has_outer_edge_to(target) {
                    vertex.add_outer_edge((source, target, edge));
                } else {
                    eprintln!("Warning: duplicate edge from {} to %{}!", source, target);
                }

                self.channel.send(ChannelMessage::Vtx(target)).unwrap();
            }
        }
    }

    fn load_vertices(&self, path: &PathBuf, parser: &(dyn Fn(&String) -> (i64, V) + Send + Sync)) {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => panic!(
                "Failed to open vertices file at {}: {}",
                path.to_string_lossy(),
                err
            ),
        };

        let mut vertices = self.vertices.borrow_mut();
        for line in io::BufReader::new(file).lines() {
            if let Ok(line) = line {
                let (id, value) = parser(&line);
                let vertex = vertices.entry(id).or_insert(Vertex::new(id));
                vertex.value = Some(value);
            }
        }
    }

    fn load(&self, context: &RwLockReadGuard<Context<V, E, M>>) {
        match (self.edges_path.as_ref(), context.edge_parser.as_ref()) {
            (Some(path), Some(parser)) => self.load_edges(path, parser),
            _ => (),
        }

        match (self.vertices_path.as_ref(), context.vertex_parser.as_ref()) {
            (Some(path), Some(parser)) => self.load_vertices(path, parser),
            _ => (),
        }

        *self.n_active_vertices.borrow_mut() = self.vertices.borrow().len() as i64;
    }

    fn clean(&self) {
        self.aggregated_values.borrow_mut().clear();
        *self.n_msg_recv.borrow_mut() = 0;
        *self.n_msg_sent.borrow_mut() = 0;
    }

    fn collect_vertex_messages(
        &self,
        context: &RwLockReadGuard<Context<V, E, M>>,
        vertex: &Vertex<V, E, M>,
    ) {
        let combiner_op = context.combiner.as_ref();
        let mut queues = self.send_queues.borrow_mut();
        let mut send_queue = vertex.send_queue.borrow_mut();

        while let Some(mut message) = send_queue.pop_front() {
            let receiver = message.receiver;
            let queue = queues.entry(receiver).or_insert(LinkedList::new());

            message.value = match (combiner_op, queue.pop_front()) {
                (Some(combiner), Some(initial)) => combiner.combine(message.value, initial.value),
                _ => message.value,
            };

            queue.push_back(message);
        }
    }

    fn send_messages(&self) {
        let mut n_msg_sent = self.n_msg_sent.borrow_mut();
        let mut send_queues = self.send_queues.borrow_mut();

        for (_, mut send_queue) in send_queues.drain() {
            while let Some(message) = send_queue.pop_front() {
                match self.channel.send(ChannelMessage::Msg(message)) {
                    Ok(_) => *n_msg_sent += 1,
                    Err(e) => eprintln!("Message sent failed: {}", e),
                }
            }
        }
    }

    fn compute(&self, context: &RwLockReadGuard<Context<V, E, M>>) {
        let mut values_mut = self.aggregated_values.borrow_mut();
        let mut n_active_vertices = self.n_active_vertices.borrow_mut();

        *n_active_vertices = self.vertices.borrow().len() as i64;
        for vertex in self.vertices.borrow_mut().values_mut() {
            // Initiate vertex activation status.
            vertex.activate();

            (context.compute)(vertex, context);

            for (name, aggregator) in &context.aggregators {
                let new_val = aggregator.report(vertex);
                let (name, value) = match values_mut.remove_entry(name) {
                    Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                    None => (name.clone(), new_val),
                };
                values_mut.insert(name, value);
            }

            // Collect vertex's pending messages for later sending.
            self.collect_vertex_messages(context, vertex);

            *n_active_vertices -= if vertex.active() { 0 } else { 1 };
        }

        self.send_messages();
    }

    fn receive_message(&self, context: &RwLockReadGuard<Context<V, E, M>>, message: Message<M>) {
        if let Some(vertex) = self.vertices.borrow_mut().get_mut(&message.receiver) {
            let value = match (
                context.combiner.as_ref(),
                vertex.read_next_step_message(context),
            ) {
                (Some(combiner), Some(init)) => combiner.combine(init, message.value),
                _ => message.value,
            };
            vertex.receive_message(context, value);
            *self.n_msg_recv.borrow_mut() += 1;
        }
    }

    fn add_vertex(&self, id: i64) {
        self.vertices
            .borrow_mut()
            .entry(id)
            .or_insert(Vertex::new(id));
    }

    pub fn run(&self) {
        let now = Instant::now();
        let context = self.context.read().unwrap();

        match context.state {
            State::INITIALIZED => self.load(&context),
            State::LOADED => self.clean(),
            State::CLEANED => self.compute(&context),
            State::COMPUTED => self.clean(),
        }

        self.channel.send(ChannelMessage::Hlt).unwrap();

        for message in &self.channel {
            match message {
                ChannelMessage::Msg(msg) => self.receive_message(&context, msg),
                ChannelMessage::Vtx(id) => self.add_vertex(id),
                ChannelMessage::Hlt => {
                    unreachable!("Channel should never return ChannelMessage::Hlt!")
                }
            }
        }

        *self.time_cost.borrow_mut() = now.elapsed().as_millis();
    }

    pub fn report(&self, name: &String) -> Option<AggVal> {
        self.aggregated_values.borrow_mut().remove(name)
    }
}
