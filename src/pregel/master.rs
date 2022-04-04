use super::aggregate::Aggregate;
use super::combine::Combine;
use super::context::Context;
use super::message::{ChannelMessage, Message};
use super::state::State;
use super::vertex::Vertex;
use super::worker::Worker;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::{mpsc, Arc, RwLock};
use std::thread::spawn;

pub struct Master<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
    nworkers: i64,
    n_active_workers: i64,
    workers: HashMap<i64, Worker<V, E, M>>,
    context: Arc<RwLock<Context<V, E, M>>>,
    sender: mpsc::Sender<ChannelMessage<M>>,
    receiver: mpsc::Receiver<ChannelMessage<M>>,
}

impl<V, E, M> Master<V, E, M>
where
    V: 'static + Send + Sync,
    E: 'static + Send + Sync,
    M: 'static + Send + Sync + Clone,
{
    pub fn new(
        nworkers: i64,
        compute: Box<dyn Fn(&mut Vertex<V, E, M>)>,
        work_path: String,
    ) -> Self {
        let path = Path::new(&work_path);
        let (sender, receiver) = mpsc::channel();

        if path.exists() {
            panic!("Work path {} exists!", work_path);
        } else {
            match fs::create_dir_all(path) {
                Ok(_) => (),
                Err(err) => panic!("Failed to create the work path: {}", err),
            }
        }

        Master {
            nworkers,
            n_active_workers: 0,
            workers: HashMap::new(),
            context: Arc::new(RwLock::new(Context::new(compute, work_path))),
            sender,
            receiver,
        }
    }

    pub fn set_edge_parser(
        &mut self,
        edge_parser: Box<dyn Fn(&String) -> (i64, i64, E)>,
    ) -> &mut Self {
        self.context.write().unwrap().edge_parser = Some(edge_parser);
        self
    }

    pub fn set_vertex_parser(
        &mut self,
        vertex_parser: Box<dyn Fn(&String) -> (i64, V)>,
    ) -> &mut Self {
        self.context.write().unwrap().vertex_parser = Some(vertex_parser);
        self
    }

    pub fn set_combiner(&mut self, combiner: Box<dyn Combine<M>>) -> &mut Self {
        self.context.write().unwrap().combiner = Some(combiner);
        self
    }

    pub fn add_aggregator(
        &mut self,
        name: String,
        aggregator: Box<dyn Aggregate<V, E, M>>,
    ) -> &mut Self {
        self.context
            .write()
            .unwrap()
            .aggregators
            .insert(name, aggregator);
        self
    }

    fn cal_index_for_edge(&self, line: &String) -> usize {
        ((self.context.read().unwrap().edge_parser.as_ref().unwrap())(line).0 % self.nworkers)
            as usize
    }

    fn cal_index_for_vertex(&self, line: &String) -> usize {
        ((self.context.read().unwrap().vertex_parser.as_ref().unwrap())(line).0 % self.nworkers)
            as usize
    }

    fn partition(
        &self,
        input_dir: &Path,
        output_dir: &Path,
        partition_edges: bool,
    ) -> io::Result<()> {
        let reader = io::BufReader::new(File::open(input_dir)?);
        let mut writers: Vec<File> = (0..self.nworkers)
            .map(|i| File::open(Path::new(output_dir).join(i.to_string())).unwrap())
            .collect();

        for line in reader.lines() {
            if let Ok(line) = line {
                let index = if partition_edges {
                    self.cal_index_for_edge(&line)
                } else {
                    self.cal_index_for_vertex(&line)
                };
                writers[index].write_all(line.as_bytes()).unwrap();
                writers[index].write_all("\n".as_bytes()).unwrap();
            }
        }

        Ok(())
    }

    pub fn load_edges(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        let edges_path = Path::new(&context.work_path).join("graph").join("parts");

        match (
            context.edge_parser.as_ref(),
            fs::create_dir_all(&edges_path),
        ) {
            (Some(_), Ok(())) => {
                let _ = self.partition(path, &edges_path, true);
            }
            _ => (),
        }
    }

    pub fn load_vertices(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        let vertices_path = Path::new(&context.work_path).join("vertices").join("parts");

        match (
            context.vertex_parser.as_ref(),
            fs::create_dir_all(&vertices_path),
        ) {
            (Some(_), Ok(())) => {
                let _ = self.partition(path, &vertices_path, false);
            }
            _ => (),
        }
    }

    fn aggregate(&self) {
        let context = self.context.write().unwrap();
        let mut values_mut = context.aggregated_values.borrow_mut();
        values_mut.clear();

        for (name, aggregator) in context.aggregators.iter() {
            for worker in self.workers.values() {
                if let Some(new_val) = worker.report(&name) {
                    let (name, value) = match values_mut.remove_entry(name) {
                        Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                        None => (name.clone(), new_val),
                    };
                    values_mut.insert(name, value);
                }
            }
        }
    }

    fn update_state(&mut self) {
        let mut context = self.context.write().unwrap();
        match context.state {
            State::INITIALIZED => context.state = State::LOADED,
            State::LOADED => context.state = State::CLEANED,
            State::CLEANED => context.state = State::COMPUTED,
            State::COMPUTED => {
                context.state = State::COMMUNICATED;
                context.superstep += 1;
                self.aggregate();
            }
            State::COMMUNICATED => context.state = State::CLEANED,
        }
    }

    pub fn run(&mut self) {
        for i in 0..self.nworkers {
            let worker = Worker::new(
                i as i64,
                None,
                None,
                Arc::clone(&self.context),
                self.sender.clone(),
            );
            self.workers.insert(i as i64, worker);
        }

        self.n_active_workers = self.workers.len() as i64;
        while self.n_active_workers > 0 {
            for (_, worker) in self.workers.drain() {
                spawn(move || {
                    worker.run();
                    worker
                });
            }
            self.update_state();
        }
    }
}
