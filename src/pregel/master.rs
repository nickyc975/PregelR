use super::aggregate::Aggregate;
use super::channel::Channel;
use super::combine::Combine;
use super::context::Context;
use super::state::State;
use super::vertex::Vertex;
use super::worker::Worker;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::spawn;
use std::time::Instant;

pub struct Master<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    nworkers: i64,
    n_active_workers: i64,
    edges_path: Option<PathBuf>,
    vertices_path: Option<PathBuf>,
    workers: HashMap<i64, Worker<V, E, M>>,
    pub context: Arc<RwLock<Context<V, E, M>>>,
}

impl<V, E, M> Master<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send + Clone,
{
    pub fn new(
        nworkers: i64,
        compute: Box<
            dyn Fn(&mut Vertex<V, E, M>, &RwLockReadGuard<Context<V, E, M>>) + Send + Sync,
        >,
        work_path: &Path,
    ) -> Self {
        if work_path.exists() {
            panic!("Work path {} exists!", work_path.to_string_lossy());
        } else {
            match fs::create_dir_all(work_path) {
                Ok(_) => (),
                Err(err) => panic!("Failed to create the work path: {}", err),
            }
        }

        Master {
            nworkers,
            n_active_workers: 0,
            edges_path: None,
            vertices_path: None,
            workers: HashMap::new(),
            context: Arc::new(RwLock::new(Context::new(compute, work_path.join("")))),
        }
    }

    pub fn set_edge_parser(
        &mut self,
        edge_parser: Box<dyn Fn(&String) -> (i64, i64, E) + Send + Sync>,
    ) -> &mut Self {
        self.context.write().unwrap().edge_parser = Some(edge_parser);
        self
    }

    pub fn set_vertex_parser(
        &mut self,
        vertex_parser: Box<dyn Fn(&String) -> (i64, V) + Send + Sync>,
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
        let mut writers: Vec<io::BufWriter<_>> = (0..self.nworkers)
            .map(|i| {
                io::BufWriter::new(
                    File::create(Path::new(output_dir).join(format!("{}.txt", i))).unwrap(),
                )
            })
            .collect();

        for line in reader.lines() {
            if let Ok(line) = line {
                let index = if partition_edges {
                    self.cal_index_for_edge(&line)
                } else {
                    self.cal_index_for_vertex(&line)
                };
                writers[index].write_all(line.as_bytes())?;
                writers[index].write_all("\n".as_bytes())?;
            }
        }

        Ok(())
    }

    pub fn load_edges(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        let edges_path = Path::new(&context.work_path).join("graph").join("parts");

        match context.edge_parser.as_ref() {
            Some(_) => {
                fs::create_dir_all(&edges_path).unwrap();
                self.partition(path, &edges_path, true).unwrap();
                self.edges_path = Some(edges_path);
            }
            None => (),
        }
    }

    pub fn load_vertices(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        let vertices_path = Path::new(&context.work_path).join("vertices").join("parts");

        match context.vertex_parser.as_ref() {
            Some(_) => {
                fs::create_dir_all(&vertices_path).unwrap();
                self.partition(path, &vertices_path, false).unwrap();
                self.vertices_path = Some(vertices_path);
            }
            None => (),
        }
    }

    fn aggregate(&self, context: &RwLockWriteGuard<Context<V, E, M>>) {
        match context.aggregated_values.write() {
            Ok(mut aggregated_values) => {
                println!("Superstep: {}", context.superstep);

                aggregated_values.clear();
                for worker in self.workers.values() {
                    println!(
                        "    worker: {}, n_active_vertices: {}, n_vertices: {}, n_edges: {}, \
                            msg_sent: {}, msg_recv: {}, time_cost: {} ms",
                        worker.id,
                        worker.n_active_vertices.borrow(),
                        worker.local_n_vertices(),
                        worker.local_n_edges(),
                        worker.n_msg_sent.borrow(),
                        worker.n_msg_recv.borrow(),
                        worker.time_cost.borrow()
                    );

                    for (name, aggregator) in context.aggregators.iter() {
                        if let Some(new_val) = worker.report(&name) {
                            let (name, value) = match aggregated_values.remove_entry(name) {
                                Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                                None => (name.clone(), new_val),
                            };
                            aggregated_values.insert(name, value);
                        }
                    }
                }
            }
            Err(_) => unreachable!(),
        }
    }

    fn create_workers(&mut self) {
        let mut senderss = vec![Vec::new(); self.nworkers as usize];
        let mut receivers = Vec::new();
        for _ in 0..self.nworkers {
            let (sender, receiver) = mpsc::channel();
            for senders in &mut senderss {
                senders.push(sender.clone());
            }
            receivers.push(receiver);
        }

        // Vec::pop() removes item from the end of the vector, which means we get receiver and senders
        // at the order (self.nworkers-1)..0. Thus, we have to create workers in such order or we'll
        // assign wrong receiver and senders to workers.
        for i in (0..self.nworkers).rev() {
            let mut worker = Worker::new(
                i as i64,
                Channel::new(receivers.pop().unwrap(), senderss.pop().unwrap()),
                Arc::clone(&self.context),
            );

            worker.edges_path = match self.edges_path.as_ref() {
                Some(path) => Some(path.join(format!("{}.txt", i))),
                None => None,
            };

            worker.vertices_path = match self.vertices_path.as_ref() {
                Some(path) => Some(path.join(format!("{}.txt", i))),
                None => None,
            };

            self.workers.insert(i as i64, worker);
        }
    }

    fn update_state(&mut self) {
        let mut context = self.context.write().unwrap();
        match context.state {
            State::INITIALIZED => context.state = State::LOADED,
            State::LOADED => context.state = State::CLEANED,
            State::CLEANED => {
                context.state = State::COMPUTED;
                context.superstep += 1;

                (context.num_edges, context.num_vertices) = self
                    .workers
                    .values()
                    .map(|w| (w.local_n_edges(), w.local_n_vertices()))
                    .fold((0, 0), |(init_n_e, init_n_v), (n_e, n_v)| {
                        (init_n_e + n_e, init_n_v + n_v)
                    });

                self.aggregate(&context);
            }
            State::COMPUTED => context.state = State::CLEANED,
        }
    }

    pub fn run(&mut self) {
        self.create_workers();

        let now = Instant::now();

        self.n_active_workers = self.nworkers;
        while self.n_active_workers > 0 {
            let mut handles = Vec::new();
            self.n_active_workers = self.nworkers;

            for (_, worker) in self.workers.drain() {
                handles.push(spawn(move || {
                    worker.run();
                    worker
                }));
            }

            for handle in handles {
                match handle.join() {
                    Ok(worker) => {
                        if *worker.n_active_vertices.borrow() <= 0 {
                            self.n_active_workers -= 1;
                        }
                        self.workers.insert(worker.id, worker);
                    }
                    Err(_) => (),
                }
            }

            self.update_state();
        }

        println!("Total time cost: {} ms", now.elapsed().as_millis());
    }
}
