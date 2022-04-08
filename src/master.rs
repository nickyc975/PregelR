use super::channel::Channel;
use super::state::State;
use super::worker::Worker;
use super::Aggregate;
use super::Combine;
use super::Context;
use super::Vertex;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
        edge_parser: Box<dyn Fn(&String) -> Option<(i64, i64, E)> + Send + Sync>,
    ) -> &mut Self {
        self.context.write().unwrap().edge_parser = Some(edge_parser);
        self
    }

    pub fn set_vertex_parser(
        &mut self,
        vertex_parser: Box<dyn Fn(&String) -> Option<(i64, V)> + Send + Sync>,
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

    fn partition(
        &self,
        input_dir: &Path,
        output_dir: &Path,
        // The cal_index function cannot take string reference as the parameter
        // because of lifetime problems, so we must transfer the ownership in
        // and out.
        cal_index: &dyn Fn(String) -> Option<(usize, String)>,
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
                if let Some((index, line)) = cal_index(line) {
                    writers[index].write_all(line.as_bytes())?;
                    writers[index].write_all("\n".as_bytes())?;
                }
            }
        }

        Ok(())
    }

    pub fn load_edges(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        match context.edge_parser.as_ref() {
            Some(parser) => {
                let cal_index = |s| match parser(&s) {
                    Some(edge) => Some(((edge.0 % self.nworkers) as usize, s)),
                    None => None,
                };

                let edges_path = Path::new(&context.work_path).join("graph").join("parts");

                fs::create_dir_all(&edges_path).unwrap();
                self.partition(path, &edges_path, &cal_index).unwrap();
                self.edges_path = Some(edges_path);
            }
            None => (),
        }
    }

    pub fn load_vertices(&mut self, path: &Path) {
        let context = self.context.read().unwrap();
        match context.vertex_parser.as_ref() {
            Some(parser) => {
                let cal_index = |s| match parser(&s) {
                    Some(vertex) => Some(((vertex.0 % self.nworkers) as usize, s)),
                    None => None,
                };

                let vertices_path = Path::new(&context.work_path).join("vertices").join("parts");

                fs::create_dir_all(&vertices_path).unwrap();
                self.partition(path, &vertices_path, &cal_index).unwrap();
                self.vertices_path = Some(vertices_path);
            }
            None => (),
        }
    }

    fn print_stats(&self, context: &RwLockWriteGuard<Context<V, E, M>>) {
        println!(
            "Superstep: {}, num_vertices: {}, num_edges: {}",
            context.superstep(),
            context.num_vertices(),
            context.num_edges()
        );

        for worker in self.workers.values() {
            println!(
                "    worker: {}, n_active_vertices: {}, n_vertices: {}, n_edges: {}, \
                    msg_sent: {}, msg_recv: {}, time_cost: {} ms",
                worker.id,
                worker.n_active_vertices,
                worker.local_n_vertices(),
                worker.local_n_edges(),
                worker.n_msg_sent,
                worker.n_msg_recv,
                worker.time_cost
            );
        }
    }

    fn aggregate(&self, context: &mut RwLockWriteGuard<Context<V, E, M>>) {
        let mut num_edges = 0;
        let mut num_vertices = 0;

        // We should always be able to get the write lock for context.aggregated_values,
        // as we have got the write lock for context itself.
        let mut aggregated_values = context.aggregated_values.write().unwrap();

        aggregated_values.clear();
        for worker in self.workers.values() {
            num_edges += worker.local_n_edges();
            num_vertices += worker.local_n_vertices();

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

        // Drop the lock in advance so we can borrow context as mutable again.
        drop(aggregated_values);

        context.num_edges = num_edges;
        context.num_vertices = num_vertices;
    }

    fn create_workers(&mut self) {
        let mut channels = Channel::create(self.nworkers as usize);
        for i in (0..self.nworkers).rev() {
            let mut worker = Worker::new(i, channels.remove(&i).unwrap());

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
        match context.state() {
            State::INITIALIZED => context.state = State::LOADED,
            State::LOADED => context.state = State::CLEANED,
            State::CLEANED => {
                self.aggregate(&mut context);
                self.print_stats(&context);

                context.state = State::COMPUTED;
                context.superstep += 1;
            }
            State::COMPUTED => context.state = State::CLEANED,
        }
    }

    pub fn run(&mut self) {
        self.create_workers();

        let now = Instant::now();
        let mut handles = Vec::new();

        self.n_active_workers = self.nworkers;
        while self.n_active_workers > 0 {
            self.n_active_workers = self.nworkers;

            for (_, mut worker) in self.workers.drain() {
                let context = self.context.clone();
                handles.push(spawn(move || {
                    worker.run(&context.read().unwrap());
                    worker
                }));
            }

            for handle in handles.drain(..) {
                let worker = handle.join().unwrap();
                if worker.n_active_vertices <= 0 {
                    self.n_active_workers -= 1;
                }
                self.workers.insert(worker.id, worker);
            }

            self.update_state();
        }

        println!("Total time cost: {} ms", now.elapsed().as_millis());
    }
}
