use crate::Aggregate;
use crate::Channel;
use crate::Combine;
use crate::ComputeFn;
use crate::EdgeParserFn;
use crate::VertexParserFn;
use crate::Worker;
use crate::{Context, Operation};

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::thread::spawn;
use std::time::Instant;

pub struct Master<V, E, M>
where
    V: 'static + Send,
    E: 'static + Send,
    M: 'static + Send,
{
    nworkers: i64,
    msg_batch_size: usize,
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
    M: 'static + Send,
{
    pub fn new(
        nworkers: i64,
        msg_batch_size: usize,
        compute: Box<ComputeFn<V, E, M>>,
        work_path: &Path,
    ) -> Self {
        assert!(nworkers > 0);
        assert!(msg_batch_size > 0);

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
            msg_batch_size,
            n_active_workers: 0,
            edges_path: None,
            vertices_path: None,
            workers: HashMap::new(),
            context: Arc::new(RwLock::new(Context::new(compute, work_path.join("")))),
        }
    }

    pub fn set_edge_parser(&mut self, edge_parser: Box<EdgeParserFn<E>>) -> &mut Self {
        self.context.write().unwrap().edge_parser = Some(edge_parser);
        self
    }

    pub fn set_vertex_parser(&mut self, vertex_parser: Box<VertexParserFn<V>>) -> &mut Self {
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

        for line in reader.lines().flatten() {
            if let Some((index, line)) = cal_index(line) {
                writers[index].write_all(line.as_bytes())?;
                writers[index].write_all("\n".as_bytes())?;
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
        let mut aggregated_values = HashMap::new();

        context.num_edges = 0;
        context.num_vertices = 0;
        for worker in self.workers.values() {
            context.num_edges += worker.local_n_edges();
            context.num_vertices += worker.local_n_vertices();

            for (name, aggregator) in context.aggregators.iter() {
                if let Some(new_val) = worker.report(name) {
                    let (name, value) = match aggregated_values.remove_entry(name) {
                        Some((name, init)) => (name, aggregator.aggregate(init, new_val)),
                        None => (name.clone(), new_val),
                    };
                    aggregated_values.insert(name, value);
                }
            }
        }

        // Extend operation will overwrite existing values automatically,
        // so we don't need clear().
        context.aggregated_values.extend(
            aggregated_values
                .drain()
                .map(|(name, value)| (name, Arc::from(value))),
        );
    }

    fn create_workers(&mut self) {
        let mut channels = Channel::create(self.nworkers as usize, self.msg_batch_size);
        for i in (0..self.nworkers).rev() {
            let mut worker = Worker::new(i, channels.remove(&i).unwrap());

            worker.edges_path = self
                .edges_path
                .as_ref()
                .map(|path| path.join(format!("{}.txt", i)));

            worker.vertices_path = self
                .vertices_path
                .as_ref()
                .map(|path| path.join(format!("{}.txt", i)));

            self.workers.insert(i as i64, worker);
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
                if worker.n_active_vertices <= 0 && worker.n_msg_recv <= 0 {
                    self.n_active_workers -= 1;
                }
                self.workers.insert(worker.id, worker);
            }

            let mut context = self.context.write().unwrap();
            match context.operation() {
                Operation::Load => context.operation = Operation::Compute,
                Operation::Compute => {
                    self.aggregate(&mut context);
                    self.print_stats(&context);
                    context.superstep += 1;
                }
            }
            drop(context);
        }

        println!("Total time cost: {} ms", now.elapsed().as_millis());
    }
}
