use pregel::Combine;
use pregel::Context;
use pregel::Master;
use pregel::Vertex;
use pregel::{AggVal, Aggregate};

use std::collections::LinkedList;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::RwLockReadGuard;
use std::sync::{Arc, Mutex};

struct SSSPCombiner;

impl Combine<f64> for SSSPCombiner {
    fn combine(&self, a: f64, b: f64) -> f64 {
        f64::min(a, b)
    }
}

struct SSSPAggregator;

impl Aggregate<f64, f64, f64> for SSSPAggregator {
    fn report(&self, v: &Vertex<f64, f64, f64>) -> AggVal {
        let mut val = LinkedList::new();
        val.push_back((v.id(), v.value));
        Arc::new(Mutex::new(val))
    }

    fn aggregate(&self, a: AggVal, b: AggVal) -> AggVal {
        let mut val: LinkedList<(i64, Option<f64>)> = LinkedList::new();

        match a.downcast::<Mutex<LinkedList<(i64, Option<f64>)>>>() {
            Ok(a_val) => val.append(&mut a_val.lock().unwrap()),
            _ => (),
        }

        match b.downcast::<Mutex<LinkedList<(i64, Option<f64>)>>>() {
            Ok(b_val) => val.append(&mut b_val.lock().unwrap()),
            _ => (),
        }

        Arc::new(Mutex::new(val))
    }
}

fn compute(vertex: &mut Vertex<f64, f64, f64>, context: &RwLockReadGuard<Context<f64, f64, f64>>) {
    let mut min = if vertex.id() == 0 {
        0_f64
    } else {
        f64::INFINITY
    };

    if context.superstep() == 0 {
        vertex.value = Some(min);
    } else {
        let orig = vertex.value.unwrap();

        while vertex.has_messages(context) {
            min = f64::min(min, vertex.read_message(context).unwrap());
            if min < vertex.value.unwrap() {
                vertex.value = Some(min);
            }
        }

        if min >= orig {
            vertex.deactivate();
            return;
        }
    }

    for (_, target, edge) in vertex.get_outer_edges().values() {
        vertex.send_message_to(*target, edge + min);
    }
}

fn edge_parser(s: &String) -> Option<(i64, i64, f64)> {
    let parts: Vec<_> = s.split('\t').collect();
    if parts.len() != 2 {
        return None;
    }

    match (parts[0].parse(), parts[1].parse()) {
        (Ok(s), Ok(t)) => Some((s, t, 1.0_f64)),
        _ => None,
    }
}

fn vertex_parser(s: &String) -> Option<(i64, f64)> {
    let parts: Vec<_> = s.split('\t').collect();
    if parts.len() != 1 {
        return None;
    }

    match parts[0].parse() {
        Ok(id) => Some((id, 0.0_f64)),
        _ => None,
    }
}

pub fn single_source_shortest_path(work_path: &str, edges_path: &str, output_path: &str) {
    let path_lengths_key = "path_lengths".to_string();
    let mut master = Master::new(8, Box::new(compute), Path::new(work_path));

    master
        .set_edge_parser(Box::new(edge_parser))
        .set_vertex_parser(Box::new(vertex_parser))
        .set_combiner(Box::new(SSSPCombiner))
        .add_aggregator(path_lengths_key.clone(), Box::new(SSSPAggregator));

    master.load_edges(Path::new(edges_path));
    master.run();

    let path_lengths_op = master
        .context
        .read()
        .unwrap()
        .get_aggregated_value::<Mutex<LinkedList<(i64, Option<f64>)>>>(&path_lengths_key);

    if let Some(path_lengths) = path_lengths_op {
        let mut writer = io::BufWriter::new(File::create(output_path).unwrap());
        for (id, length) in path_lengths.lock().unwrap().iter() {
            writer
                .write_all(format!("{}\t{}", id, length.unwrap()).as_bytes())
                .unwrap();
            writer.write_all("\n".as_bytes()).unwrap();
        }
        writer.flush().unwrap();
    }
}

fn main() {
    single_source_shortest_path("data/sssp", "data/web-Google.txt", "data/sssp/output.txt");
}
