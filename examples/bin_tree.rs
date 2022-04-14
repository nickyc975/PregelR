use pregel::Context;
use pregel::Master;
use pregel::Vertex;
use pregel::{AggVal, Aggregate};

use std::collections::LinkedList;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::RwLockReadGuard;
use std::sync::Mutex;

struct BinTreeAggregator;

impl Aggregate<(), (), ()> for BinTreeAggregator {
    fn report(&self, v: &Vertex<(), (), ()>) -> Box<AggVal> {
        let mut val = LinkedList::new();
        for e in v.get_outer_edges() {
            val.push_back((e.0, e.1));
        }
        Box::new(Mutex::new(val))
    }

    fn aggregate(&self, a: Box<AggVal>, b: Box<AggVal>) -> Box<AggVal> {
        let mut val: LinkedList<(i64, i64)> = LinkedList::new();

        match a.downcast::<Mutex<LinkedList<(i64, i64)>>>() {
            Ok(a_val) => val.append(&mut a_val.lock().unwrap()),
            _ => (),
        }

        match b.downcast::<Mutex<LinkedList<(i64, i64)>>>() {
            Ok(b_val) => val.append(&mut b_val.lock().unwrap()),
            _ => (),
        }

        Box::new(Mutex::new(val))
    }
}

fn compute(vertex: &mut Vertex<(), (), ()>, context: &RwLockReadGuard<Context<(), (), ()>>) {
    if context.superstep() <= 20 && vertex.get_outer_edges().len() <= 0 {
        let id = vertex.id() * 2;
        if vertex.id() != id {
            vertex.send_message_to(id, ());
            vertex.add_outer_edge((vertex.id(), id, ()));
        }

        vertex.send_message_to(id + 1, ());
        vertex.add_outer_edge((vertex.id(), id + 1, ()));
    } else {
        vertex.deactivate();
    }
}

fn vertex_parser(s: &String) -> Option<(i64, ())> {
    let parts: Vec<_> = s.split('\t').collect();
    if parts.len() != 1 {
        return None;
    }

    match parts[0].parse() {
        Ok(id) => Some((id, ())),
        _ => None,
    }
}

pub fn bin_tree(work_path: &str, vertices_path: &str, output_path: &str) {
    let edges_key = "edges".to_string();
    let mut master = Master::new(8, 128, Box::new(compute), Path::new(work_path));

    master
        .set_vertex_parser(Box::new(vertex_parser))
        .add_aggregator(edges_key.clone(), Box::new(BinTreeAggregator));
    master.load_vertices(Path::new(vertices_path));
    master.run();

    let edges_op = master
        .context
        .read()
        .unwrap()
        .get_aggregated_value::<Mutex<LinkedList<(i64, i64)>>>(&edges_key);

    if let Some(edges) = edges_op {
        let mut writer = io::BufWriter::new(File::create(output_path).unwrap());
        for (s, t) in edges.lock().unwrap().iter() {
            writer
                .write_all(format!("{}\t{}", s, t).as_bytes())
                .unwrap();
            writer.write_all("\n".as_bytes()).unwrap();
        }
        writer.flush().unwrap();
    }
}

fn main() {
    bin_tree(
        "data/bin_tree",
        "data/bin_tree.txt",
        "data/bin_tree/edges.txt",
    );
}
