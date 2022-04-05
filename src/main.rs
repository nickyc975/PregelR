use pregel::pregel::aggregate::{AggVal, Aggregate};
use pregel::pregel::combine::Combine;
use pregel::pregel::master::Master;
use pregel::pregel::vertex::Vertex;

use std::collections::LinkedList;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

struct SSSPCombiner;

struct SSSPAggregator;

impl Combine<f64> for SSSPCombiner {
    fn combine(&self, a: f64, b: f64) -> f64 {
        f64::min(a, b)
    }
}

impl Aggregate<f64, f64, f64> for SSSPAggregator {
    fn report(&self, v: &Vertex<f64, f64, f64>) -> AggVal {
        let mut val = LinkedList::new();
        val.push_back((v.id, v.value));
        Box::new(val)
    }

    fn aggregate(&self, a: AggVal, b: AggVal) -> AggVal {
        match (
            a.downcast::<LinkedList<(i64, Option<f64>)>>(),
            b.downcast::<LinkedList<(i64, Option<f64>)>>(),
        ) {
            (Ok(mut a_val), Ok(mut b_val)) => {
                a_val.append(&mut b_val);
                a_val
            }
            (Ok(a_val), Err(_)) => a_val,
            (Err(_), Ok(b_val)) => b_val,
            _ => {
                let val: LinkedList<(i64, Option<f64>)> = LinkedList::new();
                Box::new(val)
            }
        }
    }
}

fn main() {
    let compute = Box::new(|vertex: &mut Vertex<f64, f64, f64>| {
        let mut min = if vertex.id == 0 { 0_f64 } else { f64::INFINITY };

        if vertex.context.read().unwrap().superstep == 0 {
            vertex.value = Some(min);
        } else {
            let orig = vertex.value.unwrap();

            while vertex.has_messages() {
                min = f64::min(min, vertex.read_message().unwrap());
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
    });

    let edge_parser = Box::new(|s: &String| {
        let parts: Vec<_> = s.split('\t').collect();
        (
            parts[0].parse().unwrap(),
            parts[1].parse().unwrap(),
            1.0_f64,
        )
    });

    let vertex_parser = Box::new(|s: &String| {
        let parts: Vec<_> = s.split('\t').collect();
        (parts[0].parse().unwrap(), 0.0_f64)
    });

    let mut master = Master::new(4, compute, Path::new("data/sssp"));

    master.set_edge_parser(edge_parser);
    master.set_vertex_parser(vertex_parser);
    master.set_combiner(Box::new(SSSPCombiner));
    master.add_aggregator("path_length".to_string(), Box::new(SSSPAggregator));

    master.load_edges(Path::new("data/web-Google.txt"));
    master.run();

    let path_lengths_op =
        master.take_aggregated_value::<LinkedList<(i64, Option<f64>)>>(&"path_length".to_string());

    if let Some(path_lengths) = path_lengths_op {
        let mut writer = io::BufWriter::new(File::create("data/sssp/output.txt").unwrap());
        for (id, length) in path_lengths {
            writer
                .write_all(format!("{}\t{}", id, length.unwrap()).as_bytes())
                .unwrap();
            writer.write_all("\n".as_bytes()).unwrap();
        }
        writer.flush().unwrap();
    }
}
