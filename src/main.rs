use pregel::pregel::aggregate::Aggregate;
use pregel::pregel::combine::Combine;
use pregel::pregel::context::Context;
use pregel::pregel::master::Master;
use pregel::pregel::vertex::Vertex;

use std::path::Path;

struct SSSPCombiner();

impl Combine<f64> for SSSPCombiner {
    fn combine(&self, a: f64, b: f64) -> f64 {
        f64::min(a, b)
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
        (parts[0].parse().unwrap(), parts[1].parse().unwrap(), 1.0_f64)
    });

    let vertex_parser = Box::new(|s: &String| {
        let parts: Vec<_> = s.split('\t').collect();
        (parts[0].parse().unwrap(), 0.0_f64)
    });

    let mut master = Master::new(4, compute, Path::new("data/sssp"));

    master.set_edge_parser(edge_parser);
    master.set_vertex_parser(vertex_parser);
    master.set_combiner(Box::new(SSSPCombiner()));

    master.load_edges(Path::new("data/web-Google.txt"));
    master.run();
}
