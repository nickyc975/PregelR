use crate::pregel::aggregate::{AggVal, Aggregate};
use crate::pregel::combine::Combine;
use crate::pregel::context::Context;
use crate::pregel::master::Master;
use crate::pregel::vertex::Vertex;

use std::collections::LinkedList;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::RwLockReadGuard;
use std::sync::{Arc, Mutex};

struct PageRankCombiner;

impl Combine<f64> for PageRankCombiner {
    fn combine(&self, a: f64, b: f64) -> f64 {
        a + b
    }
}

struct PageRankMaxVertexAggregator;

impl Aggregate<f64, (), f64> for PageRankMaxVertexAggregator {
    fn report(&self, v: &Vertex<f64, (), f64>) -> AggVal {
        Arc::new((v.id(), v.value))
    }

    fn aggregate(&self, a: AggVal, b: AggVal) -> AggVal {
        match (
            a.downcast::<(i64, Option<f64>)>(),
            b.downcast::<(i64, Option<f64>)>(),
        ) {
            (Ok(a_val), Ok(b_val)) => match (a_val.1, b_val.1) {
                (Some(a_weight), Some(b_weight)) => {
                    if a_weight > b_weight {
                        a_val
                    } else {
                        b_val
                    }
                }
                _ => Arc::new((-1, Some(f64::NEG_INFINITY))),
            },
            _ => Arc::new((-1, Some(f64::NEG_INFINITY))),
        }
    }
}

struct PageRankVertexWeightAggregator;

impl Aggregate<f64, (), f64> for PageRankVertexWeightAggregator {
    fn report(&self, v: &Vertex<f64, (), f64>) -> AggVal {
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

fn compute(vertex: &mut Vertex<f64, (), f64>, context: &RwLockReadGuard<Context<f64, (), f64>>) {
    if vertex.value.is_none() {
        vertex.value = Some(0_f64);
    }

    if vertex.has_messages(context) {
        let mut sum = 0_f64;
        while let Some(msg) = vertex.read_message(context) {
            sum += msg;
        }
        let value = 0.15_f64 / context.num_vertices() as f64 + 0.85_f64 * sum;
        vertex.value = Some(value);
    }

    let n = vertex.get_outer_edges().len();
    vertex.send_message(vertex.value.unwrap() / n as f64);

    if context.superstep() > 30 {
        vertex.deactivate();
    }
}

fn edge_parser(s: &String) -> (i64, i64, ()) {
    let parts: Vec<_> = s.split('\t').collect();
    (parts[0].parse().unwrap(), parts[1].parse().unwrap(), ())
}

fn vertex_parser(s: &String) -> (i64, f64) {
    let parts: Vec<_> = s.split('\t').collect();
    (parts[0].parse().unwrap(), 0.0_f64)
}

pub fn page_rank(work_path: &str, edges_path: &str, output_path: &str) {
    let max_vertex_key = "max_vertex".to_string();
    let vertex_weights_key = "vertex_weights".to_string();
    let mut master = Master::new(8, Box::new(compute), Path::new(work_path));

    master
        .set_edge_parser(Box::new(edge_parser))
        .set_vertex_parser(Box::new(vertex_parser))
        .set_combiner(Box::new(PageRankCombiner))
        .add_aggregator(
            max_vertex_key.clone(),
            Box::new(PageRankMaxVertexAggregator),
        )
        .add_aggregator(
            vertex_weights_key.clone(),
            Box::new(PageRankVertexWeightAggregator),
        );

    master.load_edges(Path::new(edges_path));
    master.run();

    let context = master.context.read().unwrap();
    let max_vertex_op = context.get_aggregated_value::<(i64, Option<f64>)>(&max_vertex_key);
    if let Some(max_vertex) = max_vertex_op {
        println!(
            "Max vertex: {}, weight: {}",
            max_vertex.0,
            max_vertex.1.unwrap()
        );
    }

    let vertex_weights_op =
        context.get_aggregated_value::<Mutex<LinkedList<(i64, Option<f64>)>>>(&vertex_weights_key);
    if let Some(vertex_weights) = vertex_weights_op {
        let mut writer = io::BufWriter::new(File::create(output_path).unwrap());
        for (id, length) in vertex_weights.lock().unwrap().iter() {
            writer
                .write_all(format!("{}\t{}", id, length.unwrap()).as_bytes())
                .unwrap();
            writer.write_all("\n".as_bytes()).unwrap();
        }
        writer.flush().unwrap();
    }
}
