use pregel::Combine;
use pregel::Context;
use pregel::Master;
use pregel::Vertex;

use rand::prelude::*;
use std::path::Path;
use std::sync::RwLockReadGuard;

struct VirusCombiner;

impl Combine<bool> for VirusCombiner {
    fn combine(&self, _: bool, _: bool) -> bool {
        true
    }
}

fn compute(vertex: &mut Vertex<(), (), bool>, _: &RwLockReadGuard<Context<(), (), bool>>) {
    if vertex.id() == 0 || vertex.has_messages() {
        vertex.remove();
    } else {
        let rand_num: f64 = rand::thread_rng().gen();
        if rand_num < 0.01_f64 {
            vertex.remove();
        }
    }

    if vertex.removed() {
        vertex.send_message(true);
    }
}

fn edge_parser(s: &String) -> Option<(i64, i64, ())> {
    let parts: Vec<_> = s.split('\t').collect();
    if parts.len() != 2 {
        return None;
    }

    match (parts[0].parse(), parts[1].parse()) {
        (Ok(s), Ok(t)) => Some((s, t, ())),
        _ => None,
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

pub fn virus(work_path: &str, edges_path: &str) {
    let mut master = Master::new(8, Box::new(compute), Path::new(work_path));

    master
        .set_edge_parser(Box::new(edge_parser))
        .set_vertex_parser(Box::new(vertex_parser))
        .set_combiner(Box::new(VirusCombiner));

    master.load_edges(Path::new(edges_path));
    master.run();
}

fn main() {
    virus("data/virus", "data/web-Google.txt");
}
