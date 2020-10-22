use super::context::Context;
use super::vertex;
use std::sync::{Arc, Mutex};

pub struct Worker<V, E, M> {
    context: Arc<Mutex<Context<V, E, M>>>,
}
