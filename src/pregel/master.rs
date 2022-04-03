use super::aggregate::Aggregate;
use super::combine::Combine;
use super::context::Context;
use super::message::{ChannelMessage, Message};
use super::state::State;
use super::vertex::Vertex;
use super::worker::Worker;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::mpsc;

pub struct Master<'a, V, E, M>
where
    M: Clone,
{
    nworkers: i64,
    n_active_workers: i64,
    workers: HashMap<i64, Worker<'a, V, E, M>>,
    context: Context<V, E, M>,
    receiver: mpsc::Receiver<ChannelMessage<M>>,
}
