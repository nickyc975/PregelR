use super::message::Message;
use super::state::State;

pub trait Context<V, E, M> {
    fn state(&self) -> State;

    fn superstep(&self) -> i64;

    fn num_edges(&self) -> i64;

    fn num_vertices(&self) -> i64;

    fn add_vertex(&mut self, id: i64);

    fn mark_as_done(&mut self, id: i64);

    fn send_message(&mut self, message: Message<M>);
}
