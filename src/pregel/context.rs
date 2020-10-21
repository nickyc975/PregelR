use super::state::State;
use super::message::Message;

pub trait Context<V, E, M> {
    fn state(&self) -> State;

    fn superstep(&self) -> i64;

    fn getNumEdges(&self) -> i64;

    fn getNumVertices(&self) -> i64;

    fn addVertex(&mut self, id: i64);

    fn markAsDone(&mut self, id: i64);

    fn sendMessage(&mut self, message: Message<M>);
}
