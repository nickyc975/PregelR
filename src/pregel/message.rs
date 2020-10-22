pub struct Message<M> {
    pub value: M,
    pub sender: i64,
    pub receiver: i64,
    pub superstep: i64,
}

impl<M> Message<M> {
    pub fn new(value: M, sender: i64, receiver: i64, superstep: i64) -> Self {
        Message {
            value,
            sender,
            receiver,
            superstep,
        }
    }
}
