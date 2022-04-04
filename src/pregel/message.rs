pub struct Message<M>
where
    M: Send + Sync + Clone,
{
    pub value: M,
    pub sender: i64,
    pub receiver: i64,
}

impl<M> Message<M>
where
    M: Send + Sync + Clone,
{
    pub fn new(value: M, sender: i64, receiver: i64) -> Self {
        Message {
            value,
            sender,
            receiver,
        }
    }
}

unsafe impl<M> Send for Message<M> where M: Send + Sync + Clone {}

pub enum ChannelMessage<M>
where
    M: Send + Sync + Clone,
{
    Msg(Message<M>),
    Vtx(i64),
}
