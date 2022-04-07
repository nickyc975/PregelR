use super::message::Message;

use std::sync::mpsc::{self, RecvError, SendError};

pub enum ChannelMessage<M>
where
    M: 'static + Send + Clone,
{
    Msg(Message<M>),
    Vtx(i64),
    Hlt,
}

pub struct Channel<M>
where
    M: 'static + Send + Clone,
{
    receiver: mpsc::Receiver<ChannelMessage<M>>,
    senders: Vec<mpsc::Sender<ChannelMessage<M>>>,
}

impl<M> Channel<M>
where
    M: 'static + Send + Clone,
{
    pub fn new(
        receiver: mpsc::Receiver<ChannelMessage<M>>,
        senders: Vec<mpsc::Sender<ChannelMessage<M>>>,
    ) -> Self {
        Channel { receiver, senders }
    }

    pub fn send(&self, chl_msg: ChannelMessage<M>) -> Result<(), SendError<ChannelMessage<M>>> {
        match chl_msg {
            ChannelMessage::Msg(msg) => {
                let index = (msg.receiver % self.senders.len() as i64) as usize;
                self.senders[index].send(ChannelMessage::Msg(msg))
            }
            ChannelMessage::Vtx(id) => {
                let index = (id % self.senders.len() as i64) as usize;
                self.senders[index].send(ChannelMessage::Vtx(id))
            }
            ChannelMessage::Hlt => {
                for sender in &self.senders {
                    match sender.send(ChannelMessage::Hlt) {
                        Err(e) => return Result::Err(e),
                        _ => (),
                    }
                }
                Ok(())
            }
        }
    }

    pub fn recv(&self) -> Result<ChannelMessage<M>, RecvError> {
        self.receiver.recv()
    }
}

impl<'a, M> IntoIterator for &'a Channel<M>
where
    M: 'static + Send + Clone,
{
    type Item = ChannelMessage<M>;
    type IntoIter = ChannelIterator<'a, M>;

    fn into_iter(self) -> Self::IntoIter {
        ChannelIterator {
            hlt_cnt: 0,
            channel: self,
        }
    }
}

pub struct ChannelIterator<'a, M>
where
    M: 'static + Send + Clone,
{
    hlt_cnt: i64,
    channel: &'a Channel<M>,
}

impl<'a, M> Iterator for ChannelIterator<'a, M>
where
    M: 'static + Send + Clone,
{
    type Item = ChannelMessage<M>;

    fn next(&mut self) -> Option<ChannelMessage<M>> {
        while let Ok(chl_msg) = self.channel.recv() {
            match chl_msg {
                ChannelMessage::Msg(msg) => {
                    return Some(ChannelMessage::Msg(msg));
                }
                ChannelMessage::Vtx(id) => {
                    return Some(ChannelMessage::Vtx(id));
                }
                ChannelMessage::Hlt => {
                    self.hlt_cnt += 1;
                    if self.hlt_cnt >= self.channel.senders.len() as i64 {
                        return None;
                    }
                }
            }
        }

        None
    }
}
