use super::message::Message;

use std::collections::HashMap;
use std::sync::mpsc;

pub enum ChannelContent<M> {
    Message(Message<M>),
    Vertex(i64),
}

enum ChannelMessage<M> {
    Content(ChannelContent<M>),
    HaltCmd,
}

pub struct Channel<M> {
    receiver: mpsc::Receiver<ChannelMessage<M>>,
    senders: Vec<mpsc::Sender<ChannelMessage<M>>>,
}

impl<M> Channel<M> {
    pub fn create(n: usize) -> HashMap<i64, Channel<M>> {
        let mut channels = HashMap::new();

        let mut receivers = Vec::with_capacity(n);
        let mut senderss = vec![Vec::with_capacity(n); n];
        for _ in 0..n {
            let (sender, receiver) = mpsc::channel();
            for senders in &mut senderss {
                senders.push(sender.clone());
            }
            receivers.push(receiver);
        }

        // Vec::pop() removes item from the end of the vector, which means we get receiver and senders
        // at the order (self.nworkers-1)..0. Thus, we have to create workers in such order or we'll
        // assign wrong receiver and senders to workers.
        for i in (0..n).rev() {
            let channel = Channel::new(receivers.pop().unwrap(), senderss.pop().unwrap());
            channels.insert(i as i64, channel);
        }

        channels
    }

    fn new(
        receiver: mpsc::Receiver<ChannelMessage<M>>,
        senders: Vec<mpsc::Sender<ChannelMessage<M>>>,
    ) -> Self {
        Channel { receiver, senders }
    }

    pub fn send(&self, content: ChannelContent<M>) {
        let index = match &content {
            ChannelContent::Message(message) => {
                (message.receiver % self.senders.len() as i64) as usize
            }
            ChannelContent::Vertex(id) => (id % self.senders.len() as i64) as usize,
        };

        match self.senders[index].send(ChannelMessage::Content(content)) {
            Err(e) => eprintln!("Send message content failed: {}", e),
            _ => (),
        }
    }

    pub fn send_done(&self) {
        for sender in &self.senders {
            match sender.send(ChannelMessage::HaltCmd) {
                Err(e) => eprintln!("Send halt command failed: {}", e),
                _ => (),
            }
        }
    }
}

impl<'a, M> IntoIterator for &'a Channel<M> {
    type Item = ChannelContent<M>;
    type IntoIter = ChannelIterator<'a, M>;

    fn into_iter(self) -> Self::IntoIter {
        ChannelIterator {
            hlt_cnt: 0,
            channel: self,
        }
    }
}

pub struct ChannelIterator<'a, M> {
    hlt_cnt: i64,
    channel: &'a Channel<M>,
}

impl<'a, M> Iterator for ChannelIterator<'a, M> {
    type Item = ChannelContent<M>;

    fn next(&mut self) -> Option<ChannelContent<M>> {
        while let Ok(channel_message) = self.channel.receiver.recv() {
            match channel_message {
                ChannelMessage::Content(content) => {
                    return Some(content);
                }
                ChannelMessage::HaltCmd => {
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
