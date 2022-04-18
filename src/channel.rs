use crate::Message;

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::mpsc;

pub enum ChannelMessage<M> {
    Message(Message<M>),
    Vertex(i64),
}

struct ChannelMessageBatch<M>(Vec<ChannelMessage<M>>);

impl<M> Deref for ChannelMessageBatch<M> {
    type Target = Vec<ChannelMessage<M>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M> DerefMut for ChannelMessageBatch<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct Channel<M> {
    batch_size: usize,
    batches: Vec<ChannelMessageBatch<M>>,
    receiver: mpsc::Receiver<ChannelMessageBatch<M>>,
    senders: Vec<mpsc::Sender<ChannelMessageBatch<M>>>,
}

impl<M> Channel<M> {
    pub fn create(n: usize, batch_size: usize) -> HashMap<i64, Channel<M>> {
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
            let channel = Channel::new(
                batch_size,
                receivers.pop().unwrap(),
                senderss.pop().unwrap(),
            );
            channels.insert(i as i64, channel);
        }

        channels
    }

    fn new(
        batch_size: usize,
        receiver: mpsc::Receiver<ChannelMessageBatch<M>>,
        senders: Vec<mpsc::Sender<ChannelMessageBatch<M>>>,
    ) -> Self {
        let mut batches = Vec::with_capacity(senders.len());

        for _ in 0..senders.len() {
            batches.push(ChannelMessageBatch(Vec::with_capacity(batch_size)));
        }

        Channel {
            batch_size,
            batches,
            receiver,
            senders,
        }
    }

    pub fn send(&mut self, message: ChannelMessage<M>) {
        let index = match &message {
            ChannelMessage::Message(msg) => (msg.receiver % self.senders.len() as i64) as usize,
            ChannelMessage::Vertex(id) => (id % self.senders.len() as i64) as usize,
        };

        self.batches[index].push(message);
        if self.batches[index].len() >= self.batch_size {
            let batch = std::mem::replace(
                &mut self.batches[index],
                ChannelMessageBatch(Vec::with_capacity(self.batch_size)),
            );

            if let Err(e) = self.senders[index].send(batch) {
                eprintln!("Send message content failed: {}", e)
            }
        }
    }

    pub fn flush(&mut self) {
        for index in 0..self.batches.len() {
            let batch = std::mem::replace(
                &mut self.batches[index],
                ChannelMessageBatch(Vec::with_capacity(self.batch_size)),
            );

            if let Err(e) = self.senders[index].send(batch) {
                eprintln!("Send message content failed: {}", e)
            }
        }
    }
}

impl<'a, M> IntoIterator for &'a Channel<M> {
    type Item = ChannelMessage<M>;
    type IntoIter = ChannelIterator<'a, M>;

    fn into_iter(self) -> Self::IntoIter {
        ChannelIterator {
            channel: self,
            sender_cnt: self.senders.len(),
            done_cnt: 0,
            batch: ChannelMessageBatch(Vec::new()),
        }
    }
}

pub struct ChannelIterator<'a, M> {
    channel: &'a Channel<M>,
    sender_cnt: usize,
    done_cnt: usize,
    batch: ChannelMessageBatch<M>,
}

impl<'a, M> Iterator for ChannelIterator<'a, M> {
    type Item = ChannelMessage<M>;

    fn next(&mut self) -> Option<ChannelMessage<M>> {
        while self.batch.is_empty() && self.done_cnt < self.sender_cnt {
            match self.channel.receiver.recv() {
                Ok(batch) => {
                    self.batch = batch;
                    if self.batch.len() < self.channel.batch_size {
                        self.done_cnt += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Receive error: {}", e);
                    return None;
                }
            }
        }

        self.batch.pop()
    }
}
