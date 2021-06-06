use super::plumbing::*;
use super::*;

use std::fmt::{self, Debug};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use crossbeam::channel;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use rayon_core::current_thread_index;

/// An Adaptive parallel iterator
pub struct Adaptive<I: IndexedParallelIterator> {
    base: I,
    block_size: usize,
}

impl<I: IndexedParallelIterator + Debug> Debug for Adaptive<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Adaptive")
            .field("base", &self.base)
            .finish()
    }
}

impl<I> Adaptive<I>
where
    I: IndexedParallelIterator,
{
    pub(super) fn new(base: I, block_size: usize) -> Self {
        Adaptive { base, block_size }
    }
}

impl<I> ParallelIterator for Adaptive<I>
where
    I: IndexedParallelIterator,
{
    type Item = I::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }
}

impl<I> IndexedParallelIterator for Adaptive<I>
where
    I: IndexedParallelIterator,
{
    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        let len = self.len();
        return self.base.with_producer(Callback {
            callback,
            block_size: self.block_size,
            len,
        });

        struct Callback<CB> {
            callback: CB,
            len: usize,
            block_size: usize,
        }

        impl<T, CB> ProducerCallback<T> for Callback<CB>
        where
            CB: ProducerCallback<T>,
        {
            type Output = CB::Output;

            fn callback<P>(self, producer: P) -> CB::Output
            where
                P: Producer<Item = T>,
            {
                let (sender, receiver) = channel::unbounded();

                let counter = AtomicU8::new(0);
                let done = AtomicBool::new(false);

                let producer = AdaptiveProducer::new(
                    self.len,
                    producer,
                    self.block_size,
                    Role::Worker,
                    sender,
                    receiver,
                    &counter,
                    &done,
                );
                self.callback.callback(producer)
            }
        }
    }

    fn len(&self) -> usize {
        self.base.len()
    }
}

struct AdaptiveProducer<'f, P: Producer> {
    len: usize,
    base: P,
    block_size: usize,
    role: Role,
    sender: Sender<Option<AdaptiveProducer<'f, P>>>,
    receiver: Receiver<Option<AdaptiveProducer<'f, P>>>,
    counter: &'f AtomicU8,
    done: &'f AtomicBool,
}

impl<'f, P: Producer> AdaptiveProducer<'f, P> {
    fn new(
        len: usize,
        base: P,
        block_size: usize,
        role: Role,
        sender: Sender<Option<AdaptiveProducer<'f, P>>>,
        receiver: Receiver<Option<AdaptiveProducer<'f, P>>>,
        counter: &'f AtomicU8,
        done: &'f AtomicBool,
    ) -> Self {
        Self {
            len,
            base,
            block_size,
            role,
            sender,
            receiver,
            counter,
            done,
        }
    }

    fn set_role(&mut self, role: Role) {
        self.role = role;
    }
}

#[derive(Debug, Clone, Copy)]
enum Role {
    Worker,
    Stealer,
    Splitter,
}

impl<'f, P> Producer for AdaptiveProducer<'f, P>
where
    P: Producer,
{
    type Item = P::Item;
    type IntoIter = P::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.base.into_iter()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        // Splits are hollow splits. No splitting is actually happening
        match self.role {
            Role::Worker => {
                let (worker, waiter) = self.base.split_at(self.len);
                (
                    AdaptiveProducer::new(
                        self.len,
                        worker,
                        self.block_size,
                        Role::Worker,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.counter,
                        self.done,
                    ),
                    AdaptiveProducer::new(
                        0,
                        waiter,
                        self.block_size,
                        Role::Stealer,
                        self.sender,
                        self.receiver,
                        self.counter,
                        self.done,
                    ),
                )
            }

            Role::Splitter => {
                let (worker1, worker2) = self.base.split_at(index);
                (
                    AdaptiveProducer::new(
                        index,
                        worker1,
                        self.block_size,
                        Role::Worker,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.counter,
                        self.done,
                    ),
                    AdaptiveProducer::new(
                        self.len - index,
                        worker2,
                        self.block_size,
                        Role::Worker,
                        self.sender,
                        self.receiver,
                        self.counter,
                        self.done,
                    ),
                )
            }
            _ => {
                let (stealer1, stealer2) = self.base.split_at(0);

                (
                    AdaptiveProducer::new(
                        0,
                        stealer1,
                        self.block_size,
                        Role::Stealer,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.counter,
                        self.done,
                    ),
                    AdaptiveProducer::new(
                        0,
                        stealer2,
                        self.block_size,
                        Role::Stealer,
                        self.sender,
                        self.receiver,
                        self.counter,
                        self.done,
                    ),
                )
            }
        }
    }

    fn fold_with<F>(self, mut folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        let done = self.done;
        let role = self.role;
        let mut len = self.len;
        let counter = self.counter;
        let block_size = self.block_size;
        match role {
            Role::Worker => {
                let mut maybe_producer = Some(self);
                while counter.load(Ordering::Relaxed) == 0 {
                    match maybe_producer {
                        Some(mut producer) => {
                            producer.set_role(Role::Splitter);
                            let (new_folder, new_maybe_producer) = producer.partial_fold(len, block_size, folder);
                            folder = new_folder;
                            maybe_producer = new_maybe_producer;
                            if len > block_size {
                                len = len - block_size;
                            } else {
                                len = 0;
                            }
                        }
                        None => {
                            done.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                let count = counter.load(Ordering::Relaxed);
                if count != 0 && maybe_producer.is_some() {
                    let mut producer = maybe_producer.unwrap();
                    producer.set_role(Role::Splitter);
                    folder = producer.fold_with(folder);
                }

                folder
            }

            Role::Splitter => {
                counter.fetch_sub(1, Ordering::Relaxed);
                let mid = self.len / 2;
                let sender = self.sender.clone();
                let (left_p, right_p) = self.split_at(mid);
                sender
                    .send(Some(right_p))
                    .expect("Failed to send to channel");

                left_p.fold_with(folder)
            }
            Role::Stealer => {
                let done = self.done.load(Ordering::Relaxed);
                if done {
                    return folder;
                }

                counter.fetch_add(1, Ordering::Relaxed);
                println!("Stealing");
                let stolen_task = self.receiver.recv().expect("Failed to receive on channel");

                match stolen_task {
                    Some(producer) => producer.fold_with(folder),
                    None => folder,
                }
            }
        }
    }
}
