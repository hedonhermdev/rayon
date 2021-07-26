use super::plumbing::*;
use super::*;

use std::fmt::{self, Debug};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crossbeam::channel;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

use std::time::{Duration, Instant};

const TARGET_TIME: Duration = Duration::from_millis(1);

fn recalibrate(time_taken: Duration, target_time: Duration, current_size: usize) -> usize {
    return ((current_size as f64) * (target_time.as_nanos() as f64 / time_taken.as_nanos() as f64))
        as usize;
}

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
                let (sender, receiver) = channel::bounded(crate::current_num_threads());

                let stealers = AtomicUsize::new(0);
                let work = AtomicUsize::new(self.len);

                let producer = AdaptiveProducer::new(
                    self.len,
                    producer,
                    self.block_size,
                    Role::Worker,
                    sender,
                    receiver,
                    &stealers,
                    &work,
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
    stealers: &'f AtomicUsize,
    work: &'f AtomicUsize,
}

impl<'f, P: Producer> AdaptiveProducer<'f, P> {
    fn new(
        len: usize,
        base: P,
        block_size: usize,
        role: Role,
        sender: Sender<Option<AdaptiveProducer<'f, P>>>,
        receiver: Receiver<Option<AdaptiveProducer<'f, P>>>,
        stealers: &'f AtomicUsize,
        work: &'f AtomicUsize,
    ) -> Self {
        Self {
            len,
            base,
            block_size,
            role,
            sender,
            receiver,
            stealers,
            work,
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

    fn min_len(&self) -> usize {
        self.base.min_len()
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        match self.role {
            Role::Worker => {
                // hollow split
                let (worker, stealer) = self.base.split_at(self.len);
                (
                    AdaptiveProducer::new(
                        self.len,
                        worker,
                        self.block_size,
                        Role::Worker,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.stealers,
                        self.work,
                    ),
                    AdaptiveProducer::new(
                        0,
                        stealer,
                        self.block_size,
                        Role::Stealer,
                        self.sender,
                        self.receiver,
                        self.stealers,
                        self.work,
                    ),
                )
            }

            Role::Splitter => {
                // actual split
                let (worker1, worker2) = self.base.split_at(index);
                (
                    AdaptiveProducer::new(
                        index,
                        worker1,
                        self.block_size,
                        Role::Worker,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.stealers,
                        self.work,
                    ),
                    AdaptiveProducer::new(
                        self.len - index,
                        worker2,
                        self.block_size,
                        Role::Worker,
                        self.sender,
                        self.receiver,
                        self.stealers,
                        self.work,
                    ),
                )
            }
            Role::Stealer => {
                // hollow split
                let (stealer1, stealer2) = self.base.split_at(0);
                (
                    AdaptiveProducer::new(
                        0,
                        stealer1,
                        self.block_size,
                        Role::Stealer,
                        self.sender.clone(),
                        self.receiver.clone(),
                        self.stealers,
                        self.work,
                    ),
                    AdaptiveProducer::new(
                        0,
                        stealer2,
                        self.block_size,
                        Role::Stealer,
                        self.sender,
                        self.receiver,
                        self.stealers,
                        self.work,
                    ),
                )
            }
        }
    }

    fn fold_with<F>(self, mut folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        let work = self.work;
        let role = self.role;
        let mut len = self.len;
        let stealers = self.stealers;
        let sender = self.sender.clone();
        let receiver = self.receiver.clone();
        match role {
            Role::Worker => {
                if self.len == 0 {
                    return folder;
                }

                let min_len = self.min_len();
                let prev_len = self.len;
                let mut maybe_producer = Some(self);
                let mut stealer_count = stealers.load(Ordering::SeqCst);
                let mut block_size = min_len;

                while stealer_count == 0 || !(len > min_len) {
                    match maybe_producer {
                        Some(mut producer) => {
                            // Because partial_fold calls split_at and we need an actual split here
                            producer.set_role(Role::Splitter);

                            let start = Instant::now();
                            let (new_folder, new_maybe_producer) =
                                producer.partial_fold(len, block_size, folder);
                            let time_taken = start.elapsed();

                            folder = new_folder;
                            maybe_producer = new_maybe_producer;
                            if len > block_size {
                                len = len - block_size;
                            } else {
                                len = 0;
                            }

                            block_size = {
                                let new_size = recalibrate(time_taken, TARGET_TIME, block_size);
                                if new_size > min_len {
                                    new_size
                                } else {
                                    min_len
                                }
                            };
                        }
                        None => {
                            break;
                        }
                    }

                    stealer_count = stealers.load(Ordering::SeqCst);
                }

                let work_done = prev_len - len;
                let work_left = work.fetch_sub(work_done, Ordering::SeqCst) - work_done;

                if work_done > 0 && work_left == 0 {
                    // only one thread can end up here
                    for _ in 0..(crate::current_num_threads() - 1) {
                        // ensure all remaining stealers will end
                        sender.send(None).expect("Failed to send to channel");
                    }
                }

                if let Some(mut producer) = maybe_producer {
                    if producer.len != 0 {
                        let mut stealer_count = stealers.load(Ordering::SeqCst);
                        while stealer_count != 0 {
                            match stealers.compare_exchange(
                                stealer_count,
                                stealer_count - 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    producer.set_role(Role::Splitter);
                                    return producer.fold_with(folder);
                                }
                                Err(new_stealer_count) => stealer_count = new_stealer_count,
                            }
                        }
                        return producer.fold_with(folder);
                    }
                }
                folder
            }
            Role::Splitter => {
                let mid = self.len / 2;
                let (left_p, right_p) = self.split_at(mid);
                sender
                    .send(Some(right_p))
                    .expect("Failed to send to channel");
                left_p.fold_with(folder)
            }
            Role::Stealer => {
                stealers.fetch_add(1, Ordering::SeqCst);
                if work.load(Ordering::SeqCst) == 0 {
                    return folder;
                }
                let stolen_task = receiver.recv().expect("receiving failed");

                match stolen_task {
                    Some(producer) => producer.fold_with(folder),
                    _ => folder,
                }
            }
        }
    }
}
