use super::plumbing::*;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;

pub(super) struct AdaptiveProducer<'f, P: UnindexedProducer> {
    base: Option<P>,
    block_size: usize,
    role: Role,
    sender: Sender<Option<AdaptiveProducer<'f, P>>>,
    receiver: Receiver<Option<AdaptiveProducer<'f, P>>>,
    stealers: &'f AtomicUsize,
    workers: &'f AtomicUsize,
}

impl<'f, P: UnindexedProducer> AdaptiveProducer<'f, P> {
    pub(super) fn new(
        base: Option<P>,
        block_size: usize,
        role: Role,
        sender: Sender<Option<AdaptiveProducer<'f, P>>>,
        receiver: Receiver<Option<AdaptiveProducer<'f, P>>>,
        stealers: &'f AtomicUsize,
        workers: &'f AtomicUsize,
    ) -> Self {
        Self {
            base,
            block_size,
            role,
            sender,
            receiver,
            stealers,
            workers,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Role {
    Worker,
    Stealer,
    Splitter,
}

impl<'f, P: UnindexedProducer> UnindexedProducer for AdaptiveProducer<'f, P> {
    type Item = P::Item;

    fn split(mut self) -> (Self, Option<Self>) {
        match self.role {
            Role::Worker => (
                AdaptiveProducer::new(
                    self.base.take(),
                    self.block_size,
                    Role::Worker,
                    self.sender.clone(),
                    self.receiver.clone(),
                    self.stealers,
                    self.workers,
                ),
                Some(AdaptiveProducer::new(
                    None,
                    self.block_size,
                    Role::Stealer,
                    self.sender.clone(),
                    self.receiver.clone(),
                    self.stealers,
                    self.workers,
                )),
            ),
            Role::Stealer => (
                AdaptiveProducer::new(
                    None,
                    self.block_size,
                    Role::Stealer,
                    self.sender.clone(),
                    self.receiver.clone(),
                    self.stealers,
                    self.workers,
                ),
                Some(AdaptiveProducer::new(
                    None,
                    self.block_size,
                    Role::Stealer,
                    self.sender.clone(),
                    self.receiver.clone(),
                    self.stealers,
                    self.workers,
                )),
            ),
            Role::Splitter => {
                let (left_p, right_p) = self.base.unwrap().split();
                if right_p.is_some() {
                    (
                        AdaptiveProducer::new(
                            Some(left_p),
                            self.block_size,
                            Role::Worker,
                            self.sender.clone(),
                            self.receiver.clone(),
                            self.stealers,
                            self.workers,
                        ),
                        Some(AdaptiveProducer::new(
                            right_p,
                            self.block_size,
                            Role::Worker,
                            self.sender.clone(),
                            self.receiver.clone(),
                            self.stealers,
                            self.workers,
                        )),
                    )
                } else {
                    (
                        AdaptiveProducer::new(
                            Some(left_p),
                            self.block_size,
                            Role::Worker,
                            self.sender.clone(),
                            self.receiver.clone(),
                            self.stealers,
                            self.workers,
                        ),
                        Some(AdaptiveProducer::new(
                            None,
                            self.block_size,
                            Role::Stealer,
                            self.sender.clone(),
                            self.receiver.clone(),
                            self.stealers,
                            self.workers,
                        )),
                    )
                }
            }
        }
    }

    fn fold_with<F>(mut self, mut folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        match self.role {
            Role::Worker => {
                self.workers.fetch_add(1, Ordering::SeqCst);
                let mut maybe_producer = self.base.take();
                let mut stealer_count = self.stealers.load(Ordering::SeqCst);

                while maybe_producer.is_some() && stealer_count == 0 {
                    let producer = maybe_producer.unwrap();
                    let (new_folder, new_producer) = producer.partial_fold(folder, self.block_size);
                    folder = new_folder;
                    maybe_producer = new_producer;

                    if maybe_producer.is_none() {
                        break;
                    }

                    stealer_count = self.stealers.load(Ordering::SeqCst);
                }

                if let Some(producer) = maybe_producer {
                    let mut stealer_count = self.stealers.load(Ordering::SeqCst);
                    while stealer_count != 0 {
                        match self.stealers.compare_exchange(stealer_count, stealer_count - 1, Ordering::SeqCst, Ordering::SeqCst) {
                            Ok(_) => {
                                self.role = Role::Splitter;
                                self.base.insert(producer);
                                return self.fold_with(folder);
                            }
                            Err(new_stealer_count) => stealer_count = new_stealer_count,
                        }
                    }
                    self.base.insert(producer);
                    return self.fold_with(folder);
                }

                self.workers.fetch_sub(1, Ordering::SeqCst);

                folder
            }
            Role::Splitter => {
                let ref sender = self.sender.clone();

                let (left_p, right_p) = self.split();
                
                sender.send(right_p).expect("Failed to send on channel");

                left_p.fold_with(folder)
            }
            Role::Stealer => {
                self.stealers.fetch_add(1, Ordering::SeqCst);

                if self.workers.load(Ordering::SeqCst) == 0 {
                    return folder;
                }

                let stolen_task = self.receiver.recv().expect("Failed to receive on channel");

                match stolen_task {
                    Some(producer) => producer.fold_with(folder),
                    None => folder,
                }
            }
        }
    }
}

