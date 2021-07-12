use super::plumbing::*;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::current_thread_index;

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use rayon_core::current_num_threads;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
                    self.workers.fetch_add(1, Ordering::SeqCst);
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
                        None,
                    )
                }
            }
        }
    }

    fn fold_with<F>(mut self, mut folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        'fold: loop {
            if self.role == Role::Worker && self.base.is_some() && self.stealers.load(Ordering::SeqCst) != 0 {
                let mut stealer_count = self.stealers.load(Ordering::SeqCst);
                while stealer_count != 0 {
                    match self.stealers.compare_exchange(
                        stealer_count,
                        stealer_count - 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            let sender = self.sender.clone();

                            self.role = Role::Splitter;
                            let (left, right) = self.split();
                            if right.is_some() {
                                sender.send(right).expect("Failed to send on channel");
                            }
                            self = left;
                            continue 'fold;
                        }
                        Err(new_stealer_count) => stealer_count = new_stealer_count,
                    }
                }
            }

            match self.role {
                Role::Worker => {
                    let base = self.base.take().unwrap();

                    let (new_folder, new_base) =
                        base.partial_fold(folder, self.block_size);
                    folder = new_folder;

                    if new_base.is_some() {
                        self.base.insert(new_base.unwrap());
                    } else {
                        // everything done. terminate waiting stealers and return the folder
                        let workers = self.workers.fetch_sub(1, Ordering::SeqCst) - 1;
                        if workers == 0 {
                            for _ in 0..current_num_threads() {
                                self.sender.send(None).expect("Failed to send on channel");
                            }
                        }
                        return folder;
                    }
                }
                Role::Stealer => {
                    self.stealers.fetch_add(1, Ordering::SeqCst);

                    if self.workers.load(Ordering::SeqCst) == 0 {
                        return folder;
                    }
                    let stolen_task = self.receiver.recv().expect("Failed to receive on channel");

                    match stolen_task {
                        Some(producer) => {
                            self = producer;
                            self.role = Role::Worker;
                            continue;
                        }
                        None => {
                            return folder;
                        }
                    }
                }
                Role::Splitter => { panic!("not here")}
            }
        }
    }
}
