use super::plumbing::*;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use rayon_core::current_num_threads;
use tracing::{span, Level};

const TARGET_TIME: Duration = Duration::from_millis(1);

fn recalibrate(time_taken: Duration, target_time: Duration, current_size: usize) -> usize {
    return ( (current_size as f64) * ( target_time.as_nanos() as f64 / time_taken.as_nanos() as f64 ) ) as usize
}


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
        if self.role != Role::Splitter {
            let span = span!(Level::TRACE, "fake_split");
            let _guard = span.enter();
        }
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
            Role::Stealer => {
                if self.workers.load(Ordering::SeqCst) == 0 {
                    return (self, None);
                } else {
                    return (
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
                    );
                }
            }
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
            match self.role {
                Role::Worker => {
                    if self.stealers.load(Ordering::SeqCst) != 0 {
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

                    let base = self.base.take().unwrap();
                    let block_size = self.block_size;

                    let start = std::time::Instant::now();
                    let (new_folder, new_base) = base.partial_fold(folder, block_size);
                    let time_taken = std::time::Instant::now().duration_since(start);

                    folder = new_folder;

                    self.block_size = recalibrate(time_taken, TARGET_TIME, self.block_size);

                    println!("{}", self.block_size);

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
                    let span = span!(Level::TRACE, "stealer entered");
                    let _guard = span.enter();
                    self.stealers.fetch_add(1, Ordering::SeqCst);

                    if self.workers.load(Ordering::SeqCst) == 0 {
                        return folder;
                    }
                    let stolen_task = self.receiver.recv().expect("Failed to receive on channel");

                    let span = span!(Level::TRACE, "stealer received");
                    let _guard = span.enter();

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
                Role::Splitter => {
                    panic!("not here")
                }
            }
        }
    }
}
