use crossbeam::channel::{bounded, Receiver, Sender};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};

use rayon_core::current_num_threads;

use crate::iter::plumbing::*;
use crate::iter::*;

/// Adaptive parallel bridge.
pub trait AdaptiveParallelBridge: Sized {
    /// Create a parallel bridge for a seqiter.
    fn adaptive_par_bridge(self, max_vec_size: usize) -> AdaptiveIterBridge<Self>;
}

impl<T: Iterator + Sized> AdaptiveParallelBridge for T
where
    T::Item: Send,
{
    fn adaptive_par_bridge(self, max_vec_size: usize) -> AdaptiveIterBridge<Self> {
        AdaptiveIterBridge {
            iter: self,
            max_vec_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveIterBridge<Iter> {
    iter: Iter,
    max_vec_size: usize,
}

impl<Iter: Iterator + Send> ParallelIterator for AdaptiveIterBridge<Iter>
where
    Iter::Item: Send,
{
    type Item = Iter::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        let num_threads = current_num_threads();
        let split_count = AtomicUsize::new(num_threads);
        let stealers = AtomicUsize::new(0);
        let workers = AtomicUsize::new(0);
        let vec_size = AtomicUsize::new(1);
        let done = AtomicBool::new(false);

        let (sender, receiver) = bounded(num_threads);

        let producer = AdaptiveIterProducer {
            max_vec_size: self.max_vec_size,
            iter: Some(self.iter),
            vec: None,
            role: Role::Provider,
            split_count: &split_count,
            vec_size: &vec_size,
            done: &done,
            sender,
            receiver,
            stealers: &stealers,
            workers: &workers,
        };

        bridge_unindexed(producer, consumer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Role {
    Worker,
    Provider,
    Stealer,
}

enum Task<Iter: Iterator> {
    Iter(Iter),
    Vector(Vec<Iter::Item>),
}

#[derive(Debug, Clone)]
struct AdaptiveIterProducer<'a, Iter: Iterator> {
    max_vec_size: usize,
    iter: Option<Iter>,
    vec: Option<Vec<Iter::Item>>,
    role: Role,
    split_count: &'a AtomicUsize,
    vec_size: &'a AtomicUsize,
    done: &'a AtomicBool,
    sender: Sender<Option<Task<Iter>>>,
    receiver: Receiver<Option<Task<Iter>>>,
    stealers: &'a AtomicUsize,
    workers: &'a AtomicUsize,
}

impl<'a, Iter: Iterator + Send> UnindexedProducer for AdaptiveIterProducer<'a, Iter>
where
    Iter::Item: Send,
{
    type Item = Iter::Item;

    fn split(self) -> (Self, Option<Self>) {
        let mut count = self.split_count.load(Ordering::SeqCst);

        loop {
            let done = self.done.load(Ordering::SeqCst);

            match count.checked_sub(1) {
                Some(new_count) if !done => {
                    match self.split_count.compare_exchange(
                        count,
                        new_count,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            let stealer = AdaptiveIterProducer {
                                max_vec_size: self.max_vec_size,
                                vec_size: self.vec_size,
                                iter: None,
                                vec: None,
                                role: Role::Stealer,
                                split_count: self.split_count,
                                done: self.done,
                                sender: self.sender.clone(),
                                receiver: self.receiver.clone(),
                                stealers: self.stealers,
                                workers: self.workers,
                            };
                            return (self, Some(stealer));
                        }
                        Err(prev_count) => count = prev_count,
                    }
                }
                _ => {
                    return (self, None);
                }
            }
        }
    }

    fn fold_with<F>(mut self, mut folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        let receiver = self.receiver;
        let mut block_size = 1;

        // update the counts
        if self.role == Role::Worker {
            self.workers.fetch_add(1, Ordering::SeqCst);
        } else if self.role == Role::Stealer {
            self.stealers.fetch_add(1, Ordering::SeqCst);
        }

        'fold: loop {
            // if done, terminate the waiting stealers and return the folder
            if self.done.load(Ordering::SeqCst) && self.vec.is_none() && self.iter.is_none() && self.role != Role::Stealer {
                for _ in 0..current_num_threads() {
                    self.sender.send(None).expect("Failed to send");
                }
                return folder;
            }

            let mut stealer_count = self.stealers.load(Ordering::SeqCst);
            if self.vec.is_some() && stealer_count != 0 { // give task to stealer
                while stealer_count != 0 {
                    match self.stealers.compare_exchange(
                        stealer_count,
                        stealer_count - 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            // double the vec size everytime a steal occurs
                            let vec_size = self.vec_size.load(Ordering::Relaxed);
                            if vec_size < self.max_vec_size {
                                let _ = self.vec_size.compare_exchange(
                                    vec_size,
                                    vec_size * 2,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                );
                            }

                            match self.role {
                                // If worker, give half of the vector to stealer
                                Role::Worker => {
                                    block_size = 1;
                                    let mut vec = self.vec.take().unwrap();
                                    let stealer_vec = vec.split_off(vec.len() / 2);
                                    self.vec.insert(vec);
                                    self.sender
                                        .send(Some(Task::Vector(stealer_vec)))
                                        .expect("Failed to send on channel");
                                    continue 'fold;
                                }
                                Role::Provider => {
                                    let iter = self.iter.take().unwrap();
                                    self.sender
                                        .send(Some(Task::Iter(iter)))
                                        .expect("Failed to send on channel");
                                    // after provider is stolen, it gives the iterator to the
                                    // stealer and turns into a worker
                                    // and folds its own vector
                                    self.role = Role::Worker;
                                    continue 'fold;
                                }
                                Role::Stealer => {}
                            }
                        }
                        Err(new_stealer_count) => stealer_count = new_stealer_count,
                    }
                }
            }

            match self.role {
                Role::Worker => {
                    let mut vec = self.vec.take().unwrap();
                    if vec.len() > block_size {
                        let new_vec = vec.split_off(block_size);
                        if new_vec.len() != 0 {
                            self.vec.insert(new_vec);
                        }
                    }
                    folder = folder.consume_iter(vec);
                    if folder.full() {
                        self.done.store(true, Ordering::SeqCst);
                        continue 'fold;
                    }
                    if self.vec.is_none() {
                        // done working
                        self.workers.fetch_sub(1, Ordering::SeqCst);
                        // if there is no provider, turn into a provider
                        // else, turn into a stealer and steal from the other workers/providers
                        if self.iter.is_some() {
                            self.role = Role::Provider;
                        } else {
                            self.role = Role::Stealer;
                        }
                        continue 'fold;
                    }
                    block_size *= 2;
                }
                Role::Provider => {
                    let mut iter = self.iter.take().unwrap();
                    let mut vec = vec![];
                    let vec_size = self.vec_size.load(Ordering::SeqCst);
                    let mut done = false;
                    'collect: for _ in 0..vec_size {
                        if let Some(item) = iter.next() {
                            vec.push(item);
                        } else {
                            done = true;
                            self.done.store(done, Ordering::SeqCst);
                            break 'collect;
                        }
                    }

                    if done && vec.len() == 0 {
                        continue 'fold;
                    } 
                    self.vec.insert(vec);
                    self.iter.insert(iter);
                    self.role = Role::Worker;
                }
                Role::Stealer => {
                    let task = receiver.recv().expect("Failed to receive on channel");
                    match task {
                        Some(Task::Vector(vec)) => {
                            self.role = Role::Worker;
                            self.vec = Some(vec);
                        }
                        Some(Task::Iter(iter)) => {
                            self.iter = Some(iter);
                            self.role = Role::Provider;
                        }
                        None => {
                            return folder;
                        }
                    }
                }
            }
        }
    }
}
