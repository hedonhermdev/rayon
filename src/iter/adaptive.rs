use super::plumbing::*;
use super::*;

use std::fmt::{self, Debug};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

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
        println!("here");
        return self.base.with_producer(Callback {
            callback,
        });

        struct Callback<CB> {
            callback: CB,
        }

        impl<T, CB> ProducerCallback<T> for Callback<CB>
        where
            CB: ProducerCallback<T>,
        {
            type Output = CB::Output;

            fn callback<P>(self, base: P) -> CB::Output
            where
                P: Producer<Item = T>,
            {
                let done = &AtomicBool::new(false);
                let producer = AdaptiveProducer {
                    base,
                    role: Role::Worker,
                    done,
                };
                self.callback.callback(producer)
            }
        }
    }

    fn len(&self) -> usize {
        self.base.len()
    }
}

struct AdaptiveProducer<'f, P: Producer> {
    base: P,
    role: Role,
    done: &'f AtomicBool,
}

enum Role {
    Worker,
    Stealer,
}

impl<'f, P> Producer for AdaptiveProducer<'f, P>
where
    P: Producer,
{
    type Item = P::Item;
    type IntoIter = P::IntoIter;

    fn split_at(self, _index: usize) -> (Self, Self) {
        // Splits are hollow splits. No splitting is actually happening
        match self.role {
            Role::Worker => {
                let (stealer, worker) = self.base.split_at(0);
                (
                    AdaptiveProducer {
                        base: worker,
                        role: Role::Worker,
                        done: self.done,
                    },
                    AdaptiveProducer {
                        base: stealer,
                        role: Role::Stealer,
                        done: self.done,
                    },
                )
            }
            Role::Stealer => {
                let (stealer1, stealer2) = self.base.split_at(0);
                (
                    AdaptiveProducer {
                        base: stealer1,
                        role: Role::Stealer,
                        done: self.done,
                    },
                    AdaptiveProducer {
                        base: stealer2,
                        role: Role::Stealer,
                        done: self.done,
                    },
                )
            }
        }
    }

    fn fold_with<F>(self, folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        match self.role {
            Role::Worker => {
                let done = self.done;
                let iter = self.into_iter();
                
                let new_folder = folder.consume_iter(iter);
                done.store(true, Ordering::Relaxed);

                new_folder
            },
            Role::Stealer => {
                println!("in stealer");
                while !self.done.load(Ordering::Relaxed) {
                    // We wait until the worker has finished 
                }
            folder
            }
        }
    }

    fn into_iter(self) -> Self::IntoIter {
        self.base.into_iter()
    }
}
