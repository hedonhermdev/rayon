use super::plumbing::*;
use super::*;

use std::fmt::{self, Debug};

impl<U, I, ID, F, T> Scan<I, ID, F>
where
    I: ParallelIterator,
    F: Fn(&mut U, I::Item) -> T + Sync + Send,
    ID: Fn() -> U + Sync + Send,
    U: Send,
    T: Send,
{
    pub(super) fn new(base: I, identity: ID, scan_op: F) -> Self {
        Scan {
            base,
            identity,
            scan_op,
        }
    }
}


/// Scan adaptor for the ParallelIterator trait.
pub struct Scan<I, ID, F> {
    base: I,
    identity: ID,
    scan_op: F
}

impl<I: ParallelIterator + Debug, ID, F> Debug for Scan<I, ID, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scan").field("base", &self.base).finish()
    }
}

impl<U, I, ID, F, T> ParallelIterator for Scan<I, ID, F>
where 
    I: IndexedParallelIterator,
    F: Fn(&mut U, I::Item) -> T + Sync + Send,
    ID: Fn() -> U + Sync + Send,
    U: Send,
    T: Send,
{
    type Item = T;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item> {

        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        None
    }
}

impl<U, I, ID, F, T> IndexedParallelIterator for Scan<I, ID, F>
where
    I: IndexedParallelIterator,
    F: Fn(&mut U, I::Item) -> T + Sync + Send,
    ID: Fn() -> U + Sync + Send,
    U: Send,
    T: Send,
{
    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        self.base.len()
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        return self.base.with_producer(Callback {
            callback,
            scan_op: self.scan_op,
            state: self.identity,
        });

        struct Callback<CB, F, S> {
            callback: CB,
            scan_op: F,
            state: S,
        }

        impl<T, U, F, R, CB, S> ProducerCallback<T> for Callback<CB, F, S>
        where
            CB: ProducerCallback<R>,
            F: Fn(&mut U, T) -> R + Sync + Send,
            S: Fn() -> U + Sync + Send,
            R: Send,
            U: Send,
        {
            type Output = CB::Output;

            fn callback<P>(self, base: P) -> Self::Output
            where
                P: Producer<Item = T> 
            {
                let producer = ScanProducer {
                    base,
                    scan_op: &self.scan_op,
                    state: &self.state,
                };
                self.callback.callback(producer)
            }
        }
    }
}

struct ScanProducer<'f, P, F, S> {
    base: P,
    scan_op: &'f F,
    state: &'f S
}

impl<'f, P, F, U, T, ID> Producer for ScanProducer<'f, P, F, ID>
where 
    P: Producer,
    F: Fn(&mut U, <P as plumbing::Producer>::Item) -> T + Sync + Send,
    ID: Fn() -> U + Sync + Send,
    U: Send,
    T: Send,
{
    type Item = T;

    type IntoIter = ScanIter<'f, P::IntoIter, F, U>;

    fn into_iter(self) -> Self::IntoIter {
        ScanIter {
            base: self.base.into_iter(),
            scan_op: self.scan_op,
            state: (self.state)(),
        }

    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (left, right) = self.base.split_at(index);
        (
            ScanProducer {
                base: left,
                scan_op: self.scan_op,
                state: self.state
            },
            ScanProducer {
                base: right,
                scan_op: self.scan_op,
                state: self.state,
            }
        )
    }
}

struct ScanIter<'f, I, F, S> {
    base: I,
    scan_op: &'f F,
    state: S
}

impl<'f, I, F, S, T> Iterator for ScanIter<'f, I, F, S> 
where
    I: Iterator + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(&mut S, I::Item) -> T + Sync + Send,
    T: Send,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.base.next().map(|item| (self.scan_op)(&mut self.state, item))
    }
}

impl<'f, I, F, S, T> DoubleEndedIterator for ScanIter<'f, I, F, S> 
where
    I: Iterator + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(&mut S, I::Item) -> T + Sync + Send,
    T: Send,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.base.next_back().map(|item| (self.scan_op)(&mut self.state, item))
    }
}

impl<'f, I, F, S, T> ExactSizeIterator for ScanIter<'f, I, F, S> 
where
    I: Iterator + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(&mut S, I::Item) -> T + Sync + Send,
    T: Send,
{
    fn len(&self) -> usize {
        self.base.len()
    }
}
