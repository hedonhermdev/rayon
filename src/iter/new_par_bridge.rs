use crate::iter::adaptive::AdaptiveProducer;
use super::ParallelIterator;
use super::*;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam::channel::bounded;
use rayon_core::current_num_threads;

const VEC_SIZE: usize = 10_000_000;

/// parallel bridge
pub trait NewParallelBridge: Sized {
    /// create a new parallel bridge
    fn new_par_bridge(self, block_size: usize) -> NewIterBridge<Self>;
}

impl<T: Iterator + Sized> NewParallelBridge for T
where
    T::Item: Send,
{
    /// create a new parallel bridge
    fn new_par_bridge(self, block_size: usize) -> NewIterBridge<Self> {
        NewIterBridge {
            iter: self,
            block_size,
        }
    }
}

/// parallel bridge
#[derive(Debug)]
pub struct NewIterBridge<Iter> {
    iter: Iter,
    block_size: usize,
}

impl<Iter: Iterator + Send> ParallelIterator for NewIterBridge<Iter>
where
    Iter::Item: Send + Sync,
{
    type Item = Iter::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: super::plumbing::UnindexedConsumer<Self::Item>,
    {
        let vec = PVec::with_capacity(VEC_SIZE);

        let producer = NewIterProducer {
            iter: Some(BlocksIter::new(self.iter)),
            vec: &vec,
            block_size: self.block_size,
            block: None,
            role: Role::Provider,
        };

        let (sender, receiver) = bounded(current_num_threads());
        let ref stealers = AtomicUsize::new(0);
        let ref work = AtomicUsize::new(0);
        let adaptive = AdaptiveProducer::new(Some(producer), self.block_size, adaptive::Role::Worker, sender, receiver, stealers, work);
        bridge_unindexed(adaptive, consumer)
    }
}

pub(crate) struct NewIterProducer<'a, Iter: Iterator> {
    iter: Option<BlocksIter<Iter>>,
    vec: &'a PVec<Iter::Item>,
    block_size: usize,
    block: Option<PBlock<'a, Iter::Item>>,
    role: Role,
}

impl<'a, Iter: Iterator + Send> UnindexedProducer for NewIterProducer<'a, Iter>
where
    Iter::Item: Send + Sync,
{
    type Item = Iter::Item;

    fn split(mut self) -> (Self, Option<Self>) {
        match self.role {
            Role::Worker => {
                let block = self.block.take().unwrap();
                let mid = block.len() / 2;

                let (left, maybe_right) = block.split_at(mid);

                let left_p = NewIterProducer {
                    iter: None,
                    vec: self.vec,
                    block_size: self.block_size,
                    block: Some(left),
                    role: Role::Worker,
                };

                if maybe_right.is_some() {
                    let right_p = NewIterProducer {
                        iter: None,
                        vec: self.vec,
                        block_size: self.block_size,
                        block: maybe_right,
                        role: Role::Worker,
                    };
                    (left_p, Some(right_p))
                } else {
                    (left_p, None)
                }
            }
            Role::Provider => {
                let mut iter = self.iter.take().unwrap();

                let items = iter.fold_block(vec![], |mut vec, item| { vec.push(item); vec }, self.block_size);

                let block = self.vec.request_block(items.into_iter());

                let worker = NewIterProducer {
                    iter: None,
                    vec: self.vec,
                    block_size: self.block_size,
                    block: Some(block),
                    role: Role::Worker,
                };

                if iter.exhausted() {
                    return (worker, None);
                } else {
                    self.iter.insert(iter);

                    return (self, Some(worker));
                }
            }
        }
    }

    fn fold_with<F>(self, _folder: F) -> F
    where
        F: Folder<Self::Item>,
    {
        unimplemented!()
    }

    fn partial_fold<F>(mut self, mut folder: F, block_size: usize) -> (F, Option<Self>)
    where
        F: Folder<Self::Item>,
    {
        match self.role {
            Role::Worker => {
                let block = self.block.take().unwrap();

                let (work, maybe_block) = block.split_at(block_size);
                folder = folder.consume_iter(work);

                match maybe_block {
                    Some(block) => {
                        self.block.insert(block);
                        (folder, Some(self))
                    }
                    None => (folder, None)
                }
            }
            Role::Provider => {
                let mut iter = self.iter.take().unwrap();
                folder = iter.fold_block(folder, |folder, item| folder.consume(item), block_size);
                if iter.exhausted() {
                    (folder, None)
                } else {
                    self.iter.insert(iter);

                    (folder, Some(self))
                }
            }
        }
    }
}

struct BlocksIter<Iter: Iterator> {
    base: std::iter::Peekable<Iter>,
    count: usize,
}

impl<Iter: Iterator> BlocksIter<Iter> {
    fn new(iter: Iter) -> Self {
        Self {
            base: iter.peekable(),
            count: 0,
        }
    }

    fn set_limit(&mut self, limit: usize) {
        self.count = limit;
    }

    fn fold_block<F, R>(&mut self, init: R, fold_op: F, block_size: usize) -> R
    where
        F: Fn(R, Iter::Item) -> R,
    {
        self.set_limit(block_size);
        self.try_fold(init, |res, elem| -> Result<R, ()> {
            Ok(fold_op(res, elem))
        })
        .ok()
        .unwrap()
    }

    fn exhausted(&mut self) -> bool {
        self.base.peek().is_none()
    }
}

impl<Iter: Iterator> Iterator for BlocksIter<Iter> {
    type Item = Iter::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count > 0 {
            self.count -= 1;
            self.base.next()
        } else {
            None
        }
    }
}

impl<Iter: Iterator> ExactSizeIterator for BlocksIter<Iter> {

}

// PVEC ------------------------------
struct PVec<T> {
    data: Vec<T>,
    len: AtomicUsize,
}

impl<T> PVec<T> {
    fn with_capacity(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        unsafe { data.set_len(capacity) };
        PVec {
            data,
            len: AtomicUsize::new(0),
        }
    }

    fn request_block<'a, I: ExactSizeIterator<Item = T>>(&'a self, content: I) -> PBlock<'a, T> {
        let size = content.len();
        let start = self.len.fetch_add(size, Ordering::SeqCst);
        assert!(self.len.load(Ordering::SeqCst) < self.data.capacity());
        let slice = &self.data[start..start + size];
        slice
            .iter()
            .zip(content)
            .for_each(|(s, c)| unsafe { std::ptr::write(s as *const T as *mut T, c) });
        PBlock { slice }
    }
}

impl<T> std::ops::Drop for PVec<T> {
    fn drop(&mut self) {
        unsafe { self.data.set_len(0) }
    }
}

struct PBlock<'a, T> {
    slice: &'a [T],
}

impl<'a, T> PBlock<'a, T> {
    fn len(&self) -> usize {
        self.slice.len()
    }
    fn split_at(self, index: usize) -> (Self, Option<Self>) {
        let l = self.slice.len();
        if l <= index {
            (self, None)
        } else {
            let (left, right) = self.slice.split_at(index);
            (PBlock { slice: left }, Some(PBlock { slice: right }))
        }
    }
}

impl<'a, T> Iterator for PBlock<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((head, remaining)) = self.slice.split_first() {
            self.slice = remaining;
            Some(unsafe { std::ptr::read(head) })
        } else {
            None
        }
    }
}

impl<'a, T> std::ops::Drop for PBlock<'a, T> {
    fn drop(&mut self) {
        self.for_each(|_| ())
    }
}

// ROLE ------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
enum Role {
    Worker,
    Provider,
}
