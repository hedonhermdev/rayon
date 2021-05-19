//! The powers module exports a single ParallelPowers iterator and a parallel_producer function to
//! create this iterator . This iterator produces powers of 2
//! sequentially until a limit is reached. 
use crate::iter::*;

use std::ops::Range;

/// Creates a ParallelProducer with the given limit. 
pub fn parallel_powers(limit: u32) -> ParallelPowers {
    ParallelPowers {
        begin: 0,
        end: limit,
    }
}

/// Produce powers of two until the limit is reached. 
#[derive(Debug)]
pub struct ParallelPowers {
    begin: u32,
    end: u32,
}

impl ParallelIterator for ParallelPowers {
    type Item = u32;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
            C: UnindexedConsumer<Self::Item> {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.len())
    }
}

impl IndexedParallelIterator for ParallelPowers {
    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        bridge(self, consumer)
    }

    fn len(&self) -> usize {
        (self.end - self.begin) as usize
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        callback.callback(ParallelPowersProducer { range: (self.begin..self.end)})
    }
}


struct ParallelPowersProducer {
    range: Range<u32>,
}

impl Producer for ParallelPowersProducer {
    type Item = u32;
    type IntoIter = PowersIter;

    fn into_iter(self) -> Self::IntoIter {
        PowersIter { range: self.range }
    }

    fn split_at(self, index: usize) -> (Self, Self) {
        let (lrange, rrange) = split_range(self.range, index as u32);

        (
            ParallelPowersProducer { range: lrange },
            ParallelPowersProducer { range: rrange },
        )
    }
}

struct PowersIter {
    range: Range<u32>,
}

impl Iterator for PowersIter {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let base: u32 = 2;
        self.range.next().map(|x| base.pow(x))
    }
}

impl DoubleEndedIterator for PowersIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let base: u32 = 2;
        self.range.next_back().map(|x| base.pow(x))
    }
}

impl ExactSizeIterator for PowersIter {
    fn len(&self) -> usize {
        self.range.len()
    }
}


// Utility function to split a range into two ranges at given index.
fn split_range(r: Range<u32>, index: u32) -> (Range<u32>, Range<u32>) {
    if r.end - r.start <= 1 {
        panic!("Split too much")
    }

    let splitpoint = r.start + index;

    (r.start..splitpoint, splitpoint..r.end)
}
