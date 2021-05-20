use super::plumbing::*;
use super::ParallelIterator;

/// Reduce an iterator in reverse.
pub(super) fn rev_reduce<PI, R, ID, T>(pi: PI, identity: ID, reduce_op: R) -> T
where 
    PI: ParallelIterator<Item = T>,
    R: Fn(T, T) -> T + Sync,
    ID: Fn() -> T + Sync,
    T: Send,
{
    let consumer = ReverseReduceConsumer {
        identity: &identity,
        reduce_op: &reduce_op,
    };
    pi.drive_unindexed(consumer)
}

struct ReverseReduceConsumer<'r, R, ID> {
    identity: &'r ID,
    reduce_op: &'r R,
}

impl<'r, R, ID> Copy for ReverseReduceConsumer<'r, R, ID> {}

impl<'r, R, ID> Clone for ReverseReduceConsumer<'r, R, ID> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'r, R, ID, T> Consumer<T> for ReverseReduceConsumer<'r, R, ID>
where
    R: Fn(T, T) -> T + Sync,
    ID: Fn() -> T + Sync,
    T: Send,
{
    type Folder = ReverseReduceFolder<'r, R, T>;
    type Reducer = Self;
    type Result = T;

    fn split_at(self, _index: usize) -> (Self, Self, Self) {
        (self, self, self)
    }

    fn into_folder(self) -> Self::Folder {
        ReverseReduceFolder {
            reduce_op: self.reduce_op,
            item: (self.identity)(),
        }
    }

    fn full(&self) -> bool {
        false
    }
}

impl<'r, R, ID, T> UnindexedConsumer<T> for ReverseReduceConsumer<'r, R, ID>
where
    R: Fn(T, T) -> T + Sync,
    ID: Fn() -> T + Sync,
    T: Send
{
    fn split_off_left(&self) -> Self {
        *self
    }

    fn to_reducer(&self) -> Self::Reducer {
        *self
    }
}

impl<'r, R, ID, T> Reducer<T> for ReverseReduceConsumer<'r, R, ID>
where 
    R: Fn(T, T) -> T + Sync,
    ID: Fn() -> T + Sync,
    T: Send,
{
    fn reduce(self, left: T, right: T) -> T {
        (self.reduce_op)(right, left)
    }
}

struct ReverseReduceFolder<'r, R, T> {
    reduce_op: &'r R,
    item: T,
}

impl<'r, R, T> Folder<T> for ReverseReduceFolder<'r, R, T> 
where
    R: Fn(T, T) -> T,
{
    type Result = T;

    fn consume(self, item: T) -> Self {
        ReverseReduceFolder {
            reduce_op: self.reduce_op,
            item: (self.reduce_op)(item, self.item),
        }
    }

    // fn consume_iter<I>(mut self, iter: I) -> Self
    // where
    //     I: IntoIterator<Item = T>,
    // {
    //     ReverseReduceFolder {
    //         reduce_op: self.reduce_op,
    //         item: iter.into_iter().fold(self.item, self.reduce_op)
    //     }           
    // }

    fn complete(self) -> Self::Result {
        self.item
    }

    fn full(&self) -> bool {
        false
    }
}
