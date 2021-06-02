use super::plumbing::*;
use super::*;

use std::fmt::{self, Debug};

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

        let len = self.base.len();
        return self.base.with_producer(Callback {
            len,
            consumer,
            block_size: self.block_size,
        });

        struct Callback<D> { 
            consumer: D,
            block_size: usize,
            len: usize,
        }

        impl<D, T> ProducerCallback<T> for Callback<D> 
        where
            D: Consumer<T>,
            T: Send,
        {
            type Output = D::Result;
            fn callback<P>(self, producer: P) -> Self::Output
            where
                P: Producer<Item = T> 
            {
                adaptive_fold(producer, self.consumer, self.len, self.block_size)   
            }
        }
    }
}

// impl<I> IndexedParallelIterator for Adaptive<I>
// where
//     I: IndexedParallelIterator,
// {
//     fn len(&self) -> usize {
//         self.base.len()
//     }

//     fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
//         let consumer1 = AdaptiveConsumer {
//             base: consumer,
//         };
//         return self.with_producer(Callback {
//             consumer: consumer1,
//             block_size: self.block_size,
//             len: self.len(),
//         });

//         struct Callback<D> {
//             consumer: AdaptiveConsumer<D>, 
//             block_size: usize,
//             len: usize,
//         }

//         impl<D, T> ProducerCallback<T> for Callback<D>
//         where
//             D: Consumer<T>,
//             D::Reducer: Sync + Send,
//             T: Send,
//         {
//             type Output = D::Result;

//             fn callback<P>(self, producer: P) -> Self::Output
//             where
//                 P: Producer<Item = T>,
//             {
//                 adaptive_fold(producer, self.consumer, None, self.len, self.block_size)
//             }
//         }
//     }

//     fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {}
// }

// impl<C, T> Copy for AdaptiveConsumer<C, T>
// where
//     C: Consumer<T> + Copy
// {}

// impl<C, T> Clone for AdaptiveConsumer<C, T> {
//     fn clone(&self) -> Self {
//         *self
//     }
// }

// struct AdaptiveConsumer<C: Consumer<T> + Clone, T> {
//     base: C,
//     phantom: PhantomData<T>
// }

// impl<C, T> Consumer<T> for AdaptiveConsumer<C, T>
// where
//     C: Consumer<T> + Copy,
//     C::Reducer: Sync + Send,
//     T: Send,
// {
//     type Folder = C::Folder;
//     type Reducer = C::Reducer;
//     type Result = C::Result;

//     fn split_at(self, index: usize) -> (Self, Self, Self::Reducer) {
//         let (consumer1, consumer2, reducer) = self.base.split_at(index);

//         (
//             AdaptiveConsumer { base: consumer1, phantom: PhantomData },
//             AdaptiveConsumer { base: consumer2, phantom: PhantomData },
//             reducer,
//         )
//     }

//     fn into_folder(self) -> Self::Folder {
//         self.base.into_folder()
//     }

//     fn full(&self) -> bool {
//         self.base.full()
//     }
// }
