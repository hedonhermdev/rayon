use rayon::prelude::*;
use std::time::Duration;

fn main() {
    let v: Vec<u32> = (0..=10_000_000).collect();

    let result = v.par_iter()
            .map(|x| *x as u64)
            .adaptive(Duration::from_millis(8))
            .reduce(|| 0, |a, b| a + b);
    println!("Result: {:?}", result);
}
