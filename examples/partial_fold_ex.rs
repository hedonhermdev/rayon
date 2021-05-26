use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..=10_000_00).collect();

    let result = v
        .par_iter()
        .map(|x| *x as u64)
        .reduce_by_blocks(|| 0, |a, b| a + b, 5);

    println!("Result: {}", result);
}
