use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..=10_000_000).collect();

    let result = v
        .par_iter()
        .map(|x| *x as u64)
        .adaptive(10)
        .reduce(|| 0, |a, b| a + b);
        // .reduce(|| 0, |a, b| a + b);

    assert_eq!(result, 50000005000000);
}
