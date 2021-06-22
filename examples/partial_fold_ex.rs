use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..=10_000_000).collect();

    let result = v.par_iter()
            .map(|x| *x as u64)
            .adaptive(10000)
            .reduce(|| 0, |a, b| a + b);
    println!("Result: {:?}", result);
}
