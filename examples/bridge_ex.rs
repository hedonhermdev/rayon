use rayon::prelude::*;

fn main() {
    let v: Vec<i128> = (0..=1_000_000).collect();
    let result = v
        .iter()
        .new_par_bridge(10_000)
        .map(|x| *x)
        .reduce(|| 0, |a, b| a + b);
    println!("{}", result);
}
