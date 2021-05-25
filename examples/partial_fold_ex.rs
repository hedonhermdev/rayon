use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..100).collect();

    let result = v.par_iter().map(|x| *x).logged_reduce(|| 0, |a, b| a + b, 2);

    println!("Result: {}", result);
}
