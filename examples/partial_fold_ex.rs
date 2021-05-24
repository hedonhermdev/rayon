use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..10).collect();

    let result = v.par_iter().map(|x| *x).logged_reduce(|| 0, |a, b| a + b, 5);

    println!("Result: {}", result);
}
