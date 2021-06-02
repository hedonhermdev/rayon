use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..=10_000_00).collect();

    let result: Vec<u32> = v
        .par_iter()
        .adaptive(10)
        .map(|x| x+1)
        .collect();
    
    let v1: Vec<u32> = (1..=10_000_01).collect();

    assert_eq!(result, v1);
}
