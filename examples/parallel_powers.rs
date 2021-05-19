use rayon::prelude::*;
use rayon::iter::parallel_powers;


fn main() {
    let powers: Vec<u32> = parallel_powers(20)
        .collect();

    println!("{:?}", powers);
    assert_eq!(
        powers, 
        vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288]);
}
