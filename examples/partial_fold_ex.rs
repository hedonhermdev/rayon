use rayon::prelude::*;

fn main() {
    let v: Vec<u32> = (0..=10_000_000).collect();

    for _ in 1..100 {
        // let tp = rayon::ThreadPoolBuilder::new()
        //     .num_threads(2)
        //     .build()
        //     .expect("Could not build thread pool");
        // println!("{}", tp.current_num_threads());

        let result = 
            v.par_iter()
                .map(|x| *x as u64)
                .adaptive(10000)
                .reduce(|| 0, |a, b| a + b);
        println!("Result: {:?}", result);
    }
}
