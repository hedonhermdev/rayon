use rayon::prelude::*;

fn main() {
    let v: Vec<i128> = (0..=10_000_000).collect();

    for _ in 0..1000 {
        let result = v
            .iter()
            .new_par_bridge(10_000)
            .map(|x| *x)
            .reduce(|| 0, |a, b| a + b);
        assert_eq!(50000005000000, result);
        println!("{}", result);
    }
}
