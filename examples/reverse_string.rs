use rayon::prelude::*;

fn main() {
    let powers: String = "abcdefg".par_chars()
        .map(|x| x.to_string())
        .rev_reduce(String::new, |mut a, b| {a.push_str(&b); a});

    println!("{:?}", powers);
}
