use rayon::prelude::*;

fn main() {
    let reversed: String = "abcdefg".par_chars()
        .map(|x| x.to_string())
        .rev_reduce(String::new, |mut a, b| {a.push_str(&b); a});

    assert_eq!(reversed, "gfedcba".to_owned())
}
