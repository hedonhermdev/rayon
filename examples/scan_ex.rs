use rayon::prelude::*;

fn main() {

    let nums: Vec<String> = "abcdefghijklmnopqrstuvwxyzabcdefghi".chars().map(|x| x.to_string()).collect();

    let psums: Vec<String> = nums.par_iter()
        .scan(String::new , |acc, x| {
            acc.push_str(x);
            
            acc.clone()
        }).collect();

    println!("{:?}", psums);
}
