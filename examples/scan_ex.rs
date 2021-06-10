use rayon::prelude::*;

fn main() {
    let input = vec![0, 1, 0, 1, 1, 0, 1, 0];
    let output: Vec<u32> = input
        .par_windows(3) // hash all windows of 3 elements
        .scan(|| None, |h, w| {
            *h = h
                .map(|(first, h)| (w.first().copied().unwrap(), h ^ first ^ w.last().unwrap()))
                .or_else(|| {
                    w.iter()
                        .fold(None, |h, &e| h.map(|h| h ^ e).or(Some(e)))
                        .map(|h| (w.first().copied().unwrap(), h))
                });
            h.map(|(_, h)| h)
        })
        .collect();
    assert_eq!(output, vec![1, 0, 0, 0, 0, 1]);
}
