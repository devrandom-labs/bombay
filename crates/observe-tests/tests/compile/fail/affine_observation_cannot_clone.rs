use observe::affine_pair;

fn main() {
    let (_, observation) = affine_pair::<u64>();
    let duplicate = observation.clone();
    drop(duplicate);
}
