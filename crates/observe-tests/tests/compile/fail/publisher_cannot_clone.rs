use observe::pair;

fn main() {
    let (publisher, _) = pair::<u64>();
    let duplicate = publisher.clone();
    drop(duplicate);
}
