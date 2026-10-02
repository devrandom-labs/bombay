use observe::pair;

fn main() {
    let (publisher, _) = pair::<u64>();
    publisher.complete(1);
    publisher.complete(2);
}
