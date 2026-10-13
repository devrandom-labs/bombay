bombay::topology! {
    services {
        root: Root {}
        members { Primary(Primary) => node; }
        node { Next(Next) => node; }
    }
}
fn main() {}
