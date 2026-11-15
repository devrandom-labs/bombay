use bombay::remote::Export;

async fn repeat<Inputs, Declarations, Entry>(mut export: Export<Inputs, Declarations, Entry>) {
    let withdraw = export.take_withdrawal().unwrap();
    let first = withdraw().await;
    let second = withdraw().await;
    drop((first, second));
}

fn main() {}
