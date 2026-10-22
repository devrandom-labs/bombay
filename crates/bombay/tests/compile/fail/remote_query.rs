use bombay::remote::Export;

fn inspect<Inputs, Declarations, Entry>(export: &Export<Inputs, Declarations, Entry>) {
    let _query = &export.active;
}

fn main() {}
