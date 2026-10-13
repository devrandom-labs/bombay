bombay::topology! {
    application {
        root { Node => node; }
        node { Child => node; }
    }
}
