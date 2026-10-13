fn main() {
    let scope = bombay::ApplicationScope::new();
    let _duplicate = scope.clone();
}
