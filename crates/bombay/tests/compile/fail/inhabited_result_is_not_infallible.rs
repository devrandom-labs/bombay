use bombay::testing::InfallibleResultExt;

struct DomainError;

fn main() {
    let result = Result::<u64, DomainError>::Ok(41);
    let _ = result.infallible();
}
