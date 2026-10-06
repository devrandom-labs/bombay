use behavior::{Behavior, Never};
use bombay_engine::{Driver, Environment};

fn inspect<B, E>(driver: Driver<B, E>)
where
    B: Behavior<Ph = Never>,
    E: Environment<B>,
{
    let _ = driver.prepare();
    let _ = driver.run_init();
    let _ = driver.run_loop();
    let _ = driver.retire();
    let _ = driver.recover();
    let _ = driver.reset();
    let _ = driver.restart();
    let _ = driver.reuse();
    let _ = driver.clear_poison();
}

fn main() {}
