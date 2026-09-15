use bombay_engine::Driver;

fn main() {
    fn inspect<B: behavior::Behavior, E>(driver: Driver<B, E>) {
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
}
