#![allow(unused_imports)]

use bombay::prelude::*;

struct Actor;

#[bombay::actor]
impl Actor {
    fn receive(&mut self) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn main() {}
