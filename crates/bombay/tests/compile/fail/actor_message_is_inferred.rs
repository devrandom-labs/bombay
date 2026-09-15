#![allow(unused_imports)]

use bombay::prelude::*;

struct Actor;

#[bombay::actor(message = u8)]
impl Actor {
    fn receive(&mut self, _: u8) -> BehaviorActed<Self> {
        Ok(Actions::cont())
    }
}

fn main() {}
