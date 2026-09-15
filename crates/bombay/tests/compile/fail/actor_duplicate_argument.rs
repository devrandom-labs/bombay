#![allow(unused_imports)]

use bombay::prelude::*;

struct Actor;

#[bombay::actor(error = Never, error = Never)]
impl Actor {
    fn receive(&mut self, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn main() {}
