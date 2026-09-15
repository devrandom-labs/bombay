#![allow(unused_imports)]

use bombay::prelude::*;

struct Actor;

#[bombay::actor(policy = Never)]
impl Actor {
    fn receive(&mut self, message: Never) -> BehaviorActed<Self> {
        match message {}
    }
}

fn main() {}
