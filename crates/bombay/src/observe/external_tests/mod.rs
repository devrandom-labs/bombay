#[cfg(not(loom))]
mod affine;
#[cfg(not(loom))]
mod future_cancel;
#[cfg(loom)]
mod loom_external;
#[cfg(not(loom))]
mod pair;
#[cfg(not(loom))]
mod panic_safety;
#[cfg(not(loom))]
mod stress;
