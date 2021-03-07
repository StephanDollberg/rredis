mod reactor;
mod redis_handler;
mod types;
mod reusable_slab_allocator;

use crate::reactor::Reactor;

fn main() -> anyhow::Result<()> {
    let mut reac = Reactor::new().unwrap();
    return reac.run();
}
