mod reactor;
use crate::reactor::Reactor;

fn main() -> anyhow::Result<()> {
    let mut reac = Reactor::new().unwrap();
    return reac.run();
}
