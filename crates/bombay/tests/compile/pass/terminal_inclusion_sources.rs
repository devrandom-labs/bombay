pub struct PlainOwner;
pub struct PlainRole;
pub type PlainOrigin = bombay::ChildOrigin<PlainOwner, PlainRole>;

#[derive(bombay::TerminalProjection)]
pub enum PlainTerminal {
    #[structural_child]
    Count { origin: PlainOrigin, terminal: u16 },
    #[structural_child]
    Text { origin: PlainOrigin, terminal: String },
}

fn main() {}
