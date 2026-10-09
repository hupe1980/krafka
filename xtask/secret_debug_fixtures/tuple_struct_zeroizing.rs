// Planted violation: a tuple struct wrapping `Zeroizing`, whose `Debug`
// prints the inner value.
#[derive(Debug)]
pub struct Wrapped(zeroize::Zeroizing<String>);
