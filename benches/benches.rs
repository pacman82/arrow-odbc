// Without this use divan is not able to pick up the internal benchmarks declared inline in the
// modules. Using `arrow-odbc` allows the linker shinanigans of divan to work.
use arrow_odbc as _;

fn main() {
    divan::main();
}

#[divan::bench]
fn empty() {}
