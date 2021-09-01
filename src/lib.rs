// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use odbc_api as odbc;
pub use arrow;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
