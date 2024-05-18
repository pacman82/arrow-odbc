/// Quirks of databases and their drivers we can take into account exporting data from database into
/// Arrow arrays.
#[derive(Clone)]
pub struct Quirks {}

impl Quirks {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Quirks {
    fn default() -> Self {
        Quirks::new()
    }
}
