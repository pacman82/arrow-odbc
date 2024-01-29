/// Quirks of databases and their drivers we can take into account exporting data from database into
/// Arrow arrays.
#[derive(Clone)]
pub struct Quirks {
    /// IBM DB2 has been observered that the length indicators returned from memory are garbage for
    /// strings. It seems to be preferable to rely on the terminating zero exclusively to determine
    /// string length. This behavior seems to so far only manifest with variadic string fields.
    /// See: <https://github.com/pacman82/arrow-odbc-py/issues/68> and also
    /// <https://github.com/pacman82/odbc-api/issues/398>
    pub indicators_returned_from_bulk_fetch_are_memory_garbage: bool,
}

impl Quirks {
    pub fn new() -> Self {
        Self {
            indicators_returned_from_bulk_fetch_are_memory_garbage: false,
        }
    }
}

impl Default for Quirks {
    fn default() -> Self {
        Quirks::new()
    }
}
