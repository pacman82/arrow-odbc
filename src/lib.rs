use arrow::datatypes::SchemaRef;
// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use odbc_api;
pub use arrow;

/// Arrow ODBC reader. Implements the [`arrow::record_batch::RecordBatchReader`] trait so it can be
/// used to fill Arrow arrays from an ODBC data source.
///
/// This reader is generic over the cursor type so it can be used in cases there the cursor only
/// borrows a statement handle (most likely the case then using prepared queries), or owned
/// statement handles (recommened then using one shot queries, to have an easier life with the
/// borrow checker).
pub struct OdbcReader<C> {
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    pub schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source.
    pub cursor: C
}
