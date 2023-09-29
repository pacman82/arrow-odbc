use odbc_api::{BlockCursor, buffers::{ColumnarBuffer, AnyBuffer}, Cursor};

/// Fetches values from the ODBC datasource using columnar batches. Values are streamed batch by
/// batch in order to avoid reallocation of the buffers used for tranistion.
pub struct OdbcBatchStream<C: Cursor> {
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source. One column buffer must be bound for each element in column_strategies.
    pub cursor: BlockCursor<C, ColumnarBuffer<AnyBuffer>>,
}