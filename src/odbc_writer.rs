use odbc_api::{buffers::AnyColumnBuffer, handles::StatementImpl, ColumnarBulkInserter};

/// Inserts batches from an [`crate::arrow::RecordBatchReader`] into a database.
pub struct OdbcWriter<'o> {
    /// Prepared statement with bound array parameter buffers. Data is copied into these buffers
    /// until they are full. Then we execute the statement. This is repeated until we run out of
    /// data.
    pub inserter: ColumnarBulkInserter<StatementImpl<'o>, AnyColumnBuffer>,
}
