use odbc_api::{BlockCursor, buffers::{ColumnarBuffer, AnyBuffer, ColumnarAnyBuffer}, Cursor};

/// Fetches values from the ODBC datasource using columnar batches. Values are streamed batch by
/// batch in order to avoid reallocation of the buffers used for tranistion.
pub struct OdbcBatchStream<C: Cursor> {
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source. One column buffer must be bound for each element in column_strategies.
    cursor: BlockCursor<C, ColumnarBuffer<AnyBuffer>>,
}

impl<C> OdbcBatchStream<C> where C: Cursor {

    pub fn new(cursor: C, buffer: ColumnarAnyBuffer) -> Self {
        let cursor = cursor.bind_buffer(buffer).unwrap();
        OdbcBatchStream { cursor }
    }

    /// Destroy the stream and yield the underlyinng cursor object.
    pub fn into_cursor(self) -> Result<C, odbc_api::Error> {
        let (cursor, _buffer) = self.cursor.unbind()?;
        Ok(cursor)
    }

    pub fn next(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error> {
        self.cursor.fetch_with_truncation_check(true)
    }
}