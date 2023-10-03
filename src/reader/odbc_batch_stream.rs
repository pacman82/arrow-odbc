use odbc_api::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor};

/// Fetches values from the ODBC datasource using columnar batches. Values are streamed batch by
/// batch in order to avoid reallocation of the buffers used for tranistion.
pub trait OdbcBatchStream {
    type Cursor;

    fn next(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error>;
}

impl<C> OdbcBatchStream for BlockCursor<C, ColumnarAnyBuffer>
where
    C: Cursor,
{
    type Cursor = C;

    fn next(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error> {
        self.fetch_with_truncation_check(true)
    }
}
