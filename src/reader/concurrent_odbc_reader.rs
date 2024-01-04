use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use odbc_api::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor};

use crate::Error;

use super::{
    concurrent_odbc_block_cursor::ConcurrentBlockCursor, odbc_reader::next,
    to_record_batch::ToRecordBatch,
};

/// Arrow ODBC reader. Implements the [`arrow::record_batch::RecordBatchReader`] trait so it can be
/// used to fill Arrow arrays from an ODBC data source. Similar to [`crate::OdbcReader`], yet
/// [`ConcurrentOdbcReader`] fetches ODBC batches in a second transit buffer eagerly from the
/// database in a dedicated system thread. This allows the allocation of the Arrow arrays and your
/// application logic to run on the main thread, while fetching the batches from the source happens
/// concurrently. You need twice the memory for the transit buffer for this strategy, since one is
/// may be in use by the main thread in order to copy values into arrow arrays, while the other is
/// used to write values from the database.
///
/// # Example
///
/// ```no_run
/// use arrow_odbc::{odbc_api::{Environment, ConnectionOptions}, OdbcReaderBuilder};
/// use std::sync::OnceLock;
///
/// // In order to fetch in a dedicated system thread we need a cursor with static lifetime,
/// // this implies a static ODBC environment.
/// static ENV: OnceLock<Environment> = OnceLock::new();
///
/// const CONNECTION_STRING: &str = "\
///     Driver={ODBC Driver 17 for SQL Server};\
///     Server=localhost;\
///     UID=SA;\
///     PWD=My@Test@Password1;\
/// ";
///
/// fn main() -> Result<(), anyhow::Error> {
///
///     let odbc_environment = ENV.get_or_init(|| {Environment::new().unwrap() });
///     
///     // Connect with database.
///     let connection = odbc_environment.connect_with_connection_string(
///         CONNECTION_STRING,
///         ConnectionOptions::default()
///     )?;
///
///     // This SQL statement does not require any arguments.
///     let parameters = ();
///
///     // Execute query and create result set
///     let cursor = connection
///         // Using `into_cursor` instead of `execute` takes ownership of the connection and
///         // allows for a cursor with static lifetime.
///         .into_cursor("SELECT * FROM MyTable", parameters)?
///         .expect("SELECT statement must produce a cursor");
///
///     // Construct ODBC reader and make it concurrent
///     let arrow_record_batches = OdbcReaderBuilder::new().build(cursor)?.into_concurrent()?;
///
///     for batch in arrow_record_batches {
///         // ... process batch ...
///     }
///     Ok(())
/// }
/// ```
pub struct ConcurrentOdbcReader<C: Cursor> {
    /// Converts the content of ODBC buffers into Arrow record batches
    converter: ToRecordBatch,
    /// Fetches values from the ODBC datasource using columnar batches. Values are streamed batch
    /// by batch in order to avoid reallocation of the buffers used for tranistion.
    batch_stream: ConcurrentBlockCursor<C>,
}

impl<C: Cursor + Send + 'static> ConcurrentOdbcReader<C> {
    /// The schema implied by `block_cursor` and `converter` must match. Invariant is hard to check
    /// in type system, keep this constructor private to this crate. Users should use
    /// [`crate::OdbcReader::into_concurrent`] instead.
    pub(crate) fn from_block_cursor(
        block_cursor: BlockCursor<C, ColumnarAnyBuffer>,
        converter: ToRecordBatch,
        fallibale_allocations: bool,
    ) -> Result<Self, Error> {
        let max_batch_size = block_cursor.row_array_size();
        let make_buffer = || converter.allocate_buffer(max_batch_size, fallibale_allocations);
        let batch_stream = ConcurrentBlockCursor::new(block_cursor, make_buffer)?;

        Ok(Self {
            converter,
            batch_stream,
        })
    }

    /// Destroy the ODBC arrow reader and yield the underlyinng cursor object.
    ///
    /// One application of this is to process more than one result set in case you executed a stored
    /// procedure.
    ///
    /// Due to the concurrent fetching of row groups you can not know how many row groups have been
    /// extracted once the cursor is returned. Unless that is that the entire cursor has been
    /// consumed i.e. [`Self::next`] returned `None`.
    pub fn into_cursor(self) -> Result<C, odbc_api::Error> {
        self.batch_stream.into_cursor()
    }
}

impl<C> Iterator for ConcurrentOdbcReader<C>
where
    C: Cursor,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        next(&mut self.batch_stream, &mut self.converter)
    }
}

impl<C> RecordBatchReader for ConcurrentOdbcReader<C>
where
    C: Cursor,
{
    fn schema(&self) -> SchemaRef {
        self.converter.schema().clone()
    }
}
