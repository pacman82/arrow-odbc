use std::{
    mem::swap,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use odbc_api::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor};

use crate::{BufferAllocationOptions, Error, OdbcReader};

use super::{
    odbc_batch_stream::OdbcBatchStream,
    odbc_reader::{next, OdbcReaderBuilder},
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
/// use arrow_odbc::{odbc_api::{Environment, ConnectionOptions}, OdbcReader};
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
///     // Construct ODBC reader ...
///     let max_batch_size = 1000;
///     let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)
///         // ... and make it concurrent
///         .and_then(OdbcReader::into_concurrent)?;
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
    #[deprecated(since = "2.2.0", note = "use OdbcReader::into_concurrent instead")]
    /// This constructor infers the Arrow schema from the metadata of the cursor. If you want to set
    /// it explicitly use [`Self::with_arrow_schema`].
    ///
    /// # Parameters
    ///
    /// * `cursor`: ODBC cursor used to fetch batches from the data source. The constructor will
    ///   bind buffers to this cursor in order to perform bulk fetches from the source. This is
    ///   usually faster than fetching results row by row as it saves roundtrips to the database.
    ///   The type of these buffers will be inferred from the arrow schema. Not every arrow type is
    ///   supported though.
    /// * `max_batch_size`: Maximum batch size requested from the datasource.
    pub fn new(cursor: C, max_batch_size: usize) -> Result<Self, Error> {
        OdbcReaderBuilder::new()
            .set_max_num_rows_per_batch(max_batch_size)
            .build(cursor)
            .and_then(OdbcReader::into_concurrent)
    }

    #[deprecated(since = "2.2.0", note = "use OdbcReader::into_concurrent instead")]
    /// Construct a new [`crate::ConcurrentOdbcReader`] instance.
    ///
    /// # Parameters
    ///
    /// * `cursor`: ODBC cursor used to fetch batches from the data source. The constructor will
    ///   bind buffers to this cursor in order to perform bulk fetches from the source. This is
    ///   usually faster than fetching results row by row as it saves roundtrips to the database.
    ///   The type of these buffers will be inferred from the arrow schema. Not every arrow type is
    ///   supported though.
    /// * `max_batch_size`: Maximum batch size requested from the datasource.
    /// * `schema`: Arrow schema. Describes the type of the Arrow Arrays in the record batches, but
    ///    is also used to determine CData type requested from the data source.
    pub fn with_arrow_schema(
        cursor: C,
        max_batch_size: usize,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        OdbcReader::with_arrow_schema(cursor, max_batch_size, schema)
            .and_then(OdbcReader::into_concurrent)
    }

    #[deprecated(since = "2.2.0", note = "use OdbcReader::into_concurrent instead")]
    /// Construct a new [`crate::ConcurrentOdbcReader`] instance. This method allows you full
    /// control over what options to explicitly specify, and what options you want to leave to this
    /// crate to automatically decide.
    ///
    /// # Parameters
    ///
    /// * `cursor`: ODBC cursor used to fetch batches from the data source. The constructor will
    ///   bind buffers to this cursor in order to perform bulk fetches from the source. This is
    ///   usually faster than fetching results row by row as it saves roundtrips to the database.
    ///   The type of these buffers will be inferred from the arrow schema. Not every arrow type is
    ///   supported though.
    /// * `max_batch_size`: Maximum batch size requested from the datasource.
    /// * `schema`: Arrow schema. Describes the type of the Arrow Arrays in the record batches, but
    ///    is also used to determine CData type requested from the data source. Set to `None` to
    ///    infer schema from the data source.
    /// * `buffer_allocation_options`: Allows you to specify upper limits for binary and / or text
    ///    buffer types. This is useful support fetching data from e.g. VARCHAR(max) or
    ///    VARBINARY(max) columns, which otherwise might lead to errors, due to the ODBC driver
    ///    having a hard time specifying a good upper bound for the largest possible expected value.
    pub fn with(
        cursor: C,
        max_batch_size: usize,
        schema: Option<SchemaRef>,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Self, Error> {
        OdbcReader::with(cursor, max_batch_size, schema, buffer_allocation_options)
            .and_then(OdbcReader::into_concurrent)
    }

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

pub struct ConcurrentBlockCursor<C> {
    /// We currently only borrow these buffers to the converter, so we take ownership of them here.
    buffer: ColumnarAnyBuffer,
    /// In order to avoid reallocating buffers over and over again, we use this channel to send the
    /// buffers back to the fetch thread after we copied their contents into arrow arrays.
    send_buffer: SyncSender<ColumnarAnyBuffer>,
    /// Receives filled batches from the fetch thread. Once the source is empty or if an error
    /// occurs its associated sender is dropped, and receiving batches will return an error (which
    /// we expect during normal operation and cleanup, and is not forwarded to the user).
    receive_batch: Receiver<ColumnarAnyBuffer>,
    /// We join with the fetch thread if we stop receiving batches (i.e. receive_batch.recv()
    /// returns an error) or `into_cursor` is called. `None` if the thread has already been joined.
    /// In this case either an error has been reported to the user, or the cursor is stored in
    /// `cursor`.
    fetch_thread: Option<JoinHandle<Result<C, odbc_api::Error>>>,
    /// Only `Some`, if the cursor has been consumed succesfully and `fetch_thread` has been joined.
    /// Can only be `Some` if `fetch_thread` is `None`. If both `fetch_thread` and `cursor` are
    /// `None`, it is implied that `fetch_thread` returned an error joining.
    cursor: Option<C>,
}

impl<C> ConcurrentBlockCursor<C>
where
    C: Cursor + Send + 'static,
{
    /// Construct a new concurrent block cursor.
    ///
    /// # Parameters
    ///
    /// * `block_cursor`: Taking a BlockCursor instead of a Cursor allows for better resource
    ///   stealing if constructing starting from a sequential Cursor, as we do not need to undbind
    ///   and bind the cursor.
    /// * `lazy_buffer`: Constructor for a buffer holding the fetched row group. We want to
    ///   construct it lazy, so we can delay its allocation until after the fetch thread has started
    ///   and we can start fetching the first row group concurrently as earlier.
    pub fn new(
        block_cursor: BlockCursor<C, ColumnarAnyBuffer>,
        lazy_buffer: impl FnOnce() -> Result<ColumnarAnyBuffer, Error>,
    ) -> Result<Self, Error> {
        let (send_buffer, receive_buffer) = sync_channel(1);
        let (send_batch, receive_batch) = sync_channel(1);

        let fetch_thread = thread::spawn(move || {
            let mut block_cursor = block_cursor;
            loop {
                match block_cursor.fetch_with_truncation_check(true) {
                    Ok(Some(_batch)) => (),
                    Ok(None) => {
                        break block_cursor
                            .unbind()
                            .map(|(undbound_cursor, _buffer)| undbound_cursor);
                    }
                    Err(odbc_error) => {
                        drop(send_batch);
                        break Err(odbc_error);
                    }
                }
                // There has been another row group fetched by the cursor. We unbind the buffers so
                // we can pass ownership of it to the application and bind a new buffer to the
                // cursor in order to start fetching the next batch.
                let (cursor, buffer) = block_cursor.unbind()?;
                if send_batch.send(buffer).is_err() {
                    // Should the main thread stop receiving buffers, this thread should
                    // also stop fetching batches.
                    break Ok(cursor);
                }
                // Wait for the application thread to give us a buffer to fill.
                match receive_buffer.recv() {
                    Err(_) => {
                        // Application thread dropped sender and does not want more buffers to be
                        // filled. Let's stop this thread and return the cursor
                        break Ok(cursor);
                    }
                    Ok(next_buffer) => {
                        block_cursor = cursor.bind_buffer(next_buffer).unwrap();
                    }
                }
            }
        });

        let buffer = lazy_buffer()?;

        Ok(Self {
            buffer,
            send_buffer,
            receive_batch,
            fetch_thread: Some(fetch_thread),
            cursor: None,
        })
    }

    pub fn into_cursor(self) -> Result<C, odbc_api::Error> {
        // Fetch thread should never be blocked for a long time in receiving buffers. Yet it could
        // wait for a long time on the application logic to receive an arrow buffer using next. We
        // drop the receiver here explicitly in order to be always able to join the fetch thread,
        // even if the iterator has not been consumed to completion.
        drop(self.receive_batch);
        if let Some(cursor) = self.cursor {
            Ok(cursor)
        } else {
            self.fetch_thread.unwrap().join().unwrap()
        }
    }
}

impl<C> OdbcBatchStream for ConcurrentBlockCursor<C> {
    type Cursor = C;

    fn next(&mut self) -> Result<Option<&ColumnarAnyBuffer>, odbc_api::Error> {
        match self.receive_batch.recv() {
            // We successfully fetched a batch from the database.
            Ok(mut batch) => {
                swap(&mut self.buffer, &mut batch);
                let _ = self.send_buffer.send(batch);
                Ok(Some(&self.buffer))
            }
            // Fetch thread stopped sending batches. Either because we consumed the result set
            // completly or we hit an error.
            Err(_receive_error) => {
                if let Some(join_handle) = self.fetch_thread.take() {
                    // If there has been an error returning the batch, or unbinding the buffer `?`
                    // will raise it.
                    self.cursor = Some(join_handle.join().unwrap()?);
                    // We ran out of batches in the result set. End the stream.
                    Ok(None)
                } else {
                    // This only happen if `next` is called after it returned either a `None` or
                    // `Err` once. Let us just answer with `None`.
                    Ok(None)
                }
            }
        }
    }
}
