use arrow::{array::RecordBatch, error::ArrowError};
use async_stream::try_stream;
use futures_core::Stream;
use odbc_api::{
    buffers::ColumnarAnyBuffer, handles::AsStatementRef, BlockCursorPolling, CursorPolling, Sleep,
};

use crate::Error;

use super::{odbc_reader::odbc_to_arrow_error, to_record_batch::ToRecordBatch};

/// Async Arrow ODBC reader.
pub struct AsyncBatchedOdbcReader<S: AsStatementRef> {
    /// Converts the content of ODBC buffers into Arrow record batches
    converter: ToRecordBatch,
    /// Fetches values from the ODBC datasource using columnar batches. Values are streamed batch
    /// by batch in order to avoid reallocation of the buffers used for tranistion.
    batch_stream: BlockCursorPolling<CursorPolling<S>, ColumnarAnyBuffer>,
}

impl<S: AsStatementRef> AsyncBatchedOdbcReader<S> {
    /// The schema implied by `block_cursor` and `converter` must match. Invariant is hard to check
    /// in type system, keep this constructor private to this crate.
    pub(crate) fn from_cursor_polling(
        cursor_polling: CursorPolling<S>,
        converter: ToRecordBatch,
        fallibale_allocations: bool,
        max_batch_size: usize,
    ) -> Result<Self, Error> {
        let buffer = converter.allocate_buffer(max_batch_size, fallibale_allocations)?;
        let batch_stream = cursor_polling.bind_buffer(buffer).unwrap();

        Ok(Self {
            converter,
            batch_stream,
        })
    }
}

impl<S> AsyncBatchedOdbcReader<S>
where
    S: AsStatementRef,
{
    pub fn into_stream<S2>(
        mut self,
        sleep: impl Fn() -> S2,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>>
    where
        S2: Sleep,
    {
        try_stream! {
            loop {
                match self.batch_stream.fetch(sleep()).await {
                    Ok(Some(batch)) => {
                        let result_record_batch = self
                            .converter
                            .buffer_to_record_batch(batch)
                            .map_err(|mapping_error| ArrowError::ExternalError(Box::new(mapping_error)));
                        yield result_record_batch?;
                    }
                    Ok(None) =>  {
                        break;
                    }
                    Err(odbc_error) => Err(odbc_to_arrow_error(odbc_error))?,
                }
            }
        }
    }

    pub fn as_stream<'a, S2>(
        &'a mut self,
        sleep: impl Fn() -> S2 + 'a,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> + 'a
    where
        S2: Sleep,
    {
        try_stream! {
            loop {
                match self.batch_stream.fetch(sleep()).await {
                    Ok(Some(batch)) => {
                        let result_record_batch = self
                            .converter
                            .buffer_to_record_batch(batch)
                            .map_err(|mapping_error| ArrowError::ExternalError(Box::new(mapping_error)));
                        yield result_record_batch?;
                    }
                    Ok(None) =>  {
                        break;
                    }
                    Err(odbc_error) => Err(odbc_to_arrow_error(odbc_error))?,
                }
            }
        }
    }
}
