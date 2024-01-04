use std::{
    mem::swap,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use odbc_api::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor};

use crate::Error;

use super::odbc_batch_stream::OdbcBatchStream;

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
