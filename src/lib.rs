//! Fill Apache Arrow arrays from ODBC data sources.
//!
//! ## Usage
//!
//! ```no_run
//! use arrow_odbc::{odbc_api::Environment, OdbcReader};
//!
//! const CONNECTION_STRING: &str = "\
//!     Driver={ODBC Driver 17 for SQL Server};\
//!     Server=localhost;\
//!     UID=SA;\
//!     PWD=My@Test@Password1;\
//! ";
//!
//! fn main() -> Result<(), anyhow::Error> {
//!     // Your application is fine if you spin up only one Environment.
//!     let odbc_environment = Environment::new()?;
//!     
//!     // Connect with database.
//!     let connection = odbc_environment.connect_with_connection_string(CONNECTION_STRING)?;
//!
//!     // This SQL statement does not require any arguments.
//!     let parameters = ();
//!
//!     // Execute query and create result set
//!     let cursor = connection
//!         .execute("SELECT * FROM MyTable", parameters)?
//!         .expect("SELECT statement must produce a cursor");
//!
//!     // Each batch shall only consist of maximum 10.000 rows.
//!     let max_batch_size = 10_000;
//!
//!     // Read result set as arrow batches. Infer Arrow types automatically using the meta
//!     // information of `cursor`.
//!     let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)?;
//!
//!     for batch in arrow_record_batches {
//!         // ... process batch ...
//!     }
//!
//!     Ok(())
//! }
//!
//!
//!
//! ```
mod schema;

use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use column_strategy::{choose_column_strategy, ColumnStrategy};
use odbc_api::{
    buffers::{buffer_from_description, AnyColumnBuffer, ColumnarBuffer},
    Cursor, RowSetCursor,
};
use thiserror::Error;

mod column_strategy;
mod error;

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow;
pub use odbc_api;

pub use self::{column_strategy::ColumnFailure, error::Error, schema::arrow_schema_from};

/// Arrow ODBC reader. Implements the [`arrow::record_batch::RecordBatchReader`] trait so it can be
/// used to fill Arrow arrays from an ODBC data source.
///
/// This reader is generic over the cursor type so it can be used in cases there the cursor only
/// borrows a statement handle (most likely the case then using prepared queries), or owned
/// statement handles (recommened then using one shot queries, to have an easier life with the
/// borrow checker).
///
/// # Example
///
/// ```no_run
/// use arrow_odbc::{odbc_api::Environment, OdbcReader};
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
///     let odbc_environment = Environment::new()?;
///     
///     // Connect with database.
///     let connection = odbc_environment.connect_with_connection_string(CONNECTION_STRING)?;
///
///     // This SQL statement does not require any arguments.
///     let parameters = ();
///
///     // Execute query and create result set
///     let cursor = connection
///         .execute("SELECT * FROM MyTable", parameters)?
///         .expect("SELECT statement must produce a cursor");
///
///     // Each batch shall only consist of maximum 10.000 rows.
///     let max_batch_size = 10_000;
///
///     // Read result set as arrow batches. Infer Arrow types automatically using the meta
///     // information of `cursor`.
///     let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)?;
///
///     for batch in arrow_record_batches {
///         // ... process batch ...
///     }
///     Ok(())
/// }
/// ```
pub struct OdbcReader<C: Cursor> {
    /// Must contain one item for each field in [`Self::schema`]. Encapsulates all the column type
    /// specific decisions which go into filling an Arrow array from an ODBC data source.
    column_strategies: Vec<Box<dyn ColumnStrategy>>,
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source. One column buffer must be bound for each element in column_strategies.
    cursor: RowSetCursor<C, ColumnarBuffer<AnyColumnBuffer>>,
}

impl<C: Cursor> OdbcReader<C> {
    /// Construct a new `OdbcReader` instance. This constructor infers the Arrow schema from the
    /// metadata of the cursor. If you want to set it explicitly use [`Self::with_arrow_schema`].
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
        // Get number of columns from result set. We know it to contain at least one column,
        // otherwise it would not have been created.
        let schema = Arc::new(arrow_schema_from(&cursor)?);
        Self::with_arrow_schema(cursor, max_batch_size, schema)
    }

    /// Construct a new `OdbcReader instance.
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
        let column_strategies: Vec<Box<dyn ColumnStrategy>> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let col_index = (index + 1).try_into().unwrap();
                let lazy_sql_data_type = || cursor.col_data_type(col_index);
                let lazy_display_size = || cursor.col_display_size(col_index);
                choose_column_strategy(field, lazy_sql_data_type, lazy_display_size)
                    .map_err(|cause| cause.into_crate_error(field.name().clone(), index))
            })
            .collect::<Result<_, _>>()?;

        let row_set_buffer = buffer_from_description(
            max_batch_size,
            column_strategies.iter().map(|cs| cs.buffer_description()),
        )
        .map_err(|source| match source {
            odbc_api::Error::FailedSettingConnectionPooling
            | odbc_api::Error::FailedAllocatingEnvironment
            | odbc_api::Error::NoDiagnostics { .. }
            | odbc_api::Error::Diagnostics { .. }
            | odbc_api::Error::AbortedConnectionStringCompletion
            | odbc_api::Error::UnsupportedOdbcApiVersion(_)
            | odbc_api::Error::FailedReadingInput(_)
            | odbc_api::Error::InvalidRowArraySize { .. }
            | odbc_api::Error::OracleOdbcDriverDoesNotSupport64Bit(_) => {
                panic!("Unexpected error in upstream ODBC api error library")
            }
            odbc_api::Error::TooLargeColumnBufferSize {
                buffer_index,
                num_elements,
                element_size,
            } => Error::ColumnFailure {
                name: schema.field(buffer_index as usize).name().clone(),
                index: buffer_index as usize,
                source: ColumnFailure::TooLarge {
                    num_elements,
                    element_size,
                },
            },
        })?;
        let cursor = cursor.bind_buffer(row_set_buffer).unwrap();

        Ok(Self {
            column_strategies,
            schema,
            cursor,
        })
    }
}

impl<C> Iterator for OdbcReader<C>
where
    C: Cursor,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.fetch() {
            // We successfully fetched a batch from the database. Try to copy it into a record batch
            // and forward errors if any.
            Ok(Some(batch)) => {
                let columns = odbc_batch_to_arrow_columns(&self.column_strategies, batch);
                let arrow_batch = RecordBatch::try_new(self.schema.clone(), columns).unwrap();
                Some(Ok(arrow_batch))
            }
            // We ran out of batches in the result set. End the iterator.
            Ok(None) => None,
            // We had an error fetching the next batch from the database, let's report it as an
            // external error.
            Err(odbc_error) => Some(Err(ArrowError::ExternalError(Box::new(odbc_error)))),
        }
    }
}

impl<C> RecordBatchReader for OdbcReader<C>
where
    C: Cursor,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn odbc_batch_to_arrow_columns(
    column_strategies: &[Box<dyn ColumnStrategy>],
    batch: &ColumnarBuffer<AnyColumnBuffer>,
) -> Vec<ArrayRef> {
    column_strategies
        .iter()
        .enumerate()
        .map(|(index, strat)| {
            let column_view = batch.column(index);
            strat.fill_arrow_array(column_view)
        })
        .collect()
}
