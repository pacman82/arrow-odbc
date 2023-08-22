use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use odbc_api::{
    buffers::{AnyBuffer, ColumnarAnyBuffer, ColumnarBuffer},
    BlockCursor, Cursor,
};
use thiserror::Error;

use crate::{
    arrow_schema_from,
    read_strategy::{choose_column_strategy, ReadStrategy},
    BufferAllocationOptions, ColumnFailure, Error,
};

/// The source value returned from the ODBC datasource is out of range and can not be mapped into
/// its Arrow target type.
#[derive(Error, Debug)]
enum MappingError {}

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
/// use arrow_odbc::{odbc_api::{Environment, ConnectionOptions}, OdbcReader};
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
    column_strategies: Vec<Box<dyn ReadStrategy>>,
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source. One column buffer must be bound for each element in column_strategies.
    cursor: BlockCursor<C, ColumnarBuffer<AnyBuffer>>,
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
    pub fn new(mut cursor: C, max_batch_size: usize) -> Result<Self, Error> {
        // Get number of columns from result set. We know it to contain at least one column,
        // otherwise it would not have been created.
        let schema = Arc::new(arrow_schema_from(&mut cursor)?);
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
        Self::with(
            cursor,
            max_batch_size,
            Some(schema),
            BufferAllocationOptions::default(),
        )
    }

    /// Construct a new [`crate::OdbcReader`] instance. This method allows you full control over
    /// what options to explicitly specify, and what options you want to leave to this crate to
    /// automatically decide.
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
        mut cursor: C,
        max_batch_size: usize,
        schema: Option<SchemaRef>,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Self, Error> {
        // Infer schema if not given by the user
        let schema = if let Some(schema) = schema {
            schema
        } else {
            Arc::new(arrow_schema_from(&mut cursor)?)
        };

        let column_strategies: Vec<Box<dyn ReadStrategy>> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let col_index = (index + 1).try_into().unwrap();
                choose_column_strategy(field, &mut cursor, col_index, buffer_allocation_options)
                    .map_err(|cause| cause.into_crate_error(field.name().clone(), index))
            })
            .collect::<Result<_, _>>()?;

        let descs = column_strategies.iter().map(|cs| cs.buffer_desc());

        let row_set_buffer = if buffer_allocation_options.fallibale_allocations {
            ColumnarAnyBuffer::try_from_descs(max_batch_size, descs)
                .map_err(|err| map_allocation_error(err, &schema))?
        } else {
            ColumnarAnyBuffer::from_descs(max_batch_size, descs)
        };
        let cursor = cursor.bind_buffer(row_set_buffer).unwrap();

        Ok(Self {
            column_strategies,
            schema,
            cursor,
        })
    }

    /// Destroy the ODBC arrow reader and yield the underlyinng cursor object.
    ///
    /// One application of this is to process more than one result set in case you executed a stored
    /// procedure.
    pub fn into_cursor(self) -> Result<C, odbc_api::Error> {
        let (cursor, _buffer) = self.cursor.unbind()?;
        Ok(cursor)
    }
}

fn map_allocation_error(error: odbc_api::Error, schema: &Schema) -> Error {
    match error {
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
        _ => {
            panic!("Unexpected error in upstream ODBC api error library")
        }
    }
}

impl<C> Iterator for OdbcReader<C>
where
    C: Cursor,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.fetch_with_truncation_check(true) {
            // We successfully fetched a batch from the database. Try to copy it into a record batch
            // and forward errors if any.
            Ok(Some(batch)) => {
                let result_columns = odbc_batch_to_arrow_columns(&self.column_strategies, batch);
                // Fetching the but has been succesful, but could we convert all the values returned
                // by the database into their respective arrow data types?
                match result_columns {
                    Ok(columns) => {
                        let arrow_batch =
                            RecordBatch::try_new(self.schema.clone(), columns).unwrap();
                        Some(Ok(arrow_batch))
                    }
                    Err(err) => Some(Err(ArrowError::ExternalError(Box::new(err)))),
                }
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
    column_strategies: &[Box<dyn ReadStrategy>],
    batch: &ColumnarBuffer<AnyBuffer>,
) -> Result<Vec<ArrayRef>, MappingError> {
    let arrow_columns = column_strategies
        .iter()
        .enumerate()
        .map(|(index, strat)| {
            let column_view = batch.column(index);
            strat.fill_arrow_array(column_view)
        })
        .collect();
    Ok(arrow_columns)
}
