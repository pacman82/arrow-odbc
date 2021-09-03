use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveBuilder},
    datatypes::{ArrowPrimitiveType, DataType as ArrowDataType, Field, Float64Type, SchemaRef},
    record_batch::RecordBatch,
};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, ColumnarRowSet, Item},
    Cursor, RowSetCursor,
};

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow::{self, error::ArrowError};
pub use odbc_api;

/// Arrow ODBC reader. Implements the [`arrow::record_batch::RecordBatchReader`] trait so it can be
/// used to fill Arrow arrays from an ODBC data source.
///
/// This reader is generic over the cursor type so it can be used in cases there the cursor only
/// borrows a statement handle (most likely the case then using prepared queries), or owned
/// statement handles (recommened then using one shot queries, to have an easier life with the
/// borrow checker).
pub struct OdbcReader<C: Cursor> {
    column_strategies: Vec<Box<dyn ColumnStrategy>>,
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source.
    cursor: RowSetCursor<C, ColumnarRowSet>,
}

impl<C: Cursor> OdbcReader<C> {
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
    pub fn new(cursor: C, max_batch_size: usize, schema: SchemaRef) -> Self {
        let column_strategies: Vec<Box<dyn ColumnStrategy>> = schema
            .fields()
            .iter()
            .map(|field| choose_column_strategy(field))
            .collect();

        let row_set_buffer = ColumnarRowSet::new(
            max_batch_size,
            column_strategies.iter().map(|cs| cs.buffer_description()),
        );
        let cursor = cursor.bind_buffer(row_set_buffer).unwrap();

        Self {
            column_strategies,
            schema,
            cursor,
        }
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

trait ColumnStrategy {
    fn buffer_description(&self) -> BufferDescription;

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef;
}

struct NonNullDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NonNullDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ColumnStrategy for NonNullDirectStrategy<T>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: T::Native::BUFFER_KIND,
            nullable: false,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let slice = T::Native::as_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::new(slice.len());
        builder.append_slice(slice).unwrap();
        Arc::new(builder.finish())
    }
}

fn choose_column_strategy(field: &Field) -> Box<dyn ColumnStrategy> {
    match field.data_type() {
        ArrowDataType::Null => todo!(),
        ArrowDataType::Boolean => todo!(),
        ArrowDataType::Int8 => todo!(),
        ArrowDataType::Int16 => todo!(),
        ArrowDataType::Int32 => todo!(),
        ArrowDataType::Int64 => todo!(),
        ArrowDataType::UInt8 => todo!(),
        ArrowDataType::UInt16 => todo!(),
        ArrowDataType::UInt32 => todo!(),
        ArrowDataType::UInt64 => todo!(),
        ArrowDataType::Float16 => todo!(),
        ArrowDataType::Float32 => todo!(),
        ArrowDataType::Float64 => {
            if field.is_nullable() {
                todo!()
            } else {
                Box::new(NonNullDirectStrategy::<Float64Type>::new())
            }
        }
        ArrowDataType::Timestamp(_, _) => todo!(),
        ArrowDataType::Date32 => todo!(),
        ArrowDataType::Date64 => todo!(),
        ArrowDataType::Time32(_) => todo!(),
        ArrowDataType::Time64(_) => todo!(),
        ArrowDataType::Duration(_) => todo!(),
        ArrowDataType::Interval(_) => todo!(),
        ArrowDataType::Binary => todo!(),
        ArrowDataType::FixedSizeBinary(_) => todo!(),
        ArrowDataType::LargeBinary => todo!(),
        ArrowDataType::Utf8 => todo!(),
        ArrowDataType::LargeUtf8 => todo!(),
        ArrowDataType::List(_) => todo!(),
        ArrowDataType::FixedSizeList(_, _) => todo!(),
        ArrowDataType::LargeList(_) => todo!(),
        ArrowDataType::Struct(_) => todo!(),
        ArrowDataType::Union(_) => todo!(),
        ArrowDataType::Dictionary(_, _) => todo!(),
        ArrowDataType::Decimal(_, _) => todo!(),
    }
}

fn odbc_batch_to_arrow_columns(
    column_strategies: &[Box<dyn ColumnStrategy>],
    batch: &ColumnarRowSet,
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
