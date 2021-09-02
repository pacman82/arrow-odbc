use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Builder},
    datatypes::{DataType as ArrowDataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use odbc_api::{
    buffers::{AnyColumnView, ColumnarRowSet, Item},
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
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    pub schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source.
    pub cursor: RowSetCursor<C, ColumnarRowSet>,
}

impl<C: Cursor> OdbcReader<C> {
    fn arrow_to_odbc_batch(
        batch: &ColumnarRowSet,
        schema: SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let column_view = batch.column(index);
                Self::odbc_to_arrow_column(column_view, field)
            })
            .collect();
        RecordBatch::try_new(schema, columns)
    }

    fn odbc_to_arrow_column(column_view: AnyColumnView, field: &Field) -> ArrayRef {
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
                    let slice = f64::as_slice(column_view).unwrap();
                    let mut builder = Float64Builder::new(slice.len());
                    builder.append_slice(&slice).unwrap();
                    Arc::new(builder.finish())
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
            Ok(Some(batch)) => Some(Self::arrow_to_odbc_batch(batch, self.schema.clone())),
            // We ran out of batches in the result set. End the iterator.
            Ok(None) => None,
            // We had an error fetching the next batch from the database, let's report it as an
            // external error.
            Err(odbc_error) => Some(Err(ArrowError::ExternalError(Box::new(odbc_error)))),
        }
    }
}
