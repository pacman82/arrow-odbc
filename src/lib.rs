use std::{convert::TryInto, marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanBuilder, PrimitiveBuilder},
    datatypes::{
        ArrowPrimitiveType, DataType as ArrowDataType, Field, Float32Type, Float64Type, Int16Type,
        Int32Type, Int64Type, Int8Type, Schema, SchemaRef, UInt8Type,
    },
    error::ArrowError,
    record_batch::RecordBatch,
};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarRowSet, Item},
    Bit, ColumnDescription, Cursor, DataType as OdbcDataType, RowSetCursor,
};

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow;
pub use odbc_api;

/// Arrow ODBC reader. Implements the [`arrow::record_batch::RecordBatchReader`] trait so it can be
/// used to fill Arrow arrays from an ODBC data source.
///
/// This reader is generic over the cursor type so it can be used in cases there the cursor only
/// borrows a statement handle (most likely the case then using prepared queries), or owned
/// statement handles (recommened then using one shot queries, to have an easier life with the
/// borrow checker).
pub struct OdbcReader<C: Cursor> {
    /// Must contain one item for each field in [`Self::schema`]. Encapsulates all the column type
    /// specific decisions which go into filling an Arrow array from an ODBC data source.
    column_strategies: Vec<Box<dyn ColumnStrategy>>,
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    schema: SchemaRef,
    /// Odbc cursor with a bound buffer we repeatedly fill with the batches send to us by the data
    /// source. One column buffer must be bound for each element in column_strategies.
    cursor: RowSetCursor<C, ColumnarRowSet>,
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
    pub fn new(cursor: C, max_batch_size: usize) -> Result<Self, odbc_api::Error> {
        // Get number of columns from result set. We know it to contain at least one column,
        // otherwise it would not have been created.
        let num_cols: u16 = cursor.num_result_cols()?.try_into().unwrap();
        let mut fields = Vec::new();

        for index in 0..num_cols {
            let mut column_description = ColumnDescription::default();
            cursor.describe_col(index + 1, &mut column_description)?;

            let field = Field::new(
                &column_description
                    .name_to_string()
                    .expect("Column name must be representable in utf8"),
                match column_description.data_type {
                    OdbcDataType::Unknown => todo!(),
                    OdbcDataType::Char { length: _ } => todo!(),
                    OdbcDataType::WChar { length: _ } => todo!(),
                    OdbcDataType::Numeric {
                        precision: _,
                        scale: _,
                    } => todo!(),
                    OdbcDataType::Decimal {
                        precision: _,
                        scale: _,
                    } => todo!(),
                    OdbcDataType::Integer => ArrowDataType::Int32,
                    OdbcDataType::SmallInt => ArrowDataType::Int16,
                    OdbcDataType::Real | OdbcDataType::Float { precision: 0..=24 } => {
                        ArrowDataType::Float32
                    }
                    OdbcDataType::Float { precision: _ } | OdbcDataType::Double => {
                        ArrowDataType::Float64
                    }
                    OdbcDataType::Varchar { length: _ } => todo!(),
                    OdbcDataType::WVarchar { length: _ } => todo!(),
                    OdbcDataType::LongVarchar { length: _ } => todo!(),
                    OdbcDataType::LongVarbinary { length: _ } => todo!(),
                    OdbcDataType::Date => todo!(),
                    OdbcDataType::Time { precision: _ } => todo!(),
                    OdbcDataType::Timestamp { precision: _ } => todo!(),
                    OdbcDataType::BigInt => ArrowDataType::Int64,
                    OdbcDataType::TinyInt => ArrowDataType::Int8,
                    OdbcDataType::Bit => ArrowDataType::Boolean,
                    OdbcDataType::Varbinary { length: _ } => todo!(),
                    OdbcDataType::Binary { length: _ } => todo!(),
                    OdbcDataType::Other {
                        data_type: _,
                        column_size: _,
                        decimal_digits: _,
                    } => todo!(),
                },
                column_description.could_be_nullable(),
            );

            fields.push(field)
        }

        let schema = Arc::new(Schema::new(fields));
        let odbc_reader = Self::with_arrow_schema(cursor, max_batch_size, schema);
        Ok(odbc_reader)
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
    pub fn with_arrow_schema(cursor: C, max_batch_size: usize, schema: SchemaRef) -> Self {
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

fn choose_column_strategy(field: &Field) -> Box<dyn ColumnStrategy> {
    match field.data_type() {
        ArrowDataType::Null => todo!(),
        ArrowDataType::Boolean => {
            if field.is_nullable() {
                Box::new(NullableBoolean)
            } else {
                Box::new(NonNullableBoolean)
            }
        }
        ArrowDataType::Int8 => primitive_arrow_type_startegy::<Int8Type>(field.is_nullable()),
        ArrowDataType::Int16 => primitive_arrow_type_startegy::<Int16Type>(field.is_nullable()),
        ArrowDataType::Int32 => primitive_arrow_type_startegy::<Int32Type>(field.is_nullable()),
        ArrowDataType::Int64 => primitive_arrow_type_startegy::<Int64Type>(field.is_nullable()),
        ArrowDataType::UInt8 => primitive_arrow_type_startegy::<UInt8Type>(field.is_nullable()),
        ArrowDataType::UInt16 => todo!(),
        ArrowDataType::UInt32 => todo!(),
        ArrowDataType::UInt64 => todo!(),
        ArrowDataType::Float16 => todo!(),
        ArrowDataType::Float32 => primitive_arrow_type_startegy::<Float32Type>(field.is_nullable()),
        ArrowDataType::Float64 => primitive_arrow_type_startegy::<Float64Type>(field.is_nullable()),
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

fn primitive_arrow_type_startegy<T>(nullable: bool) -> Box<dyn ColumnStrategy>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    if nullable {
        Box::new(NullableDirectStrategy::<T>::new())
    } else {
        Box::new(NonNullDirectStrategy::<T>::new())
    }
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

struct NullableDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NullableDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ColumnStrategy for NullableDirectStrategy<T>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: T::Native::BUFFER_KIND,
            nullable: true,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = T::Native::as_nullable_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::new(1);
        for value in values {
            builder.append_option(value.copied()).unwrap();
        }
        Arc::new(builder.finish())
    }
}

struct NonNullableBoolean;

impl ColumnStrategy for NonNullableBoolean {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Bit,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = Bit::as_slice(column_view).unwrap();
        let mut builder = BooleanBuilder::new(values.len());
        for bit in values {
            builder.append_value(bit.as_bool()).unwrap();
        }
        Arc::new(builder.finish())
    }
}

struct NullableBoolean;

impl ColumnStrategy for NullableBoolean {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Bit,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = Bit::as_nullable_slice(column_view).unwrap();
        let mut builder = BooleanBuilder::new(1);
        for bit in values {
            builder
                .append_option(bit.copied().map(Bit::as_bool))
                .unwrap()
        }
        Arc::new(builder.finish())
    }
}
