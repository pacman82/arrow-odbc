//! Fill Apache Arrow arrays from ODBC data sources.
//!
//! ## Usage
//!
//! ```no_run
//! use odbc_arrow::{odbc_api::Environment, OdbcReader};
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
//!     let odbc_environment = unsafe {
//!         Environment::new().unwrap()
//!     };
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
use std::{char::decode_utf16, convert::TryInto, marker::PhantomData, sync::Arc};

use chrono::NaiveDate;
use thiserror::Error;
use atoi::FromRadix10Signed;

use arrow::{array::{ArrayRef, BooleanBuilder, Date32Builder, DecimalBuilder, PrimitiveBuilder, StringBuilder}, datatypes::{
        ArrowPrimitiveType, DataType as ArrowDataType, Field, Float32Type, Float64Type, Int16Type,
        Int32Type, Int64Type, Int8Type, Schema, SchemaRef, UInt8Type,
    }, error::ArrowError, record_batch::{RecordBatch, RecordBatchReader}};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarRowSet, Item},
    sys::Date,
    Bit, ColumnDescription, Cursor, DataType as OdbcDataType, RowSetCursor,
};

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow;
pub use odbc_api;

#[derive(Error, Debug)]
pub enum Error {
    #[error(
        "Unsupported arrow type: `{0}`. This type can currently not be fetched from an ODBC data \
        source by an instance of OdbcReader."
    )]
    /// The type specified in the arrow schema is not supported to be fetched from the database.
    UnsupportedArrowType(ArrowDataType),
    #[error(
        "An error occurred fetching the column description or data type from the metainformation \
        attached to the ODBC result set:\n{0}"
    )]
    FailedToDescribeColumn(#[source] odbc_api::Error),
    #[error(
        "Unable to deduce the maximum string length for the SQL Data Type reported by the ODBC \
        driver. Reported SQL data type is: {:?}.\n Error fetching column display size: {source}",
        sql_type
    )]
    UnknownStringLength {
        sql_type: OdbcDataType,
        source: odbc_api::Error,
    },
    #[error("ODBC reported a display size of {0}.")]
    InvalidDisplaySize(isize),
    #[error("Unable to retrieve number of columns in result set.\n{0}")]
    UnableToRetrieveNumCols(odbc_api::Error),
}

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
    pub fn new(cursor: C, max_batch_size: usize) -> Result<Self, Error> {
        // Get number of columns from result set. We know it to contain at least one column,
        // otherwise it would not have been created.
        let num_cols: u16 = cursor
            .num_result_cols()
            .map_err(Error::UnableToRetrieveNumCols)?
            .try_into()
            .unwrap();
        let mut fields = Vec::new();

        for index in 0..num_cols {
            let mut column_description = ColumnDescription::default();
            cursor
                .describe_col(index + 1, &mut column_description)
                .map_err(Error::FailedToDescribeColumn)?;

            let field = Field::new(
                &column_description
                    .name_to_string()
                    .expect("Column name must be representable in utf8"),
                match column_description.data_type {
                    OdbcDataType::Numeric { precision: p @ 0..=38, scale }
                    | OdbcDataType::Decimal { precision: p@  0..=38, scale } => {
                        ArrowDataType::Decimal(p, scale.try_into().unwrap())
                    }
                    OdbcDataType::Integer => ArrowDataType::Int32,
                    OdbcDataType::SmallInt => ArrowDataType::Int16,
                    OdbcDataType::Real | OdbcDataType::Float { precision: 0..=24 } => {
                        ArrowDataType::Float32
                    }
                    OdbcDataType::Float { precision: _ } | OdbcDataType::Double => {
                        ArrowDataType::Float64
                    }
                    OdbcDataType::LongVarchar { length: _ } => todo!(),
                    OdbcDataType::LongVarbinary { length: _ } => todo!(),
                    OdbcDataType::Date => ArrowDataType::Date32,
                    OdbcDataType::Timestamp { precision: _ } => todo!(),
                    OdbcDataType::BigInt => ArrowDataType::Int64,
                    OdbcDataType::TinyInt => ArrowDataType::Int8,
                    OdbcDataType::Bit => ArrowDataType::Boolean,
                    OdbcDataType::Varbinary { length: _ } => todo!(),
                    OdbcDataType::Binary { length: _ } => todo!(),
                    OdbcDataType::Unknown
                    | OdbcDataType::Time { precision: _ }
                    | OdbcDataType::Numeric { .. }
                    | OdbcDataType::Decimal { .. }
                    | OdbcDataType::Other {
                        data_type: _,
                        column_size: _,
                        decimal_digits: _,
                    }
                    | OdbcDataType::WChar { length: _ }
                    | OdbcDataType::Char { length: _ }
                    | OdbcDataType::WVarchar { length: _ }
                    | OdbcDataType::Varchar { length: _ } => ArrowDataType::Utf8,
                },
                column_description.could_be_nullable(),
            );

            fields.push(field)
        }

        let schema = Arc::new(Schema::new(fields));
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
                let lazy_sql_data_type = || {
                    cursor
                        .col_data_type(col_index)
                        .map_err(Error::FailedToDescribeColumn)
                };
                let lazy_display_size = || cursor.col_display_size(col_index);
                choose_column_strategy(field, lazy_sql_data_type, lazy_display_size)
            })
            .collect::<Result<_, _>>()?;

        let row_set_buffer = ColumnarRowSet::new(
            max_batch_size,
            column_strategies.iter().map(|cs| cs.buffer_description()),
        );
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

/// All decisions needed to copy data from an ODBC buffer to an Arrow Array
trait ColumnStrategy {
    /// Describes the buffer which is bound to the ODBC cursor.
    fn buffer_description(&self) -> BufferDescription;

    /// Create an arrow array from an ODBC buffer described in [`Self::buffer_description`].
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

fn choose_column_strategy(
    field: &Field,
    lazy_sql_type: impl Fn() -> Result<OdbcDataType, Error>,
    lazy_display_size: impl Fn() -> Result<isize, odbc_api::Error>,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    let strat: Box<dyn ColumnStrategy> = match field.data_type() {
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
        ArrowDataType::Float32 => primitive_arrow_type_startegy::<Float32Type>(field.is_nullable()),
        ArrowDataType::Float64 => primitive_arrow_type_startegy::<Float64Type>(field.is_nullable()),
        ArrowDataType::Timestamp(_, _) => todo!(),
        ArrowDataType::Date32 => {
            if field.is_nullable() {
                Box::new(NullableDate)
            } else {
                Box::new(NonNullableDate)
            }
        }
        ArrowDataType::Date64 => todo!(),
        ArrowDataType::Time32(_) => todo!(),
        ArrowDataType::Time64(_) => todo!(),
        ArrowDataType::Duration(_) => todo!(),
        ArrowDataType::Interval(_) => todo!(),
        ArrowDataType::Binary => todo!(),
        ArrowDataType::FixedSizeBinary(_) => todo!(),
        ArrowDataType::LargeBinary => todo!(),
        ArrowDataType::Utf8 => {
            // Currently we request text data as utf16 as this works well with both Posix and
            // Windows environments.

            // Use the SQL type first to determine buffer length. For character data the display
            // length is automatically multiplied by two, to make room for characters consisting of
            // more than one `u16`.
            let sql_type = lazy_sql_type()?;
            let utf16_len = if let Some(len) = sql_type.utf16_len() {
                len
            } else {
                // The data type does not seem to have a display size associated with it. As a final
                // ditch effort request display size directly from the metadata.
                let signed = lazy_display_size()
                    .map_err(|source| Error::UnknownStringLength { sql_type, source })?;
                if signed < 1 {
                    return Err(Error::InvalidDisplaySize(signed));
                }
                signed as usize
            };
            Box::new(WideText::new(field.is_nullable(), utf16_len))
        }
        ArrowDataType::LargeUtf8 => todo!(),
        ArrowDataType::List(_) => todo!(),
        ArrowDataType::FixedSizeList(_, _) => todo!(),
        ArrowDataType::LargeList(_) => todo!(),
        ArrowDataType::Struct(_) => todo!(),
        ArrowDataType::Union(_) => todo!(),
        ArrowDataType::Dictionary(_, _) => todo!(),
        ArrowDataType::Decimal(precision, scale) => {
            Box::new(Decimal::new(field.is_nullable(), *precision, *scale))
        }
        arrow_type
        @
        (ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Float16) => return Err(Error::UnsupportedArrowType(arrow_type.clone())),
    };
    Ok(strat)
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
        let mut builder = PrimitiveBuilder::<T>::new(values.len());
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
        let mut builder = BooleanBuilder::new(values.len());
        for bit in values {
            builder
                .append_option(bit.copied().map(Bit::as_bool))
                .unwrap()
        }
        Arc::new(builder.finish())
    }
}

struct WideText {
    /// Maximum string length in u16, excluding terminating zero
    max_str_len: usize,
    nullable: bool,
}

impl WideText {
    fn new(nullable: bool, max_str_len: usize) -> Self {
        Self {
            max_str_len,
            nullable,
        }
    }
}

impl ColumnStrategy for WideText {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.nullable,
            kind: BufferKind::WText {
                max_str_len: self.max_str_len,
            },
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = match column_view {
            AnyColumnView::WText(values) => values,
            _ => unreachable!(),
        };
        let mut builder = StringBuilder::new(1);
        // Buffer used to convert individual values from utf16 to utf8.
        let mut buf_utf8 = String::new();
        for value in values {
            buf_utf8.clear();
            let opt = if let Some(utf16) = value {
                for c in decode_utf16(utf16.as_slice().iter().cloned()) {
                    buf_utf8.push(c.unwrap());
                }
                Some(&buf_utf8)
            } else {
                None
            };
            builder.append_option(opt).unwrap();
        }
        Arc::new(builder.finish())
    }
}

struct NonNullableDate;

impl ColumnStrategy for NonNullableDate {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Date,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = Date::as_slice(column_view).unwrap();
        let mut builder = Date32Builder::new(values.len());
        for odbc_date in values {
            builder.append_value(days_since_epoch(&odbc_date)).unwrap();
        }
        Arc::new(builder.finish())
    }
}

struct NullableDate;

impl ColumnStrategy for NullableDate {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Date,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = Date::as_nullable_slice(column_view).unwrap();
        let mut builder = Date32Builder::new(values.len());
        for odbc_date in values {
            builder
                .append_option(odbc_date.map(days_since_epoch))
                .unwrap();
        }
        Arc::new(builder.finish())
    }
}

/// Transform date to days since unix epoch as i32
fn days_since_epoch(date: &Date) -> i32 {
    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
    let date = NaiveDate::from_ymd(date.year as i32, date.month as u32, date.day as u32);
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}

struct Decimal {
    nullable: bool,
    precision: usize,
    scale: usize,
}

impl Decimal {
    fn new(nullable: bool, precision: usize, scale: usize) -> Self {
        Self {
            nullable,
            precision,
            scale,
        }
    }
}

impl ColumnStrategy for Decimal {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription{
            nullable: self.nullable,
            // Must be able to hold num precision digits a sign and a decimal point 
            kind: BufferKind::Text { max_str_len: self.precision + 2 }
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        match column_view {
            AnyColumnView::Text(values) => {
                let capacity = values.len();
                let mut builder = DecimalBuilder::new(capacity, self.precision, self.scale);

                let mut buf_digits = Vec::new();

                for opt in values {
                    if let Some(text) = opt {
                        buf_digits.clear();
                        buf_digits.extend(text.iter().filter(|&&c| c != b'.'));

                        let (num, _consumed) = i128::from_radix_10_signed(&buf_digits);

                        builder.append_value(num).unwrap();
                    } else {
                        builder.append_null().unwrap();
                    }
                }

                Arc::new(builder.finish())
            }
            _ => unreachable!()
        }
    }
}
