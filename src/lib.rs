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
use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{
        DataType as ArrowDataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, Schema, SchemaRef, TimeUnit, UInt8Type,
    },
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use column_strategy::{
    with_conversion, ColumnStrategy, DateConversion, FixedSizedBinary, NarrowText,
    TimestampMsConversion, TimestampNsConversion, TimestampSecConversion, TimestampUsConversion,
};
use odbc_api::{
    buffers::ColumnarRowSet, ColumnDescription, Cursor, DataType as OdbcDataType, RowSetCursor,
};
use thiserror::Error;

use self::column_strategy::{
    no_conversion, Binary, Decimal, NonNullableBoolean, NullableBoolean, WideText,
};

mod column_strategy;

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow;
pub use odbc_api;

/// A variation of things which can go wrong then creating an [`OdbcReader`].
#[derive(Error, Debug)]
pub enum Error {
    /// The type specified in the arrow schema is not supported to be fetched from the database.
    #[error(
        "Unsupported arrow type: `{0}`. This type can currently not be fetched from an ODBC data \
        source by an instance of OdbcReader."
    )]
    UnsupportedArrowType(ArrowDataType),
    /// At ODBC api calls gaining information about the columns did fail.
    #[error(
        "An error occurred fetching the column description or data type from the metainformation \
        attached to the ODBC result set:\n{0}"
    )]
    FailedToDescribeColumn(#[source] odbc_api::Error),
    /// Unable to retrieve the column display size for the column.
    #[error(
        "Unable to deduce the maximum string length for the SQL Data Type reported by the ODBC \
        driver. Reported SQL data type is: {:?}.\n Error fetching column display or octet size: \
        {source}",
        sql_type
    )]
    UnknownStringLength {
        sql_type: OdbcDataType,
        source: odbc_api::Error,
    },
    /// We are getting a display or octet size from ODBC but it is not larger than 0.
    #[error("ODBC reported a display size of {0}.")]
    InvalidDisplaySize(isize),
    /// Failure to retrieve the number of columns from the result set.
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
                    OdbcDataType::Numeric {
                        precision: p @ 0..=38,
                        scale,
                    }
                    | OdbcDataType::Decimal {
                        precision: p @ 0..=38,
                        scale,
                    } => ArrowDataType::Decimal(p, scale.try_into().unwrap()),
                    OdbcDataType::Integer => ArrowDataType::Int32,
                    OdbcDataType::SmallInt => ArrowDataType::Int16,
                    OdbcDataType::Real | OdbcDataType::Float { precision: 0..=24 } => {
                        ArrowDataType::Float32
                    }
                    OdbcDataType::Float { precision: _ } | OdbcDataType::Double => {
                        ArrowDataType::Float64
                    }
                    OdbcDataType::Date => ArrowDataType::Date32,
                    OdbcDataType::Timestamp { precision: 0 } => {
                        ArrowDataType::Timestamp(TimeUnit::Second, None)
                    }
                    OdbcDataType::Timestamp { precision: 1..=3 } => {
                        ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                    OdbcDataType::Timestamp { precision: 4..=6 } => {
                        ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    OdbcDataType::Timestamp { precision: _ } => {
                        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
                    }
                    OdbcDataType::BigInt => ArrowDataType::Int64,
                    OdbcDataType::TinyInt => ArrowDataType::Int8,
                    OdbcDataType::Bit => ArrowDataType::Boolean,
                    OdbcDataType::Binary { length } => {
                        ArrowDataType::FixedSizeBinary(length.try_into().unwrap())
                    }
                    OdbcDataType::LongVarbinary { length: _ }
                    | OdbcDataType::Varbinary { length: _ } => ArrowDataType::Binary,
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
                    | OdbcDataType::LongVarchar { length: _ }
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
                let lazy_octet_size = || cursor.col_octet_length(col_index);
                choose_column_strategy(
                    field,
                    lazy_sql_data_type,
                    lazy_octet_size,
                    lazy_display_size,
                )
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
    lazy_octet_size: impl Fn() -> Result<isize, odbc_api::Error>,
    lazy_display_size: impl Fn() -> Result<isize, odbc_api::Error>,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    let strat: Box<dyn ColumnStrategy> = match field.data_type() {
        ArrowDataType::Boolean => {
            if field.is_nullable() {
                Box::new(NullableBoolean)
            } else {
                Box::new(NonNullableBoolean)
            }
        }
        ArrowDataType::Int8 => no_conversion::<Int8Type>(field.is_nullable()),
        ArrowDataType::Int16 => no_conversion::<Int16Type>(field.is_nullable()),
        ArrowDataType::Int32 => no_conversion::<Int32Type>(field.is_nullable()),
        ArrowDataType::Int64 => no_conversion::<Int64Type>(field.is_nullable()),
        ArrowDataType::UInt8 => no_conversion::<UInt8Type>(field.is_nullable()),
        ArrowDataType::Float32 => no_conversion::<Float32Type>(field.is_nullable()),
        ArrowDataType::Float64 => no_conversion::<Float64Type>(field.is_nullable()),
        ArrowDataType::Date32 => with_conversion(field.is_nullable(), DateConversion),
        ArrowDataType::Utf8 => {
            // Use the SQL type first to determine buffer length.
            let sql_type = lazy_sql_type()?;
            let is_text = matches!(
                sql_type,
                OdbcDataType::LongVarchar { .. }
                    | OdbcDataType::Varchar { .. }
                    | OdbcDataType::WVarchar { .. }
                    | OdbcDataType::Char { .. }
                    | OdbcDataType::WChar { .. }
            );
            if is_text {
                let octet_len = lazy_octet_size()
                    .map_err(|source| Error::UnknownStringLength { sql_type, source })?;
                if cfg!(target_os = "windows") {
                    // Use wide text in windows as default locale can not be expected to be UTF-8
                    wide_text_strategy(octet_len, field)?
                } else {
                    narrow_text_strategy(octet_len, field)?
                }
            } else {
                let display_size = lazy_display_size()
                    .map_err(|source| Error::UnknownStringLength { sql_type, source })?;
                // We assume non text type colmuns to only consist of ASCII characters.
                narrow_text_strategy(display_size, field)?
            }
        }
        ArrowDataType::Decimal(precision, scale) => {
            Box::new(Decimal::new(field.is_nullable(), *precision, *scale))
        }
        ArrowDataType::Binary => {
            let sql_type = lazy_sql_type()?;
            let length = sql_type.column_size();
            Box::new(Binary::new(field.is_nullable(), length))
        }
        ArrowDataType::Timestamp(TimeUnit::Second, _) => {
            with_conversion(field.is_nullable(), TimestampSecConversion)
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            with_conversion(field.is_nullable(), TimestampMsConversion)
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            with_conversion(field.is_nullable(), TimestampUsConversion)
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            with_conversion(field.is_nullable(), TimestampNsConversion)
        }
        ArrowDataType::FixedSizeBinary(length) => Box::new(FixedSizedBinary::new(
            field.is_nullable(),
            (*length).try_into().unwrap(),
        )),
        arrow_type
        @
        (ArrowDataType::Null
        | ArrowDataType::Date64
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::LargeBinary
        | ArrowDataType::LargeUtf8
        | ArrowDataType::List(_)
        | ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::Struct(_)
        | ArrowDataType::Union(_)
        | ArrowDataType::Dictionary(_, _)
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Map(_, _)
        | ArrowDataType::Float16) => return Err(Error::UnsupportedArrowType(arrow_type.clone())),
    };
    Ok(strat)
}

fn wide_text_strategy(
    octet_length: isize,
    field: &Field,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    if octet_length < 1 {
        return Err(Error::InvalidDisplaySize(octet_length));
    }
    let octet_length = octet_length as usize;
    // An octet is a byte, a u16 consists of two bytes therefore we are dividing by
    // two to get the correct length.
    let utf16_len = octet_length / 2;
    Ok(Box::new(WideText::new(field.is_nullable(), utf16_len)))
}

fn narrow_text_strategy(octet_len: isize, field: &Field) -> Result<Box<dyn ColumnStrategy>, Error> {
    if octet_len < 1 {
        return Err(Error::InvalidDisplaySize(octet_len));
    }
    let utf8_len = octet_len as usize;
    Ok(Box::new(NarrowText::new(field.is_nullable(), utf8_len)))
}
