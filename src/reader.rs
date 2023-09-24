use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanBuilder, Decimal128Builder},
    datatypes::{
        DataType as ArrowDataType, Date32Type, Field, Float32Type, Float64Type, Int16Type,
        Int32Type, Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    },
};

use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnySlice, BufferDesc, Item},
    Bit, DataType as OdbcDataType, ResultSetMetadata,
};
use thiserror::Error;

mod binary;
mod map_odbc_to_arrow;
mod odbc_reader;
mod text;

use self::map_odbc_to_arrow::MapOdbcToArrow;

use crate::date_time::{
    days_since_epoch, ms_since_epoch, ns_since_epoch, seconds_since_epoch, us_since_epoch,
};

pub use self::{
    binary::{Binary, FixedSizedBinary},
    text::choose_text_strategy,
    map_odbc_to_arrow::MappingError,
    odbc_reader::OdbcReader,
};

/// All decisions needed to copy data from an ODBC buffer to an Arrow Array
pub trait ReadStrategy {
    /// Describes the buffer which is bound to the ODBC cursor.
    fn buffer_desc(&self) -> BufferDesc;

    /// Create an arrow array from an ODBC buffer described in [`Self::buffer_description`].
    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError>;
}

pub struct NonNullableBoolean;

impl ReadStrategy for NonNullableBoolean {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Bit { nullable: false }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let values = Bit::as_slice(column_view).unwrap();
        let mut builder = BooleanBuilder::new();
        for bit in values {
            builder.append_value(bit.as_bool());
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct NullableBoolean;

impl ReadStrategy for NullableBoolean {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Bit { nullable: true }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let values = Bit::as_nullable_slice(column_view).unwrap();
        let mut builder = BooleanBuilder::new();
        for bit in values {
            builder.append_option(bit.copied().map(Bit::as_bool))
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct Decimal {
    precision: u8,
    scale: i8,
}

impl Decimal {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self { precision, scale }
    }
}

impl ReadStrategy for Decimal {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            // Must be able to hold num precision digits a sign and a decimal point
            max_str_len: self.precision as usize + 2,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = Decimal128Builder::new();

        let mut buf_digits = Vec::new();

        for opt in view.iter() {
            if let Some(text) = opt {
                buf_digits.clear();
                buf_digits.extend(text.iter().filter(|&&c| c != b'.'));

                let (num, _consumed) = i128::from_radix_10_signed(&buf_digits);

                builder.append_value(num);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(
            builder
                .finish()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap(),
        ))
    }
}

/// Allows setting limits for buffers bound to the ODBC data source. Check this out if you find that
/// you get memory allocation, or zero sized column errors. Used than constructing a reader using
/// [`crate::OdbcReader::with`].
#[derive(Default, Debug, Clone, Copy)]
pub struct BufferAllocationOptions {
    /// An upper limit for the size of buffers bound to variadic text columns of the data source.
    /// This limit does not (directly) apply to the size of the created arrow buffers, but rather
    /// applies to the buffers used for the data in transit. Use this option if you have e.g.
    /// `VARCHAR(MAX)` fields in your database schema. In such a case without an upper limit, the
    /// ODBC driver of your data source is asked for the maximum size of an element, and is likely
    /// to answer with either `0` or a value which is way larger than any actual entry in the column
    /// If you can not adapt your database schema, this limit might be what you are looking for. On
    /// windows systems the size is double words (16Bit), as windows utilizes an UTF-16 encoding. So
    /// this translates to roughly the size in letters. On non windows systems this is the size in
    /// bytes and the datasource is assumed to utilize an UTF-8 encoding. `None` means no upper
    /// limit is set and the maximum element size, reported by ODBC is used to determine buffer
    /// sizes.
    pub max_text_size: Option<usize>,
    /// An upper limit for the size of buffers bound to variadic binary columns of the data source.
    /// This limit does not (directly) apply to the size of the created arrow buffers, but rather
    /// applies to the buffers used for the data in transit. Use this option if you have e.g.
    /// `VARBINARY(MAX)` fields in your database schema. In such a case without an upper limit, the
    /// ODBC driver of your data source is asked for the maximum size of an element, and is likely
    /// to answer with either `0` or a value which is way larger than any actual entry in the
    /// column. If you can not adapt your database schema, this limit might be what you are looking
    /// for. This is the maximum size in bytes of the binary column.
    pub max_binary_size: Option<usize>,
    /// Set to `true` in order to trigger an [`ColumnFailure::TooLarge`] instead of a panic in case
    /// the buffers can not be allocated due to their size. This might have a performance cost for
    /// constructing the reader. `false` by default.
    pub fallibale_allocations: bool,
}

pub fn choose_column_strategy(
    field: &Field,
    query_metadata: &mut impl ResultSetMetadata,
    col_index: u16,
    buffer_allocation_options: BufferAllocationOptions,
) -> Result<Box<dyn ReadStrategy>, ColumnFailure> {
    let strat: Box<dyn ReadStrategy> = match field.data_type() {
        ArrowDataType::Boolean => {
            if field.is_nullable() {
                Box::new(NullableBoolean)
            } else {
                Box::new(NonNullableBoolean)
            }
        }
        ArrowDataType::Int8 => Int8Type::identical(field.is_nullable()),
        ArrowDataType::Int16 => Int16Type::identical(field.is_nullable()),
        ArrowDataType::Int32 => Int32Type::identical(field.is_nullable()),
        ArrowDataType::Int64 => Int64Type::identical(field.is_nullable()),
        ArrowDataType::UInt8 => UInt8Type::identical(field.is_nullable()),
        ArrowDataType::Float32 => Float32Type::identical(field.is_nullable()),
        ArrowDataType::Float64 => Float64Type::identical(field.is_nullable()),
        ArrowDataType::Date32 => {
            Date32Type::map_with(field.is_nullable(), |e| Ok(days_since_epoch(e)))
        }
        ArrowDataType::Utf8 => {
            let sql_type = query_metadata
                .col_data_type(col_index)
                .map_err(ColumnFailure::FailedToDescribeColumn)?;
            let lazy_display_size = || query_metadata.col_display_size(col_index);
            // Use the SQL type first to determine buffer length.
            choose_text_strategy(
                sql_type,
                lazy_display_size,
                buffer_allocation_options.max_text_size,
            )?
        }
        ArrowDataType::Decimal128(precision, scale @ 0..) => {
            Box::new(Decimal::new(*precision, *scale))
        }
        ArrowDataType::Binary => {
            let sql_type = query_metadata
                .col_data_type(col_index)
                .map_err(ColumnFailure::FailedToDescribeColumn)?;
            let length = sql_type.column_size();
            let length = match (length, buffer_allocation_options.max_binary_size) {
                (0, None) => return Err(ColumnFailure::ZeroSizedColumn { sql_type }),
                (0, Some(limit)) => limit,
                (len, None) => len,
                (len, Some(limit)) => {
                    if len < limit {
                        len
                    } else {
                        limit
                    }
                }
            };
            Box::new(Binary::new(length))
        }
        ArrowDataType::Timestamp(TimeUnit::Second, _) => {
            TimestampSecondType::map_with(field.is_nullable(), |e| Ok(seconds_since_epoch(e)))
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            TimestampMillisecondType::map_with(field.is_nullable(), |e| Ok(ms_since_epoch(e)))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            TimestampMicrosecondType::map_with(field.is_nullable(), |e| Ok(us_since_epoch(e)))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            TimestampNanosecondType::map_with(field.is_nullable(), ns_since_epoch)
        }
        ArrowDataType::FixedSizeBinary(length) => {
            Box::new(FixedSizedBinary::new((*length).try_into().unwrap()))
        }
        unsupported_arrow_type => {
            return Err(ColumnFailure::UnsupportedArrowType(
                unsupported_arrow_type.clone(),
            ))
        }
    };
    Ok(strat)
}

/// Read error related to a specific column
#[derive(Error, Debug)]
pub enum ColumnFailure {
    /// We are getting a display or column size from ODBC but it is not larger than 0.
    #[error(
        "ODBC reported a size of '0' for the column. This might indicate that the driver cannot \
        specify a sensible upper bound for the column. E.g. for cases like VARCHAR(max). Try \
        casting the column into a type with a sensible upper bound. The type of the column causing \
        this error is {:?}.",
        sql_type
    )]
    ZeroSizedColumn { sql_type: OdbcDataType },
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
    #[error(
        "Column buffer is too large to be allocated. Tried to alloacte {num_elements} elements \
        with {element_size} bytes in size each."
    )]
    TooLarge {
        num_elements: usize,
        element_size: usize,
    },
}

impl ColumnFailure {
    /// Provides the error with additional context of Error with column name and index.
    pub fn into_crate_error(self, name: String, index: usize) -> crate::Error {
        crate::Error::ColumnFailure {
            name,
            index,
            source: self,
        }
    }
}
