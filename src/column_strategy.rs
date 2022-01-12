use std::{convert::TryInto, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanBuilder, DecimalBuilder},
    datatypes::{
        DataType as ArrowDataType, Field, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, TimeUnit, UInt8Type,
    },
};

use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, Item},
    Bit, DataType as OdbcDataType,
};
use thiserror::Error;

mod binary;
mod date_time;
mod no_conversion;
mod text;
mod with_conversion;

pub use self::{
    binary::{Binary, FixedSizedBinary},
    date_time::{
        DateConversion, TimestampMsConversion, TimestampNsConversion, TimestampSecConversion,
        TimestampUsConversion,
    },
    no_conversion::no_conversion,
    text::choose_text_strategy,
    with_conversion::{with_conversion, Conversion},
};

/// All decisions needed to copy data from an ODBC buffer to an Arrow Array
pub trait ColumnStrategy {
    /// Describes the buffer which is bound to the ODBC cursor.
    fn buffer_description(&self) -> BufferDescription;

    /// Create an arrow array from an ODBC buffer described in [`Self::buffer_description`].
    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef;
}

pub struct NonNullableBoolean;

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

pub struct NullableBoolean;

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

pub struct Decimal {
    nullable: bool,
    precision: usize,
    scale: usize,
}

impl Decimal {
    pub fn new(nullable: bool, precision: usize, scale: usize) -> Self {
        Self {
            nullable,
            precision,
            scale,
        }
    }
}

impl ColumnStrategy for Decimal {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.nullable,
            // Must be able to hold num precision digits a sign and a decimal point
            kind: BufferKind::Text {
                max_str_len: self.precision + 2,
            },
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
            _ => unreachable!(),
        }
    }
}

pub fn choose_column_strategy(
    field: &Field,
    lazy_sql_type: impl Fn() -> Result<OdbcDataType, odbc_api::Error>,
    lazy_display_size: impl Fn() -> Result<isize, odbc_api::Error>,
) -> Result<Box<dyn ColumnStrategy>, ColumnFailure> {
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
            let sql_type = lazy_sql_type().map_err(ColumnFailure::FailedToDescribeColumn)?;
            // Use the SQL type first to determine buffer length.
            choose_text_strategy(sql_type, lazy_display_size, field.is_nullable())?
        }
        ArrowDataType::Decimal(precision, scale) => {
            Box::new(Decimal::new(field.is_nullable(), *precision, *scale))
        }
        ArrowDataType::Binary => {
            let sql_type = lazy_sql_type().map_err(ColumnFailure::FailedToDescribeColumn)?;
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
        | ArrowDataType::Union(_, _)
        | ArrowDataType::Dictionary(_, _)
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64
        | ArrowDataType::Map(_, _)
        | ArrowDataType::Float16) => {
            return Err(ColumnFailure::UnsupportedArrowType(arrow_type.clone()))
        }
    };
    Ok(strat)
}

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
