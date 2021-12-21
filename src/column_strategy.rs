use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, DecimalBuilder};
use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, Item},
    Bit,
};

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
