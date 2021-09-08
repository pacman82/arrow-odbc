use std::{char::decode_utf16, sync::Arc};

use arrow::array::{ArrayRef, BooleanBuilder, DecimalBuilder, StringBuilder};
use atoi::FromRadix10Signed;
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, Item},
    Bit,
};

mod binary;
mod date_time;
mod no_conversion;
mod with_conversion;

pub use self::{
    binary::{Binary, FixedSizedBinary},
    date_time::{
        DateConversion, TimestampMsConversion, TimestampNsConversion, TimestampSecConversion,
        TimestampUsConversion,
    },
    no_conversion::no_conversion,
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

pub struct WideText {
    /// Maximum string length in u16, excluding terminating zero
    max_str_len: usize,
    nullable: bool,
}

impl WideText {
    pub fn new(nullable: bool, max_str_len: usize) -> Self {
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
        let mut builder = StringBuilder::new(values.len());
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
