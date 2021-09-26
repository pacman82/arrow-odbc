use std::{char::decode_utf16, sync::Arc};

use arrow::array::{ArrayRef, StringBuilder};
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};

use super::ColumnStrategy;

/// Strategy requesting the text from the database as UTF-16 (Wide characters) and emmitting it as
/// UTF-8. We use it, since the narrow representation in ODBC is not always guaranteed to be UTF-8,
/// but depends on the local instead.
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

pub struct NarrowText {
    /// Maximum string length in u8, excluding terminating zero
    max_str_len: usize,
    nullable: bool,
}

impl NarrowText {
    pub fn new(nullable: bool, max_str_len: usize) -> Self {
        Self {
            max_str_len,
            nullable,
        }
    }
}

impl ColumnStrategy for NarrowText {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.nullable,
            kind: BufferKind::Text {
                max_str_len: self.max_str_len,
            },
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = match column_view {
            AnyColumnView::Text(values) => values,
            _ => unreachable!(),
        };
        let mut builder = StringBuilder::new(values.len());
        for value in values {
            builder
                .append_option(value.map(|bytes| {
                    std::str::from_utf8(bytes)
                        .expect("ODBC column had been expected to return valid utf8, but did not.")
                }))
                .unwrap();
        }
        Arc::new(builder.finish())
    }
}
