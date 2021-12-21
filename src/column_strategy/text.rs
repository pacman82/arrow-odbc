use std::{char::decode_utf16, sync::Arc};

use arrow::array::{ArrayRef, StringBuilder};
use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind},
    DataType as OdbcDataType,
};

use crate::Error;

use super::ColumnStrategy;

pub fn choose_text_strategy(
    lazy_sql_type: impl Fn() -> Result<OdbcDataType, Error>,
    lazy_octet_size: impl Fn() -> Result<isize, odbc_api::Error>,
    lazy_display_size: impl Fn() -> Result<isize, odbc_api::Error>,
    is_nullable: bool,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    let sql_type = lazy_sql_type()?;
    let is_narrow = matches!(
        sql_type,
        OdbcDataType::LongVarchar { .. } | OdbcDataType::Varchar { .. } | OdbcDataType::Char { .. }
    );
    let is_wide = matches!(
        sql_type,
        OdbcDataType::WVarchar { .. } | OdbcDataType::WChar { .. }
    );
    let is_text = is_narrow || is_wide;
    Ok(if is_text {
        let octet_len =
            lazy_octet_size().map_err(|source| Error::UnknownStringLength { sql_type, source })?;
        if cfg!(target_os = "windows") {
            let octet_len = if is_narrow {
                // Narrow column queried from a windows platform => Utf8 to Utf16
                // conversion. The letters in the range from U+0000 to U+007F take two bytes
                // in UTF-16 but only one byte in UTF-8
                octet_len * 2
            } else {
                // Wide text column on database queried from a windows platform.
                octet_len
            };
            // Use wide text in windows as default locale can not be expected to be UTF-8
            wide_text_strategy(octet_len, is_nullable)?
        } else {
            let octet_len = if is_wide {
                // Wide column queried on non-window platform => Utf16 to Utf8 conversion.
                // We must adjust the binary size reported by the database, since it assumes
                // UTF-16 encoding. While usually an UTF-8 string is shorter for characters
                // in the range from U+0800 to U+FFFF UTF-8 is three bytes theras UTF-16 is
                // only two bytes large. We have to allocate enough memory to hold the
                // largest possible string.
                (octet_len / 2) * 3
            } else {
                // Narrow column queried on non-windows platform => Utf8 to Utf8
                octet_len
            };
            narrow_text_strategy(octet_len, is_nullable)?
        }
    } else {
        let display_size = lazy_display_size()
            .map_err(|source| Error::UnknownStringLength { sql_type, source })?;
        // We assume non text type colmuns to only consist of ASCII characters.
        narrow_text_strategy(display_size, is_nullable)?
    })
}

fn wide_text_strategy(
    octet_length: isize,
    is_nullable: bool,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    if octet_length < 1 {
        return Err(Error::InvalidDisplaySize(octet_length));
    }
    let octet_length = octet_length as usize;
    // An octet is a byte, a u16 consists of two bytes therefore we are dividing by
    // two to get the correct length.
    let utf16_len = octet_length / 2;
    Ok(Box::new(WideText::new(is_nullable, utf16_len)))
}

fn narrow_text_strategy(
    octet_len: isize,
    is_nullable: bool,
) -> Result<Box<dyn ColumnStrategy>, Error> {
    if octet_len < 1 {
        return Err(Error::InvalidDisplaySize(octet_len));
    }
    let utf8_len = octet_len as usize;
    Ok(Box::new(NarrowText::new(is_nullable, utf8_len)))
}

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
