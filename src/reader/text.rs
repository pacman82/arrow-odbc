use std::{char::decode_utf16, cmp::min, num::NonZeroUsize, sync::Arc};

use arrow::array::{ArrayRef, StringBuilder};
use odbc_api::{
    DataType as OdbcDataType,
    buffers::{AnySlice, BufferDesc},
};

use super::{ColumnFailure, MappingError, ReadStrategy};

/// This function decides wether this column will be queried as narrow (assumed to be utf-8) or
/// wide text (assumed to be utf-16). The reason we do not always use narrow is that the encoding
/// dependends on the system locals which is usually not UTF-8 on windows systems. Furthermore we
/// are trying to adapt the buffer size to the maximum string length the column could contain.
pub fn choose_text_strategy(
    sql_type: OdbcDataType,
    lazy_display_size: impl FnOnce() -> Result<Option<NonZeroUsize>, odbc_api::Error>,
    max_text_size: Option<usize>,
    trim_fixed_sized_character_strings: bool,
    text_encoding: TextEncoding,
) -> Result<Box<dyn ReadStrategy + Send>, ColumnFailure> {
    let apply_buffer_limit = |len| match (len, max_text_size) {
        (None, None) => Err(ColumnFailure::ZeroSizedColumn { sql_type }),
        (None, Some(limit)) => Ok(limit),
        (Some(len), None) => Ok(len),
        (Some(len), Some(limit)) => Ok(min(len, limit)),
    };
    let is_fixed_sized_char = matches!(
        sql_type,
        OdbcDataType::Char { .. } | OdbcDataType::WChar { .. }
    );
    let trim = trim_fixed_sized_character_strings && is_fixed_sized_char;
    let strategy: Box<dyn ReadStrategy + Send> = if text_encoding.use_utf16() {
        let hex_len = sql_type
            .utf16_len()
            .map(Ok)
            .or_else(|| lazy_display_size().transpose())
            .transpose()
            .map_err(|source| ColumnFailure::UnknownStringLength { sql_type, source })?;
        let hex_len = apply_buffer_limit(hex_len.map(NonZeroUsize::get))?;
        wide_text_strategy(hex_len, trim)
    } else {
        let octet_len = sql_type
            .utf8_len()
            .map(Ok)
            .or_else(|| lazy_display_size().transpose())
            .transpose()
            .map_err(|source| ColumnFailure::UnknownStringLength { sql_type, source })?;
        let octet_len = apply_buffer_limit(octet_len.map(NonZeroUsize::get))?;
        // So far only Linux users seemed to have complained about panics due to garbage indices?
        // Linux usually would use UTF-8, so we only invest work in working around this for narrow
        // strategies
        narrow_text_strategy(octet_len, trim)
    };

    Ok(strategy)
}

/// Used to indicate the preferred encoding for text columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextEncoding {
    /// Evaluates to [`Self::Utf16`] on windows and [`Self::Utf8`] on other systems. We do this,
    /// because most systems e.g. MacOs and Linux use UTF-8 as their default encoding, while windows
    /// may still use a Latin1 or some other extended ASCII as their narrow encoding. On the other
    /// hand many Posix drivers are lacking in their support for wide function calls and UTF-16. So
    /// using `Wide` on windows and `Narrow` everythere else is a good starting point.
    Auto,
    /// Use narrow characters (one byte) to encode text in payloads. ODBC lets the client choose the
    /// encoding which should be based on the system local. This is often not what is actually
    /// happening though. If we use narrow encoding, we assume the text to be UTF-8 and error if we
    /// find that not to be the case.
    Utf8,
    /// Use wide characters (two bytes) to encode text in payloads. ODBC defines the encoding to
    /// be always UTF-16.
    Utf16,
}

impl Default for TextEncoding {
    fn default() -> Self {
        Self::Auto
    }
}

impl TextEncoding {
    pub fn use_utf16(&self) -> bool {
        match self {
            Self::Auto => cfg!(target_os = "windows"),
            Self::Utf8 => false,
            Self::Utf16 => true,
        }
    }
}

fn wide_text_strategy(u16_len: usize, trim: bool) -> Box<dyn ReadStrategy + Send> {
    Box::new(WideText::new(u16_len, trim))
}

fn narrow_text_strategy(octet_len: usize, trim: bool) -> Box<dyn ReadStrategy + Send> {
    Box::new(NarrowText::new(octet_len, trim))
}

/// Strategy requesting the text from the database as UTF-16 (Wide characters) and emmitting it as
/// UTF-8. We use it, since the narrow representation in ODBC is not always guaranteed to be UTF-8,
/// but depends on the local instead.
pub struct WideText {
    /// Maximum string length in u16, excluding terminating zero
    max_str_len: usize,
    /// Wether the string should be trimmed.
    trim: bool,
}

impl WideText {
    pub fn new(max_str_len: usize, trim: bool) -> Self {
        Self { max_str_len, trim }
    }
}

impl ReadStrategy for WideText {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::WText {
            max_str_len: self.max_str_len,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_w_text_view().unwrap();
        let item_capacity = view.len();
        // Any utf-16 character could take up to 4 Bytes if represented as utf-8, but since mostly
        // this is 1 to one, and also not every string is likeyl to use its maximum capacity, we
        // rather accept the reallocation in these scenarios.
        let data_capacity = self.max_str_len * item_capacity;
        let mut builder = StringBuilder::with_capacity(item_capacity, data_capacity);
        // Buffer used to convert individual values from utf16 to utf8.
        let mut buf_utf8 = String::new();
        for value in view.iter() {
            buf_utf8.clear();
            let opt = if let Some(utf16) = value {
                for c in decode_utf16(utf16.as_slice().iter().cloned()) {
                    buf_utf8.push(c.unwrap());
                }
                let slice = if self.trim {
                    buf_utf8.trim()
                } else {
                    buf_utf8.as_str()
                };
                Some(slice)
            } else {
                None
            };
            builder.append_option(opt);
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct NarrowText {
    /// Maximum string length in u8, excluding terminating zero
    max_str_len: usize,
    /// Wether the string should be trimmed.
    trim: bool,
}

impl NarrowText {
    pub fn new(max_str_len: usize, trim: bool) -> Self {
        Self { max_str_len, trim }
    }
}

impl ReadStrategy for NarrowText {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: self.max_str_len,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = StringBuilder::with_capacity(view.len(), self.max_str_len * view.len());
        for value in view.iter() {
            builder.append_option(
                value
                    .map(|bytes| {
                        let untrimmed =
                            std::str::from_utf8(bytes).map_err(|_| MappingError::InvalidUtf8 {
                                lossy_value: String::from_utf8_lossy(bytes).into_owned(),
                            })?;
                        Ok(if self.trim {
                            untrimmed.trim()
                        } else {
                            untrimmed
                        })
                    })
                    .transpose()?,
            );
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use odbc_api::buffers::{AnySlice, ColumnBuffer, TextColumn};

    use crate::reader::{MappingError, ReadStrategy as _};

    use super::NarrowText;

    #[test]
    fn must_return_error_for_invalid_utf8() {
        // Given a slice with invalid utf-8
        let mut column = TextColumn::new(1, 10);
        column.set_value(0, Some(&[b'H', b'e', b'l', b'l', b'o', 0xc3]));
        let column_view = AnySlice::Text(column.view(1));

        // When
        let strategy = NarrowText::new(5, false);
        let result = strategy.fill_arrow_array(column_view);

        // Then
        let error = result.unwrap_err();
        let MappingError::InvalidUtf8 { lossy_value } = error else {
            panic!("Not an InvalidUtf8 error")
        };
        assert_eq!(lossy_value, "Hello�");
    }
}
