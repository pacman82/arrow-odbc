use std::{cmp::min, num::NonZeroUsize, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BinaryBuilder, StringArray, StringBuilder},
    datatypes::DataType,
};
use encoding_rs::mem::convert_utf16_to_str;
use odbc_api::{
    DataType as OdbcDataType,
    buffers::{AnyColumnBufferSlice, BufferDesc, TextColumnSlice},
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

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_wide_text().unwrap();
        let item_capacity = view.len();
        // Any utf-16 character could take up to 4 Bytes if represented as utf-8, but since mostly
        // this is 1 to one, and also not every string is likeyl to use its maximum capacity, we
        // rather accept the reallocation in these scenarios.
        let data_capacity = self.max_str_len * item_capacity;
        let mut builder = StringBuilder::with_capacity(item_capacity, data_capacity);

        let mut converter = Utf16ToUtf8Converter::new();
        for value in view.iter() {
            let opt = if let Some(utf16) = value {
                let slice = converter.utf16_to_utf8(utf16.as_slice());
                let slice = if self.trim { slice.trim() } else { slice };
                Some(slice)
            } else {
                None
            };
            builder.append_option(opt);
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct Utf16ToUtf8Converter {
    // Buffer used to convert individual values from utf16 to utf8.
    buf_utf8: String,
}

impl Utf16ToUtf8Converter {
    fn new() -> Self {
        Self {
            buf_utf8: String::new(),
        }
    }

    fn utf16_to_utf8(&mut self, utf16: &[u16]) -> &str {
        let max_utf8_len = utf16.len() * 3;
        while self.buf_utf8.len() < max_utf8_len {
            self.buf_utf8.push('\0');
        }
        let written = convert_utf16_to_str(utf16, &mut self.buf_utf8);
        &self.buf_utf8[..written]
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

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text().unwrap();
        let (total, non_null) = view
            .iter()
            .flatten()
            .fold((0, 0usize), |(total, count), bytes| {
                (total + bytes.len(), count + 1)
            });
        // simdutf8 uses scalar validation below 64 bytes; batch these short values together.
        if !self.trim && total < non_null.saturating_mul(64) {
            narrow_text_validated_in_bulk(view, total)
        } else {
            narrow_text_validated_per_value(view, total, self.trim)
        }
    }
}

/// Copies all values first, then validates the whole buffer as UTF-8 at once.
fn narrow_text_validated_in_bulk(
    view: TextColumnSlice<u8>,
    total: usize,
) -> Result<ArrayRef, MappingError> {
    let mut builder = BinaryBuilder::with_capacity(view.len(), total);
    for value in view.iter() {
        builder.append_option(value);
    }
    let data = builder
        .finish()
        .into_data()
        .into_builder()
        .data_type(DataType::Utf8);
    match data.build() {
        Ok(data) => Ok(Arc::new(StringArray::from(data))),
        Err(error) => {
            let first_invalid = view
                .iter()
                .flatten()
                .find(|bytes| simdutf8::basic::from_utf8(bytes).is_err())
                .unwrap_or_else(|| panic!("{error}"));
            Err(invalid_utf8(first_invalid))
        }
    }
}

/// Validates each value on its own. Needed for trimming, and faster for long values.
fn narrow_text_validated_per_value(
    view: TextColumnSlice<u8>,
    total: usize,
    trim: bool,
) -> Result<ArrayRef, MappingError> {
    let mut builder = StringBuilder::with_capacity(view.len(), total);
    for value in view.iter() {
        let text = value
            .map(|bytes| simdutf8::basic::from_utf8(bytes).map_err(|_| invalid_utf8(bytes)))
            .transpose()?;
        builder.append_option(if trim { text.map(str::trim) } else { text });
    }
    Ok(Arc::new(builder.finish()))
}

fn invalid_utf8(bytes: &[u8]) -> MappingError {
    MappingError::InvalidUtf8 {
        lossy_value: String::from_utf8_lossy(bytes).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, StringArray};
    use odbc_api::buffers::{AnyColumnBuffer, Slice, TextColumn};

    use crate::reader::{MappingError, ReadStrategy as _, text::Utf16ToUtf8Converter};

    use super::NarrowText;

    /// Then querying under windows (implying UTF-16 encoding) a column with value
    /// "Colt Telecom España S.A." was followed by a column if max_length with 18 bytes in UTF-8
    /// encoding. This let to a panic due to a violation of the String invariant, because that would
    /// split the reused buffer in the middle of the UTF-8 character "ñ" (which is 2 bytes in
    /// UTF-8).
    ///
    /// See: <https://github.com/pacman82/arrow-odbc/issues/177>
    ///
    /// And also a similar issue in odbc2parquet:
    /// <https://github.com/pacman82/odbc2parquet/issues/862>
    #[test]
    fn do_not_split_buffer_accross_char_boundaries() {
        // Given a string with a multibyte character at position 18 then encoded in UTF-8.
        let utf_16_with_multibyte = "Colt Telecom España S.A."
            .encode_utf16()
            .collect::<Vec<u16>>();
        // And a string value with 6 characters. A maximum length of a character in UTF-8 is 3
        // bytes. 3 x 6 = 18, so this would cause the multibyte character in the previous string to
        // be splitted if we reuse the same buffer for both strings.
        let six = "123456".encode_utf16().collect::<Vec<u16>>();

        // When convertig both values in succession to UTF-8
        let mut converter = Utf16ToUtf8Converter::new();
        let first = converter.utf16_to_utf8(&utf_16_with_multibyte).to_owned();
        let second = converter.utf16_to_utf8(&six);

        // Then both strings should be correct and no panic occurred (implied by reaching this line
        assert_eq!(first, "Colt Telecom España S.A.");
        assert_eq!(second, "123456");
    }

    #[test]
    fn must_return_error_for_invalid_utf8() {
        // Given a slice with invalid utf-8
        let mut column = TextColumn::new(1, 10);
        column.set_value(0, Some(&[b'H', b'e', b'l', b'l', b'o', 0xc3]));
        let column: Box<dyn AnyColumnBuffer> = Box::new(column);
        let column_view = column.slice(1);

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

    fn read_narrow(values: &[Option<&[u8]>], trim: bool) -> Result<ArrayRef, MappingError> {
        let max_len = values
            .iter()
            .flatten()
            .map(|bytes| bytes.len())
            .max()
            .unwrap_or(0);
        let mut column = TextColumn::<u8>::new(values.len(), max_len);
        for (index, value) in values.iter().enumerate() {
            column.set_value(index, *value);
        }
        let column: Box<dyn AnyColumnBuffer> = Box::new(column);
        NarrowText::new(max_len, trim).fill_arrow_array(column.slice(values.len()))
    }

    #[test]
    fn invalid_utf8_in_middle_returns_first_invalid_value() {
        let error =
            read_narrow(&[Some(b"valid"), Some(b"Bad\xff"), Some(b"\xfe")], false).unwrap_err();
        let MappingError::InvalidUtf8 { lossy_value } = error else {
            panic!("Not an InvalidUtf8 error")
        };
        assert_eq!(lossy_value, "Bad�");
    }

    #[test]
    fn utf8_character_must_not_span_values() {
        let error = read_narrow(&[Some(&[0xc3]), Some(&[0xb1])], false).unwrap_err();
        let MappingError::InvalidUtf8 { lossy_value } = error else {
            panic!("Not an InvalidUtf8 error")
        };
        assert_eq!(lossy_value, "�");
    }

    #[test]
    fn narrow_text_preserves_nulls_and_empty_strings() {
        let array = read_narrow(&[None, Some(b""), Some("ñ".as_bytes()), None], false).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            [None, Some(""), Some("ñ"), None]
        );
    }

    #[test]
    fn narrow_text_trims_values_and_rejects_invalid_utf8() {
        let array = read_narrow(&[Some(" \tñ \n".as_bytes()), Some(b"  ")], true).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.iter().collect::<Vec<_>>(), [Some("ñ"), Some("")]);

        let error = read_narrow(&[Some(b"good"), Some(b" \xff ")], true).unwrap_err();
        let MappingError::InvalidUtf8 { lossy_value } = error else {
            panic!("Not an InvalidUtf8 error")
        };
        assert_eq!(lossy_value, " � ");
    }

    #[test]
    fn invalid_utf8_in_long_value_returns_error() {
        let valid = [b'a'; 64];
        let mut invalid = [b'b'; 64];
        invalid[63] = 0xff;
        let error = read_narrow(&[Some(&valid), None, Some(&invalid)], false).unwrap_err();
        let MappingError::InvalidUtf8 { lossy_value } = error else {
            panic!("Not an InvalidUtf8 error")
        };
        assert_eq!(lossy_value, format!("{}�", "b".repeat(63)));
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use odbc_api::buffers::{BoxColumnBuffer, Slice, TextColumn};
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::{NarrowText, ReadStrategy, WideText};

    const ROWS: usize = 1024;

    #[divan::bench]
    fn short_ascii(bencher: divan::Bencher) {
        let values = (0..ROWS).map(|i| format!("{i:04} {}", "a".repeat(11 + i * 37 % 113)));
        wide_text_to_arrow(bencher, values);
    }

    #[divan::bench]
    fn mixed_unicode(bencher: divan::Bencher) {
        let values = (0..ROWS).map(|i| {
            format!(
                "  Café 漢字 🙂 {i:04} {} \t",
                "naïve 東京 🚀 ".repeat(1 + i * 17 % 8)
            )
        });
        wide_text_to_arrow(bencher, values);
    }

    #[divan::bench]
    fn long_ascii(bencher: divan::Bencher) {
        let values = (0..ROWS).map(|i| format!("{i:04} {}", "x".repeat(1019 + i * 37 % 3073)));
        wide_text_to_arrow(bencher, values);
    }

    /// Measures converting a wide text column holding `values` into an Arrow array.
    fn wide_text_to_arrow(bencher: divan::Bencher, values: impl Iterator<Item = String>) {
        let utf16: Vec<Vec<u16>> = values.map(|text| text.encode_utf16().collect()).collect();
        let max_str_len = utf16.iter().map(Vec::len).max().unwrap();
        let mut column = TextColumn::<u16>::new(ROWS, max_str_len);
        for (index, value) in utf16.iter().enumerate() {
            column.set_value(index, Some(value));
        }
        let column: BoxColumnBuffer = Box::new(column);
        let strategy = WideText::new(max_str_len, false);

        bencher.bench_local(|| {
            // Drop within the timed closure, so destruction of the array is measured, too.
            let array = strategy.fill_arrow_array(column.slice(ROWS)).unwrap();
            drop(divan::black_box(array));
        });
    }

    #[divan::bench]
    fn narrow_short_ascii(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 255, false, |rng| {
            "a".repeat(rng.random_range(5..=40))
        });
    }

    #[divan::bench]
    fn narrow_long_ascii(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 4096, false, |rng| {
            "a".repeat(rng.random_range(1024..=4096))
        });
    }

    #[divan::bench]
    fn narrow_mixed_unicode(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 255, false, |rng| {
            "Café 漢字 🙂 ".repeat(rng.random_range(1..=8))
        });
    }

    #[divan::bench]
    fn narrow_short_multibyte(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 255, false, |rng| {
            "Café 漢字 🙂 ".repeat(rng.random_range(1..=2))
        });
    }

    #[divan::bench]
    fn narrow_skewed(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 2000, false, |rng| {
            let len = if rng.random_range(0..100) < 2 {
                rng.random_range(1000..=2000)
            } else {
                rng.random_range(5..=40)
            };
            "a".repeat(len)
        });
    }

    #[divan::bench]
    fn narrow_trimmed(bencher: divan::Bencher) {
        narrow_text_to_arrow(bencher, 255, true, |rng| {
            format!(" \t{} \n", "a".repeat(rng.random_range(5..=40)))
        });
    }

    fn narrow_text_to_arrow(
        bencher: divan::Bencher,
        max_str_len: usize,
        trim: bool,
        value: impl Fn(&mut StdRng) -> String,
    ) {
        const ROWS: usize = 10_000;
        let mut rng = StdRng::seed_from_u64(42);
        let mut column = TextColumn::<u8>::new(ROWS, max_str_len);
        for index in 0..ROWS {
            let text = value(&mut rng);
            column.set_value(
                index,
                (rng.random_range(0..100) != 0).then_some(text.as_bytes()),
            );
        }
        let column: BoxColumnBuffer = Box::new(column);
        let strategy = NarrowText::new(max_str_len, trim);

        bencher.bench_local(|| {
            let array = strategy.fill_arrow_array(column.slice(ROWS)).unwrap();
            drop(divan::black_box(array));
        });
    }
}
