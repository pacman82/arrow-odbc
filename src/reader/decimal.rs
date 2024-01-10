use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Builder};
use atoi::FromRadix10Signed;
use odbc_api::buffers::{AnySlice, BufferDesc};

use super::{MappingError, ReadStrategy};

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
                let num = decimal_text_to_int(text, &mut buf_digits);
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

fn decimal_text_to_int(text: &[u8], buf_digits: &mut Vec<u8>) -> i128 {
    buf_digits.clear();
    // Originally we just filtered out the decimal point. Yet only leaving ascii digits is more
    // robust, as it would also filter out seperators of thousands as well as the decimal point,
    // even if it is not actually a decimal point, but a comma (`,`). We still rely on all trailing
    // zeroes being present though.
    buf_digits.extend(text.iter().filter(|&&c| c.is_ascii_digit()));
    let (num, _consumed) = i128::from_radix_10_signed(buf_digits);
    num
}

#[cfg(test)]
mod tests {
    use super::decimal_text_to_int;

    /// An user of an Oracle database got invalid values from decimal after setting
    /// `NLS_NUMERIC_CHARACTERS` to ",." instead of ".".
    ///
    /// See issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_is_represented_with_comma_as_radix() {
        let mut buf_digits = Vec::new();
        let actual = decimal_text_to_int(b"10,00000", &mut buf_digits);
        assert_eq!(1000000, actual);
    }
}