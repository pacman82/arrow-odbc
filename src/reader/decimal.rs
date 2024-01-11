use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Builder};
use atoi::FromRadix10Signed;
use odbc_api::buffers::{AnySlice, BufferDesc};

use super::{MappingError, ReadStrategy};

pub struct Decimal {
    precision: u8,
    /// We know scale to be non-negative, yet we can save us some conversions storing it as i8.
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
        let scale = self.scale as usize;

        for opt in view.iter() {
            if let Some(text) = opt {
                let num = decimal_text_to_int(text, scale);
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

fn decimal_text_to_int(text: &[u8], scale: usize) -> i128 {
    // High is now the number before the decimal point
    let (mut high, num_digits_high) = i128::from_radix_10_signed(text);
    let (low, num_digits_low) = if num_digits_high == text.len() {
        (0, 0)
    } else {
        i128::from_radix_10_signed(&text[(num_digits_high + 1)..])
    };
    // Left shift high so it is compatible with low
    for _ in 0..num_digits_low {
        high *= 10;
    }
    // We want to increase the absolute of high by low without changing highs sign
    let mut n = if high < 0 {
        high - low
    } else {
        high + low
    };
    // We would be done now, if every database would include trailing zeroes, but they might choose
    // to omit those. Therfore we see if we need to leftshift n further in order to meet scale.
    for _ in 0..(scale - num_digits_low) {
        n *= 10;
    }
    n
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
        let actual = decimal_text_to_int(b"10,00000", 5);
        assert_eq!(1_000_000, actual);
    }

    /// Since scale is 5 in this test case we would expect five digits after the radix, yet Oracle
    /// seems to not emit trailing zeroes. Also see issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_with_less_zeroes() {
        let actual = decimal_text_to_int(b"10.0", 5);
        assert_eq!(1_000_000, actual);
    }

    #[test]
    fn negative_decimal() {
        let actual = decimal_text_to_int(b"-10.00000", 5);
        assert_eq!(-1_000_000, actual);
    }
}