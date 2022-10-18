use arrow::array::{Array, Decimal128Array, Decimal256Array};
use odbc_api::buffers::{BufferDescription, BufferKind, AnySliceMut};

use crate::{odbc_writer::WriteStrategy, WriterError};

pub struct NullableDecimal128AsText {
    precision: u8,
    scale: u8,
}

impl NullableDecimal128AsText {
    pub fn new(precision: u8, scale: u8) -> Self {
        Self { precision, scale }
    }
}

pub struct NullableDecimal256AsText {
    precision: u8,
    scale: u8,
}

impl NullableDecimal256AsText {
    pub fn new(precision: u8, scale: u8) -> Self {
        Self { precision, scale }
    }
}

/// Length of a text representation of a decimal
fn len_text(scale: u8, precision: u8) -> usize {
    let radix_character_length = if scale == 0 { 0 } else { 1 };
    // Precision digits + optional point + sign
    precision as usize + radix_character_length + 1
}

impl WriteStrategy for NullableDecimal128AsText {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Text {
                max_str_len: len_text(self.scale, self.precision),
            },
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let length = len_text(self.scale, self.precision);

        let from = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let mut to = column_buf.as_text_view().unwrap();

        for (index, cell) in from.iter().enumerate() {
            if let Some(value) = cell {
                let buf = to.set_mut(index + param_offset, length);
                write_integer_as_decimal(value.as_i128(), self.precision, self.scale, buf)
            } else {
                to.set_cell(index + param_offset, None)
            }
        }
        Ok(())
    }
}

impl WriteStrategy for NullableDecimal256AsText {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Text {
                max_str_len: len_text(self.scale, self.precision),
            },
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
        let mut to = column_buf.as_text_view().unwrap();

        for index in 0..from.len() {
            if from.is_null(index) {
                to.set_cell(index + param_offset, None)
            } else {
                let value = from.value(index);
                let text = value.to_string();
                to.set_cell(index + param_offset, Some(text.as_bytes()))
            }
        }
        Ok(())
    }
}

fn write_integer_as_decimal(mut n: i128, precision: u8, scale: u8, text: &mut [u8]) {
    if n.is_negative() {
        n *= n.signum();
        text[0] = b'-';
    } else {
        text[0] = b'+';
    }

    // Number of digits + one decimal separator (`.`)
    let str_len = if scale == 0 { precision } else { precision + 1 };

    let ten = 10;
    for index in (0..str_len).rev() {
        // The separator will not be printed in case of scale == 0 since index is never going to
        // reach `precision`.
        let char = if index == precision - scale {
            b'.'
        } else {
            let digit: u8 = (n % ten) as u8;
            n /= ten;
            b'0' + digit
        };
        // +1 offset to make space for sign character
        text[index as usize + 1] = char;
    }
}
