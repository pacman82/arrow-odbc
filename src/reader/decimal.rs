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
                buf_digits.clear();
                buf_digits.extend(text.iter().filter(|&&c| c != b'.'));

                let (num, _consumed) = i128::from_radix_10_signed(&buf_digits);

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
