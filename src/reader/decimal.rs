use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Builder};
use odbc_api::{
    buffers::{AnyColumnBufferSlice, BufferDesc},
    decimal_text_to_i128,
};

use super::{MappingError, ReadStrategy};

pub fn decimal(precision: u8, scale: i8) -> Box<dyn ReadStrategy + Send> {
    Box::new(Decimal { precision, scale })
}

struct Decimal {
    precision: u8,
    /// We know scale to be non-negative, yet we can save us some conversions storing it as i8.
    scale: i8,
}

impl ReadStrategy for Decimal {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            // Must be able to hold num precision digits a sign and a decimal point
            max_str_len: self.precision as usize + 2,
        }
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text().unwrap();
        let mut builder = Decimal128Builder::new();
        let scale = self.scale as usize;

        for opt in view.iter() {
            if let Some(text) = opt {
                let num = decimal_text_to_i128(text, scale);
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

#[cfg(feature = "internal_benches")]
mod bench {
    #[divan::bench()]
    fn empty() {}
}
