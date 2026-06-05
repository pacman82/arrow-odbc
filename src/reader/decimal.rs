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
        let mut builder = Decimal128Builder::with_capacity(view.len());
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
    use super::decimal;
    use odbc_api::buffers::{BoxColumnBuffer, Slice, TextColumn};
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    const SEED: u64 = 0xDEAD_BEEF_F00D_CAFE;
    const ROWS: usize = 1000;

    #[divan::bench()]
    fn integer(bencher: divan::Bencher) {
        let strategy = decimal(38, 0);
        let input = random_integers_in_text_column();
        bencher.bench_local(|| {
            let view = input.slice(ROWS);
            strategy.fill_arrow_array(view).unwrap()
        });
    }

    fn random_integers_in_text_column() -> BoxColumnBuffer {
        let mut buffer = TextColumn::<u8>::new(ROWS, 38 + 2);
        let mut rng = StdRng::seed_from_u64(SEED);
        let min = -(10i128.pow(38));
        let max = 10i128.pow(38);
        for i in 0..ROWS {
            let value: i128 = rng.random_range(min..=max);
            buffer.set_value(i, Some(value.to_string().as_bytes()));
        }
        Box::new(buffer)
    }
}
