use std::{convert::TryInto, sync::Arc};

use arrow::array::{ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder};
use odbc_api::buffers::{AnyColumnBufferSlice, BufferDesc};

use super::{MappingError, ReadStrategy};

pub struct Binary {
    /// Maximum length in bytes of elements
    max_len: usize,
}

impl Binary {
    pub fn new(max_len: usize) -> Self {
        Self { max_len }
    }
}

impl ReadStrategy for Binary {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Binary {
            max_bytes: self.max_len,
        }
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_binary().unwrap();
        let total = view.iter().flatten().map(<[u8]>::len).sum();
        let mut builder = BinaryBuilder::with_capacity(view.len(), total);
        for value in view.iter() {
            if let Some(bytes) = value {
                builder.append_value(bytes);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

pub struct FixedSizedBinary {
    /// Length in bytes of elements
    len: u32,
}

impl FixedSizedBinary {
    pub fn new(len: u32) -> Self {
        Self { len }
    }
}

impl ReadStrategy for FixedSizedBinary {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Binary {
            max_bytes: self.len as usize,
        }
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_binary().unwrap();
        let mut builder =
            FixedSizeBinaryBuilder::with_capacity(view.len(), self.len.try_into().unwrap());
        for value in view.iter() {
            if let Some(bytes) = value {
                builder.append_value(bytes).unwrap();
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use odbc_api::buffers::{BinColumn, BoxColumnBuffer, Slice};
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::{Binary, ReadStrategy};

    #[divan::bench]
    fn rows_10_000(bencher: divan::Bencher) {
        binary_to_arrow(bencher, 10_000);
    }

    #[divan::bench]
    fn rows_65_535(bencher: divan::Bencher) {
        binary_to_arrow(bencher, 65_535);
    }

    fn binary_to_arrow(bencher: divan::Bencher, rows: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let mut column = BinColumn::new(rows, 200);
        for index in 0..rows {
            let value: Vec<u8> = (0..rng.random_range(0..=200))
                .map(|_| rng.random())
                .collect();
            column.set_value(
                index,
                (rng.random_range(0..100) != 0).then_some(value.as_slice()),
            );
        }
        let column: BoxColumnBuffer = Box::new(column);
        let strategy = Binary::new(200);

        bencher.bench_local(|| {
            let array = strategy.fill_arrow_array(column.slice(rows)).unwrap();
            drop(divan::black_box(array));
        });
    }
}
