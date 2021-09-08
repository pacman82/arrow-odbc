use std::{convert::TryInto, sync::Arc};

use arrow::array::{ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder};
use odbc_api::buffers::{AnyColumnView, BufferDescription, BufferKind};

use super::ColumnStrategy;

pub struct Binary {
    /// Maximum length in bytes of elements
    max_len: usize,
    nullable: bool,
}

impl Binary {
    pub fn new(nullable: bool, max_len: usize) -> Self {
        Self { max_len, nullable }
    }
}

impl ColumnStrategy for Binary {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.nullable,
            kind: BufferKind::Binary {
                length: self.max_len,
            },
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = match column_view {
            AnyColumnView::Binary(values) => values,
            _ => unreachable!(),
        };
        let mut builder = BinaryBuilder::new(values.len());
        for value in values {
            if let Some(bytes) = value {
                builder.append_value(bytes).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        Arc::new(builder.finish())
    }
}

pub struct FixedSizedBinary {
    /// Length in bytes of elements
    len: usize,
    nullable: bool,
}

impl FixedSizedBinary {
    pub fn new(nullable: bool, len: usize) -> Self {
        Self { len, nullable }
    }
}

impl ColumnStrategy for FixedSizedBinary {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: self.nullable,
            kind: BufferKind::Binary { length: self.len },
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = match column_view {
            AnyColumnView::Binary(values) => values,
            _ => unreachable!(),
        };
        let mut builder = FixedSizeBinaryBuilder::new(values.len(), self.len.try_into().unwrap());
        for value in values {
            if let Some(bytes) = value {
                builder.append_value(bytes).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        Arc::new(builder.finish())
    }
}
