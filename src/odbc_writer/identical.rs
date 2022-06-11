use arrow::array::Int8Array;
use odbc_api::buffers::{BufferDescription, AnyColumnSliceMut, BufferKind};

use crate::WriterError;

use super::WriteStrategy;

pub struct NullableInt8;

impl WriteStrategy for NullableInt8 {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription { nullable: true, kind: BufferKind::I8 }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn arrow::array::Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<Int8Array>().unwrap();
        let mut to = column_buf.as_nullable_slice::<i8>().unwrap();
        for (index, cell) in from.iter().enumerate() {
            to.set_cell(index + param_offset, cell);
        }
        Ok(())
    }
}