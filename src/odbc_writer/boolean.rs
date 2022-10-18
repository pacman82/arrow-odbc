use arrow::array::{Array, BooleanArray};
use odbc_api::{
    buffers::{AnySliceMut, BufferDescription, BufferKind},
    Bit,
};

use crate::WriterError;

use super::WriteStrategy;

pub fn boolean_to_bit(nullable: bool) -> Box<dyn WriteStrategy> {
    if nullable {
        Box::new(Nullable)
    } else {
        Box::new(NonNullable)
    }
}

struct Nullable;

impl WriteStrategy for Nullable {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Bit,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut to = column_buf.as_nullable_slice::<Bit>().unwrap();
        for (index, cell) in from.iter().enumerate() {
            to.set_cell(index + param_offset, cell.map(Bit::from_bool))
        }
        Ok(())
    }
}

struct NonNullable;

impl WriteStrategy for NonNullable {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Bit,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let to = column_buf.as_slice::<Bit>().unwrap();
        for index in 0..from.len() {
            to[index + param_offset] = Bit::from_bool(from.value(index))
        }
        Ok(())
    }
}
