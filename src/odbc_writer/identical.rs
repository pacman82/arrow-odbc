use std::marker::PhantomData;

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnSliceMut, BufferDescription, Item};

use crate::WriterError;

use super::WriteStrategy;

pub struct Nullable<P> {
    _phantom: PhantomData<P>,
}

impl<P> Nullable<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P> WriteStrategy for Nullable<P>
where
    P: ArrowPrimitiveType,
    P::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: P::Native::BUFFER_KIND,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let mut to = column_buf.as_nullable_slice::<P::Native>().unwrap();
        for (index, cell) in from.iter().enumerate() {
            to.set_cell(index + param_offset, cell);
        }
        Ok(())
    }
}
