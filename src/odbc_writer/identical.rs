use std::marker::PhantomData;

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnSliceMut, BufferDescription, Item};

use crate::WriterError;

use super::WriteStrategy;

/// Write strategy for situations there arrow primitive native and odbc C Data type are identical.
pub fn identical<P>(nullable: bool) -> Box<dyn WriteStrategy>
where
    P: ArrowPrimitiveType,
    P::Native: Item,
{
    if nullable {
        Box::new(Nullable::<P>::new())
    } else {
        Box::new(NonNullable::<P>::new())
    }
}

struct Nullable<P> {
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

struct NonNullable<P> {
    _phantom: PhantomData<P>,
}

impl<P> NonNullable<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P> WriteStrategy for NonNullable<P>
where
    P: ArrowPrimitiveType,
    P::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
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
        let to = column_buf.as_slice::<P::Native>().unwrap();
        to[param_offset..(param_offset + from.len())].copy_from_slice(from.values());
        Ok(())
    }
}
