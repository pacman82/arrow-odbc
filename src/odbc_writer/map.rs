use std::marker::PhantomData;

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnSliceMut, BufferDescription, Item};

use crate::WriterError;

use super::WriteStrategy;

pub trait ExtArrowPrimitiveType {
    type Native;

    fn map_with<U>(
        nullable: bool,
        arrow_to_odbc: impl Fn(Self::Native) -> U + 'static,
    ) -> Box<dyn WriteStrategy>
    where
        U: Item;
}

impl<T> ExtArrowPrimitiveType for T
where
    T: ArrowPrimitiveType,
{
    type Native = T::Native;

    fn map_with<U>(
        nullable: bool,
        arrow_to_odbc: impl Fn(Self::Native) -> U + 'static,
    ) -> Box<dyn WriteStrategy>
    where
        U: Item,
    {
        if nullable {
            Box::new(Nullable::<T, _>::new(arrow_to_odbc))
        } else {
            Box::new(NonNullable::<T, _>::new(arrow_to_odbc))
        }
    }
}

struct Nullable<P, F> {
    // We use this type information to correctly downcast from a `&dyn Array`.
    _primitive_type: PhantomData<P>,
    arrow_to_odbc: F,
}

impl<P, F> Nullable<P, F> {
    fn new(arrow_to_odbc: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            arrow_to_odbc,
        }
    }
}

impl<P, F, U> WriteStrategy for Nullable<P, F>
where
    P: ArrowPrimitiveType,
    F: Fn(P::Native) -> U,
    U: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: U::BUFFER_KIND,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let mut to = column_buf.as_nullable_slice::<U>().unwrap();
        for (index, cell) in from.iter().enumerate() {
            to.set_cell(index + param_offset, cell.map(&self.arrow_to_odbc))
        }
        Ok(())
    }
}

struct NonNullable<P, F> {
    // We use this type information to correctly downcast from a `&dyn Array`.
    _primitive_type: PhantomData<P>,
    arrow_to_odbc: F,
}

impl<P, F> NonNullable<P, F> {
    fn new(arrow_to_odbc: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            arrow_to_odbc,
        }
    }
}

impl<P, F, U> WriteStrategy for NonNullable<P, F>
where
    P: ArrowPrimitiveType,
    F: Fn(P::Native) -> U,
    U: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: U::BUFFER_KIND,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let to = column_buf.as_slice::<U>().unwrap();
        for index in 0..from.len() {
            to[index + param_offset] = (self.arrow_to_odbc)(from.value(index))
        }
        Ok(())
    }
}
