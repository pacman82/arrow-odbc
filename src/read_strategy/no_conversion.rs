use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveBuilder},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnView, BufferDescription, Item};

use super::ReadStrategy;

/// This is applicable thenever there is a Primitive Arrow array whose native type is identical with
/// the ODBC buffer type.
pub fn no_conversion<T>(nullable: bool) -> Box<dyn ReadStrategy>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    if nullable {
        Box::new(NullableDirectStrategy::<T>::new())
    } else {
        Box::new(NonNullDirectStrategy::<T>::new())
    }
}

struct NonNullDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NonNullDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ReadStrategy for NonNullDirectStrategy<T>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: T::Native::BUFFER_KIND,
            nullable: false,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let slice = T::Native::as_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::new(slice.len());
        builder.append_slice(slice).unwrap();
        Arc::new(builder.finish())
    }
}

struct NullableDirectStrategy<T> {
    phantom: PhantomData<T>,
}

impl<T> NullableDirectStrategy<T> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T> ReadStrategy for NullableDirectStrategy<T>
where
    T: ArrowPrimitiveType,
    T::Native: Item,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: T::Native::BUFFER_KIND,
            nullable: true,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let values = T::Native::as_nullable_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<T>::new(values.len());
        for value in values {
            builder.append_option(value.copied()).unwrap();
        }
        Arc::new(builder.finish())
    }
}
