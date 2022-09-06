use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveBuilder},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnView, BufferDescription, Item};

use super::ReadStrategy;

pub trait MapOdbcToArrow {
    type ArrowElement;

    fn map_with<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Self::ArrowElement + 'static,
    ) -> Box<dyn ReadStrategy>
    where
        U: Item + 'static;

    fn identical(nullable: bool) -> Box<dyn ReadStrategy>
    where
        Self::ArrowElement: Item;
}

impl<T> MapOdbcToArrow for T
where
    T: ArrowPrimitiveType,
{
    type ArrowElement = T::Native;

    fn map_with<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Self::ArrowElement + 'static,
    ) -> Box<dyn ReadStrategy>
    where
        U: Item + 'static,
    {
        if nullable {
            Box::new(NullableStrategy::<Self, U, _>::new(odbc_to_arrow))
        } else {
            Box::new(NonNullableStrategy::<Self, U, _>::new(odbc_to_arrow))
        }
    }

    fn identical(nullable: bool) -> Box<dyn ReadStrategy>
    where
        Self::ArrowElement: Item,
    {
        if nullable {
            Box::new(NullableDirectStrategy::<Self>::new())
        } else {
            Box::new(NonNullDirectStrategy::<Self>::new())
        }
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
        let mut builder = PrimitiveBuilder::<T>::new();
        builder.append_slice(slice);
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
        let mut builder = PrimitiveBuilder::<T>::new();
        for value in values {
            builder.append_option(value.copied());
        }
        Arc::new(builder.finish())
    }
}

struct NonNullableStrategy<P, O, F> {
    _primitive_type: PhantomData<P>,
    _odbc_item: PhantomData<O>,
    odbc_to_arrow: F,
}

impl<P, O, F> NonNullableStrategy<P, O, F> {
    fn new(odbc_to_arrow: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            _odbc_item: PhantomData,
            odbc_to_arrow,
        }
    }
}

impl<P, O, F> ReadStrategy for NonNullableStrategy<P, O, F>
where
    P: ArrowPrimitiveType,
    O: Item,
    F: Fn(&O) -> P::Native,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: O::BUFFER_KIND,
            nullable: false,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let slice = column_view.as_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::new();
        for odbc_value in slice {
            builder.append_value((self.odbc_to_arrow)(odbc_value));
        }
        Arc::new(builder.finish())
    }
}

struct NullableStrategy<P, O, F> {
    _primitive_type: PhantomData<P>,
    _odbc_item: PhantomData<O>,
    odbc_to_arrow: F,
}

impl<P, O, F> NullableStrategy<P, O, F> {
    fn new(odbc_to_arrow: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            _odbc_item: PhantomData,
            odbc_to_arrow,
        }
    }
}

impl<P, O, F> ReadStrategy for NullableStrategy<P, O, F>
where
    P: ArrowPrimitiveType,
    O: Item,
    F: Fn(&O) -> P::Native,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: O::BUFFER_KIND,
            nullable: true,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let opts = column_view.as_nullable_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::new();
        for odbc_opt in opts {
            builder.append_option(odbc_opt.map(&self.odbc_to_arrow));
        }
        Arc::new(builder.finish())
    }
}
