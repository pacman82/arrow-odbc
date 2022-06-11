use std::{sync::Arc};

use arrow::{
    array::{ArrayRef, PrimitiveBuilder},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::buffers::{AnyColumnView, BufferDescription, Item};

use super::ReadStrategy;

pub trait Conversion {
    /// ODBC buffer item type
    type Odbc: Item;
    /// Arrow Primitive type
    type Arrow: ArrowPrimitiveType;

    /// Convert from ODBC to Arrow type
    fn convert(&self, from: &Self::Odbc) -> <Self::Arrow as ArrowPrimitiveType>::Native;
}

/// This is applicable whenever there is a Primitive Arrow array whose native type is identical with
/// the ODBC buffer type.
pub fn with_conversion<C: Conversion + 'static>(
    nullable: bool,
    conversion: C,
) -> Box<dyn ReadStrategy> {
    if nullable {
        Box::new(NullableStrategy::new(conversion))
    } else {
        Box::new(NonNullStrategy::new(conversion))
    }
}

struct NonNullStrategy<C> {
    conversion: C,
}

impl<C> NonNullStrategy<C> {
    fn new(conversion: C) -> Self {
        Self { conversion }
    }
}

impl<C> ReadStrategy for NonNullStrategy<C>
where
    C: Conversion,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: C::Odbc::BUFFER_KIND,
            nullable: false,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let slice = C::Odbc::as_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<C::Arrow>::new(slice.len());
        for odbc_value in slice {
            builder
                .append_value(self.conversion.convert(odbc_value))
                .unwrap();
        }
        Arc::new(builder.finish())
    }
}

struct NullableStrategy<C> {
    conversion: C,
}

impl<C> NullableStrategy<C> {
    fn new(conversion: C) -> Self {
        Self { conversion }
    }
}

impl<C> ReadStrategy for NullableStrategy<C>
where
    C: Conversion,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            kind: C::Odbc::BUFFER_KIND,
            nullable: true,
        }
    }

    fn fill_arrow_array(&self, column_view: AnyColumnView) -> ArrayRef {
        let opts = C::Odbc::as_nullable_slice(column_view).unwrap();
        let mut builder = PrimitiveBuilder::<C::Arrow>::new(opts.len());
        for odbc_opt in opts {
            builder
                .append_option(odbc_opt.map(|odbc_value| self.conversion.convert(odbc_value)))
                .unwrap();
        }
        Arc::new(builder.finish())
    }
}
