use std::marker::PhantomData;

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
};
use odbc_api::{
    BindParamDesc,
    buffers::{BoxColumBufferRefMut, Item},
    sys::NULL_DATA,
};

use crate::WriterError;

use super::WriteStrategy;

pub trait MapArrowToOdbc {
    type ArrowElement;

    fn map_with<U>(
        nullable: bool,
        arrow_to_odbc: impl Fn(Self::ArrowElement) -> U + 'static,
    ) -> Box<dyn WriteStrategy>
    where
        U: Item;

    fn identical(nullable: bool) -> Box<dyn WriteStrategy>
    where
        Self::ArrowElement: Item;
}

impl<T> MapArrowToOdbc for T
where
    T: ArrowPrimitiveType,
{
    type ArrowElement = T::Native;

    fn map_with<U>(
        nullable: bool,
        arrow_to_odbc: impl Fn(Self::ArrowElement) -> U + 'static,
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

    fn identical(nullable: bool) -> Box<dyn WriteStrategy>
    where
        Self::ArrowElement: Item,
    {
        if nullable {
            Box::new(NullableIdentical::<Self>::new())
        } else {
            Box::new(NonNullableIdentical::<Self>::new())
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
    fn buffer_desc(&self) -> BindParamDesc {
        U::bind_param_desc(true)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
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
    fn buffer_desc(&self) -> BindParamDesc {
        U::bind_param_desc(false)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
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

struct NullableIdentical<P> {
    _phantom: PhantomData<P>,
}

impl<P> NullableIdentical<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P> WriteStrategy for NullableIdentical<P>
where
    P: ArrowPrimitiveType,
    P::Native: Item,
{
    fn buffer_desc(&self) -> BindParamDesc {
        P::Native::bind_param_desc(true)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let mut to = column_buf.as_nullable_slice::<P::Native>().unwrap();
        let (values, indicators) = to.raw_values();
        copy_nullable_values(from, values, indicators, param_offset);
        Ok(())
    }
}

fn copy_nullable_values<P: ArrowPrimitiveType>(
    from: &PrimitiveArray<P>,
    values: &mut [P::Native],
    indicators: &mut [isize],
    param_offset: usize,
) {
    let end = param_offset + from.len();
    let indicators = &mut indicators[param_offset..end];
    let null_count = from.null_count();
    if null_count == from.len() {
        indicators.fill(NULL_DATA);
        return;
    }

    values[param_offset..end].copy_from_slice(from.values());
    if null_count == 0 {
        indicators.fill(0);
    } else {
        for (index, indicator) in indicators.iter_mut().enumerate() {
            *indicator = if from.is_valid(index) { 0 } else { NULL_DATA };
        }
    }
}

struct NonNullableIdentical<P> {
    _phantom: PhantomData<P>,
}

impl<P> NonNullableIdentical<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P> WriteStrategy for NonNullableIdentical<P>
where
    P: ArrowPrimitiveType,
    P::Native: Item,
{
    fn buffer_desc(&self) -> BindParamDesc {
        P::Native::bind_param_desc(false)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let to = column_buf.as_slice::<P::Native>().unwrap();
        to[param_offset..(param_offset + from.len())].copy_from_slice(from.values());
        Ok(())
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use arrow::datatypes::{Float64Type, Int32Type};

    use super::*;

    const ROWS: usize = 1000;

    #[divan::bench(types = [Int32Type, Float64Type])]
    fn nullable_write_without_nulls<P: ArrowPrimitiveType>(bencher: divan::Bencher) {
        let input = nullable_array::<P>(|_| false);
        measure_copy(bencher, &input);
    }

    #[divan::bench(types = [Int32Type, Float64Type])]
    fn nullable_write_with_nulls<P: ArrowPrimitiveType>(bencher: divan::Bencher) {
        let input = nullable_array::<P>(|row| row % 100 == 0);
        measure_copy(bencher, &input);
    }

    #[divan::bench(types = [Int32Type, Float64Type])]
    fn nullable_write_all_nulls<P: ArrowPrimitiveType>(bencher: divan::Bencher) {
        let input = nullable_array::<P>(|_| true);
        measure_copy(bencher, &input);
    }

    fn measure_copy<P: ArrowPrimitiveType>(bencher: divan::Bencher, input: &PrimitiveArray<P>) {
        let mut values = vec![P::Native::default(); ROWS];
        let mut indicators = vec![NULL_DATA; ROWS];
        bencher.bench_local(|| {
            copy_nullable_values(
                divan::black_box(input),
                divan::black_box(&mut values),
                divan::black_box(&mut indicators),
                0,
            );
        });
    }

    /// Array with `ROWS` elements, which are null wherever `is_null` returns `true`.
    fn nullable_array<P: ArrowPrimitiveType>(is_null: impl Fn(usize) -> bool) -> PrimitiveArray<P> {
        PrimitiveArray::from_iter(
            (0..ROWS).map(|row| (!is_null(row)).then_some(P::Native::default())),
        )
    }
}
