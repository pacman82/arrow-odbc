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

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Float64Type, Int32Type};

    use super::*;

    #[test]
    fn nullable_copy_preserves_values_nulls_and_surrounding_rows() {
        check_copy::<Int32Type>();
        check_copy::<Float64Type>();
    }

    fn check_copy<P: ArrowPrimitiveType>()
    where
        P::Native: From<i32>,
    {
        for values in [
            vec![],
            vec![Some(10), Some(-20), Some(30)],
            vec![None, None, None],
            vec![Some(10), None, Some(-20), None, Some(30)],
        ] {
            // Slice the input so its value and validity buffers both start at a nonzero offset.
            let input = PrimitiveArray::<P>::from_iter(
                [Some(99)]
                    .into_iter()
                    .chain(values.iter().copied())
                    .chain([None])
                    .map(|value| value.map(P::Native::from)),
            );
            let sliced = Array::slice(&input, 1, values.len());
            let input = sliced.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
            let capacity = values.len() + 5;
            let mut data = vec![P::Native::from(77); capacity];
            let mut indicators = vec![123; capacity];
            copy_nullable_values(input, &mut data, &mut indicators, 3);
            for index in 0..capacity {
                if (3..3 + values.len()).contains(&index) {
                    match values[index - 3] {
                        Some(value) => {
                            assert_eq!(data[index], P::Native::from(value));
                            assert_eq!(indicators[index], 0);
                        }
                        None => assert_eq!(indicators[index], NULL_DATA),
                    }
                } else {
                    assert_eq!(data[index], P::Native::from(77));
                    assert_eq!(indicators[index], 123);
                }
            }
        }
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use arrow::datatypes::{Float64Type, Int32Type};

    use super::*;

    #[divan::bench(
        types = [Int32Type, Float64Type],
        consts = [1024, 65_535],
        args = [0, 1, 50, 100]
    )]
    fn nullable_identical<P: ArrowPrimitiveType, const ROWS: usize>(
        bencher: divan::Bencher,
        null_percent: u32,
    ) where
        P::Native: From<i32>,
    {
        let input = PrimitiveArray::<P>::from_iter((0..ROWS).map(|index| {
            // A deterministic scattered mask, without random generation in the measured loop.
            let bucket = ((index as u32).wrapping_mul(2_654_435_761) >> 16) % 100;
            (bucket >= null_percent).then(|| P::Native::from(index as i32 - 32_768))
        }));
        let mut values = vec![P::Native::default(); ROWS];
        let mut indicators = vec![NULL_DATA; ROWS];

        bencher.bench_local(|| {
            copy_nullable_values(
                divan::black_box(&input),
                divan::black_box(&mut values),
                divan::black_box(&mut indicators),
                0,
            );
        });
    }
}
