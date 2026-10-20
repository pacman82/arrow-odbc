use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::{ArrayData, ArrayRef, PrimitiveArray, PrimitiveBuilder},
    buffer::Buffer,
    datatypes::ArrowPrimitiveType,
};
use chrono::NaiveDateTime;
use odbc_api::{
    Pod,
    buffers::{AnyColumnBufferSlice, BufferDesc, Item},
    sys::NULL_DATA,
};
use thiserror::Error;

use super::ReadStrategy;

/// Extend an arrow primitive type to serve as a builder for Read strategies.
pub trait MapOdbcToArrow {
    type ArrowElement;

    /// Use the provided function to convert an element of an ODBC column buffer into the desired
    /// element of an arrow array. This method assumes the conversion is falliable.
    fn map_falliable<U>(
        nullable: bool,
        map_errors_to_null: bool,
        odbc_to_arrow: impl Fn(&U) -> Result<Self::ArrowElement, MappingError> + 'static + Send,
    ) -> Box<dyn ReadStrategy + Send>
    where
        U: Item + Pod + 'static + Send;

    /// Use the infallible function provided to convert an element of an ODBC column buffer into the
    /// desired element of an arrow array.
    fn map_infalliable<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Self::ArrowElement + 'static + Send,
    ) -> Box<dyn ReadStrategy + Send>
    where
        U: Item + Pod + 'static + Send;

    /// Should the arrow array element be identical to an item in the ODBC buffer no mapping is
    /// needed. We still need to account for nullability.
    fn identical(nullable: bool) -> Box<dyn ReadStrategy + Send>
    where
        Self::ArrowElement: Item + Pod;
}

impl<T> MapOdbcToArrow for T
where
    T: ArrowPrimitiveType + Send,
{
    type ArrowElement = T::Native;

    fn map_falliable<U>(
        nullable: bool,
        map_errors_to_null: bool,
        odbc_to_arrow: impl Fn(&U) -> Result<Self::ArrowElement, MappingError> + 'static + Send,
    ) -> Box<dyn ReadStrategy + Send>
    where
        U: Item + Pod + 'static + Send,
    {
        if map_errors_to_null {
            return Box::new(ErrorToNullStrategy::<Self, U, _>::new(odbc_to_arrow));
        }

        if nullable {
            return Box::new(NullableStrategy::<Self, U, _>::new(odbc_to_arrow));
        }

        Box::new(NonNullableStrategy::<Self, U, _>::new(odbc_to_arrow))
    }

    fn map_infalliable<U>(
        nullable: bool,
        odbc_to_arrow: impl Fn(&U) -> Self::ArrowElement + 'static + Send,
    ) -> Box<dyn ReadStrategy + Send>
    where
        U: Item + Pod + 'static + Send,
    {
        if nullable {
            Box::new(NullableStrategy::<Self, U, _>::new(OkWrappedMapped(
                odbc_to_arrow,
            )))
        } else {
            Box::new(NonNullableStrategy::<Self, U, _>::new(OkWrappedMapped(
                odbc_to_arrow,
            )))
        }
    }

    fn identical(nullable: bool) -> Box<dyn ReadStrategy + Send>
    where
        T::Native: Item + Pod,
    {
        if nullable {
            Box::new(NullableDirectStrategy::<Self>::new())
        } else {
            Box::new(NonNullDirectStrategy::<Self>::new())
        }
    }
}

/// We introduce this trait instead of using the Fn(...) trait syntax directly, in order to being
/// able to provide an implementation for `OkWrappedMapped`. Which in turn we need to reuse our
/// Strategy implementations for falliable and infalliable cases.
///
/// We could save our selves all of this if Rust would be better at figuring out then to promote
/// the lifetimes in closures to higher order liftimes, but at time of writing this, I've not been
/// able to Ok wrapping with a straight forward lambda `|e| Ok(f(e))``. (Current version 1.79).
///
/// Since Fn traits can not be implemented manually either we introduce this one.
trait MapElement<O, A> {
    fn map_element(&self, odbc: &O) -> Result<A, MappingError>;
}

impl<T, O, A> MapElement<O, A> for T
where
    T: Fn(&O) -> Result<A, MappingError>,
{
    fn map_element(&self, odbc: &O) -> Result<A, MappingError> {
        self(odbc)
    }
}

struct OkWrappedMapped<F>(F);

impl<F, O, A> MapElement<O, A> for OkWrappedMapped<F>
where
    F: Fn(&O) -> A,
{
    fn map_element(&self, odbc: &O) -> Result<A, MappingError> {
        Ok((self.0)(odbc))
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
    T: ArrowPrimitiveType + Send,
    T::Native: Item + Pod,
{
    fn buffer_desc(&self) -> BufferDesc {
        T::Native::buffer_desc(false)
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let slice = column_view.as_slice::<T::Native>().unwrap();
        let mut builder = PrimitiveBuilder::<T>::with_capacity(slice.len());
        builder.append_slice(slice);
        Ok(Arc::new(builder.finish()))
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
    T: ArrowPrimitiveType + Send,
    T::Native: Item + Pod,
{
    fn buffer_desc(&self) -> BufferDesc {
        T::Native::buffer_desc(true)
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let (values, indicators) = column_view
            .as_nullable_slice::<T::Native>()
            .unwrap()
            .raw_values();
        let validity = indicators.contains(&NULL_DATA).then(|| {
            indicators
                .iter()
                .map(|&indicator| indicator != NULL_DATA)
                .collect::<Buffer>()
        });
        // Copy values in bulk; the validity bitmap masks any stale values beneath nulls.
        let data = ArrayData::builder(T::DATA_TYPE)
            .len(values.len())
            .add_buffer(Buffer::from_slice_ref(values))
            .null_bit_buffer(validity)
            .build()
            .unwrap();
        Ok(Arc::new(PrimitiveArray::<T>::from(data)))
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
    P: ArrowPrimitiveType + Send,
    O: Item + Pod + Send,
    F: MapElement<O, P::Native>,
{
    fn buffer_desc(&self) -> BufferDesc {
        O::buffer_desc(false)
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let slice = column_view.as_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::with_capacity(slice.len());
        for odbc_value in slice {
            builder.append_value(self.odbc_to_arrow.map_element(odbc_value)?);
        }
        Ok(Arc::new(builder.finish()))
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
    P: ArrowPrimitiveType + Send,
    O: Item + Pod + Send,
    F: MapElement<O, P::Native>,
{
    fn buffer_desc(&self) -> BufferDesc {
        O::buffer_desc(true)
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let opts = column_view.as_nullable_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::with_capacity(opts.len());
        for odbc_opt in opts {
            builder.append_option(
                odbc_opt
                    .map(|odbc_element| self.odbc_to_arrow.map_element(odbc_element))
                    .transpose()?,
            );
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// Map invalid values to `NULL` rather than emitting a [`MappingError`]`.
struct ErrorToNullStrategy<P, O, F> {
    _primitive_type: PhantomData<P>,
    _odbc_item: PhantomData<O>,
    odbc_to_arrow: F,
}

impl<P, O, F> ErrorToNullStrategy<P, O, F> {
    fn new(odbc_to_arrow: F) -> Self {
        Self {
            _primitive_type: PhantomData,
            _odbc_item: PhantomData,
            odbc_to_arrow,
        }
    }
}

impl<P, O, F> ReadStrategy for ErrorToNullStrategy<P, O, F>
where
    P: ArrowPrimitiveType + Send,
    O: Item + Pod + Send,
    F: Fn(&O) -> Result<P::Native, MappingError> + Send,
{
    fn buffer_desc(&self) -> BufferDesc {
        O::buffer_desc(true)
    }

    fn fill_arrow_array(
        &self,
        column_view: AnyColumnBufferSlice,
    ) -> Result<ArrayRef, MappingError> {
        let opts = column_view.as_nullable_slice::<O>().unwrap();
        let mut builder = PrimitiveBuilder::<P>::with_capacity(opts.len());
        for odbc_opt in opts {
            builder.append_option(odbc_opt.and_then(|val| (self.odbc_to_arrow)(val).ok()));
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// The source value returned from the ODBC datasource is out of range and can not be mapped into
/// its Arrow target type.
#[derive(Error, Debug)]
pub enum MappingError {
    #[error(
        "Timestamp is not representable in arrow: {value}\n\
        Timestamps with nanoseconds precision are represented using a signed 64 Bit integer. This \
        limits their range to values between 1677-09-21 00:12:44 and 2262-04-11 \
        23:47:16.854775807. The value returned from the database is outside of this range. \
        Suggestions to fix this error either reduce the precision or fetch the values as text."
    )]
    OutOfRangeTimestampNs { value: NaiveDateTime },
    #[error("Datasource returned invalid UTF-8. Lossy representation of value: {lossy_value}")]
    InvalidUtf8 { lossy_value: String },
}

#[cfg(any(test, feature = "internal_benches"))]
mod test_data {
    use odbc_api::{buffers::BoxColumnBuffer, sys::NULL_DATA};

    use super::Item;

    pub fn is_null(index: usize, percent: u64) -> bool {
        // Deterministic scattered nulls, including at bitmap boundaries.
        let sample = (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32;
        sample % 100 < percent
    }

    pub fn nullable_column<T: Item>(values: &[T], percent: u64) -> BoxColumnBuffer {
        let mut column = T::buffer_desc(true).column_buffer(values.len());
        // SAFETY: The description allocates values.len() initialized T values and isize indicators.
        // This fixture owns the unbound buffer, and these pointers remain valid during the writes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                values.as_ptr(),
                column.mut_value_ptr().cast::<T>(),
                values.len(),
            );
            let indicators =
                std::slice::from_raw_parts_mut(column.mut_indicator_ptr(), values.len());
            for (index, indicator) in indicators.iter_mut().enumerate() {
                *indicator = if is_null(index, percent) {
                    NULL_DATA
                } else {
                    0
                };
            }
        }
        column
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::PrimitiveArray,
        datatypes::{Float64Type, Int32Type},
    };
    use odbc_api::buffers::Slice;

    use super::{ArrowPrimitiveType, Item, NullableDirectStrategy, ReadStrategy, test_data};

    fn check_nullable<T>(values: &[T::Native])
    where
        T: ArrowPrimitiveType + Send,
        T::Native: Item,
    {
        let strategy = NullableDirectStrategy::<T>::new();
        for percent in [0, 1, 50, 100] {
            let column = test_data::nullable_column(values, percent);
            // Include empty and partial batches and both sides of bitmap byte/word boundaries.
            for rows in [0, 1, 7, 8, 9, 63, 64, 65, values.len()] {
                let actual = strategy.fill_arrow_array(column.slice(rows)).unwrap();
                let actual = actual.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
                let expected: Vec<_> = values[..rows]
                    .iter()
                    .enumerate()
                    .map(|(index, &value)| (!test_data::is_null(index, percent)).then_some(value))
                    .collect();
                // ODBC may leave stale values under nulls; compare only semantic values.
                assert_eq!(actual.iter().collect::<Vec<_>>(), expected);
            }
        }
    }

    #[test]
    fn nullable_integers_preserve_values_and_validity() {
        let mut values: Vec<i32> = (-33..34).collect();
        values[1] = i32::MIN;
        values[2] = i32::MAX;
        check_nullable::<Int32Type>(&values);
    }

    #[test]
    fn nullable_floats_preserve_values_and_validity() {
        let mut values: Vec<f64> = (-33..34).map(|value| value as f64 / 4.0).collect();
        values[1] = f64::NEG_INFINITY;
        values[2] = f64::INFINITY;
        check_nullable::<Float64Type>(&values);
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use arrow::datatypes::{Float64Type, Int32Type};
    use odbc_api::buffers::Slice;

    use super::{ArrowPrimitiveType, Item, NullableDirectStrategy, ReadStrategy, test_data};

    #[divan::bench(
        types = [Int32Type, Float64Type],
        consts = [1024, 65535],
        args = [0, 1, 50, 100],
    )]
    fn nullable_direct<T, const ROWS: usize>(bencher: divan::Bencher, null_percent: u64)
    where
        T: ArrowPrimitiveType + Send,
        T::Native: Item + From<i32>,
    {
        let values: Vec<_> = (0..ROWS)
            .map(|index| T::Native::from(index as i32 - ROWS as i32 / 2))
            .collect();
        let column = test_data::nullable_column(&values, null_percent);
        let strategy = NullableDirectStrategy::<T>::new();
        bencher
            .counter(divan::counter::ItemsCount::new(ROWS))
            .bench_local(|| {
                let array = strategy
                    .fill_arrow_array(divan::black_box(column.slice(ROWS)))
                    .unwrap();
                // Include destruction, as well as allocating and populating each Arrow array.
                drop(divan::black_box(array));
            });
    }
}
