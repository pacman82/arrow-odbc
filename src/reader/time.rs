use std::{
    ops::{Add, Mul},
    sync::Arc,
};

use arrow::array::{
    ArrayRef, Time32MillisecondBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
};
use atoi::FromRadix10;
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::Time,
};

use super::{MappingError, ReadStrategy};

pub fn seconds_since_midnight(time: &Time) -> i32 {
    (time.hour as i32 * 60 + time.minute as i32) * 60 + time.second as i32
}

/// Strategy for fetching the time as text and parsing it into an `i32` which represents
/// milliseconds after midnight.
pub struct TimeMsI32;

impl ReadStrategy for TimeMsI32 {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            // Expected format is HH:MM:SS.fff
            max_str_len: 12,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = Time32MillisecondBuilder::new();

        for opt in view.iter() {
            if let Some(text) = opt {
                let num = ticks_since_midnights_from_text::<i32>(text, 3);
                builder.append_value(num);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// Strategy for fetching the time as text and parsing it into an `i32` which represents
/// milliseconds after midnight.
pub struct TimeUsI64;

impl ReadStrategy for TimeUsI64 {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            // Expected format is HH:MM:SS.ffffff
            max_str_len: 15,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = Time64MicrosecondBuilder::new();

        for opt in view.iter() {
            if let Some(text) = opt {
                let num = ticks_since_midnights_from_text::<i64>(text, 6);
                builder.append_value(num);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// Strategy for fetching the time as text and parsing it into an `i32` which represents
/// milliseconds after midnight.
pub struct TimeNsI64;

impl ReadStrategy for TimeNsI64 {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            // Expected format is HH:MM:SS.fffffffff
            max_str_len: 18,
        }
    }

    fn fill_arrow_array(&self, column_view: AnySlice) -> Result<ArrayRef, MappingError> {
        let view = column_view.as_text_view().unwrap();
        let mut builder = Time64NanosecondBuilder::new();

        for opt in view.iter() {
            if let Some(text) = opt {
                let num = ticks_since_midnights_from_text::<i64>(text, 9);
                builder.append_value(num);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

fn ticks_since_midnights_from_text<I>(text: &[u8], precision: u32) -> I
where
    I: Tick,
{
    // HH:MM:SS.fff
    // 012345678901
    let (hours, hours_digits) = I::from_radix_10(&text[0..2]);
    debug_assert_eq!(2, hours_digits);
    debug_assert_eq!(b':', text[2]);
    let (min, min_digits) = I::from_radix_10(&text[3..5]);
    debug_assert_eq!(2, min_digits);
    debug_assert_eq!(b':', text[5]);
    let (sec, sec_digits) = I::from_radix_10(&text[6..8]);
    debug_assert_eq!(2, sec_digits);
    // check for fractional part
    let (frac, frac_digits) = if text.len() > 9 {
        I::from_radix_10(&text[9..])
    } else {
        (I::ZERO, 0)
    };
    let frac = frac * I::TEN.pow(precision - frac_digits as u32);
    ((hours * I::SIXTY + min) * I::SIXTY + sec) * I::TEN.pow(precision) + frac
}

trait Tick: FromRadix10 + Mul<Output = Self> + Add<Output = Self> {
    const ZERO: Self;
    const TEN: Self;
    const SIXTY: Self;

    fn pow(self, exp: u32) -> Self;
}

impl Tick for i32 {
    const ZERO: Self = 0;
    const TEN: Self = 10;
    const SIXTY: Self = 60;

    fn pow(self, exp: u32) -> Self {
        self.pow(exp)
    }
}

impl Tick for i64 {
    const ZERO: Self = 0;
    const TEN: Self = 10;
    const SIXTY: Self = 60;

    fn pow(self, exp: u32) -> Self {
        self.pow(exp)
    }
}
