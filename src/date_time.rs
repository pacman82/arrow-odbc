use std::{
    convert::TryInto,
    fmt::Display,
    io::Write,
    marker::PhantomData,
    ops::{Div, Mul, Rem},
};

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::{
        ArrowPrimitiveType, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType,
    },
};
use chrono::{Datelike, NaiveDate};
use odbc_api::{
    buffers::{AnySliceMut, BufferDesc, TextColumnSliceMut},
    sys::{Date, Time, Timestamp},
};

use crate::{WriterError, odbc_writer::WriteStrategy, reader::MappingError};

/// Transform date to days since unix epoch as i32
pub fn days_since_epoch(date: &Date) -> i32 {
    let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date =
        NaiveDate::from_ymd_opt(date.year as i32, date.month as u32, date.day as u32).unwrap();
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}

pub fn seconds_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd_opt(from.year as i32, from.month as u32, from.day as u32)
        .unwrap()
        .and_hms_opt(from.hour as u32, from.minute as u32, from.second as u32)
        .unwrap();
    ndt.and_utc().timestamp()
}

pub fn ms_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd_opt(from.year as i32, from.month as u32, from.day as u32)
        .unwrap()
        .and_hms_nano_opt(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        )
        .unwrap();
    ndt.and_utc().timestamp_millis()
}

pub fn us_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd_opt(from.year as i32, from.month as u32, from.day as u32)
        .unwrap()
        .and_hms_nano_opt(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        )
        .unwrap();
    ndt.and_utc().timestamp_micros()
}

pub fn ns_since_epoch(from: &Timestamp) -> Result<i64, MappingError> {
    let ndt = NaiveDate::from_ymd_opt(from.year as i32, from.month as u32, from.day as u32)
        .unwrap()
        .and_hms_nano_opt(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        )
        .unwrap();

    // The dates that can be represented as nanoseconds are between 1677-09-21T00:12:44.0 and
    // 2262-04-11T23:47:16.854775804
    ndt.and_utc()
        .timestamp_nanos_opt()
        .ok_or(MappingError::OutOfRangeTimestampNs { value: ndt })
}

pub fn epoch_to_date(from: i32) -> Date {
    // Offset between between ce and unix epoch
    const OFFSET: i32 = 719_163;
    let nd = NaiveDate::from_num_days_from_ce_opt(from + OFFSET).unwrap();
    Date {
        year: nd.year().try_into().unwrap(),
        month: nd.month().try_into().unwrap(),
        day: nd.day().try_into().unwrap(),
    }
}

pub fn sec_since_midnight_to_time(from: i32) -> Time {
    let unit_min = 60;
    let unit_hour = unit_min * 60;
    let hour = from / unit_hour;
    let minute = (from % unit_hour) / unit_min;
    let second = from % unit_min;
    Time {
        hour: hour.try_into().unwrap(),
        minute: minute.try_into().unwrap(),
        second: second.try_into().unwrap(),
    }
}

pub struct NullableTimeAsText<P> {
    _phantom: PhantomData<P>,
}

impl<P> NullableTimeAsText<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

pub trait TimePrimitive {
    type Integer: From<i32>
        + Copy
        + Mul<Output = Self::Integer>
        + Div<Output = Self::Integer>
        + Rem<Output = Self::Integer>
        + Display;
    const SCALE: usize;
    const PRECISION_FACTOR: Self::Integer;
    const STR_LEN: usize;

    fn insert_at(index: usize, from: Self::Integer, to: &mut TextColumnSliceMut<u8>) {
        let sixty: Self::Integer = 60.into();
        let unit_min = sixty * Self::PRECISION_FACTOR;
        let unit_hour = unit_min * sixty;
        let hour = from / unit_hour;
        let minute = (from % unit_hour) / unit_min;
        let second = (from % unit_min) / Self::PRECISION_FACTOR;
        let fraction = from % Self::PRECISION_FACTOR;
        write!(
            to.set_mut(index, Self::STR_LEN),
            "{hour:02}:{minute:02}:{second:02}.{fraction:0s$}",
            s = Self::SCALE
        )
        .unwrap();
    }
}

impl TimePrimitive for Time32MillisecondType {
    type Integer = i32;
    const SCALE: usize = 3;
    const PRECISION_FACTOR: i32 = 1_000;
    // Length of text representation of time. HH:MM:SS.fff
    const STR_LEN: usize = 12;
}

impl TimePrimitive for Time64MicrosecondType {
    type Integer = i64;

    const SCALE: usize = 6;
    const PRECISION_FACTOR: i64 = 1_000_000;
    // Length of text representation of time. HH:MM:SS.ffffff
    const STR_LEN: usize = 15;
}

impl TimePrimitive for Time64NanosecondType {
    type Integer = i64;
    // For now we insert nanoseconds with a precision of 7 digits rather than 9
    const SCALE: usize = 9;
    const PRECISION_FACTOR: i64 = 1_000_000_000;
    // Length of text representation of time. HH:MM:SS.fffffffff
    const STR_LEN: usize = 18;
}

impl<P> WriteStrategy for NullableTimeAsText<P>
where
    P: ArrowPrimitiveType + TimePrimitive<Integer = <P as ArrowPrimitiveType>::Native>,
{
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: P::STR_LEN,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
        let mut to = column_buf.as_text_view().unwrap();
        for (index, elapsed_since_midnight) in from.iter().enumerate() {
            if let Some(from) = elapsed_since_midnight {
                P::insert_at(index + param_offset, from, &mut to)
            } else {
                to.set_cell(index + param_offset, None)
            }
        }
        Ok(())
    }
}
