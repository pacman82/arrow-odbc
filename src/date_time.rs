use std::{convert::TryInto, io::Write, marker::PhantomData};

use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::{
        ArrowPrimitiveType, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType,
    },
};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use odbc_api::{
    buffers::{AnySliceMut, BufferDescription, BufferKind, TextColumnSliceMut},
    sys::{Date, Time, Timestamp},
};

use crate::{odbc_writer::WriteStrategy, WriterError};

/// Transform date to days since unix epoch as i32
pub fn days_since_epoch(date: &Date) -> i32 {
    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
    let date = NaiveDate::from_ymd(date.year as i32, date.month as u32, date.day as u32);
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}

pub fn seconds_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32).and_hms(
        from.hour as u32,
        from.minute as u32,
        from.second as u32,
    );
    ndt.timestamp()
}

pub fn ms_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
        .and_hms_nano(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        );
    ndt.timestamp_millis()
}

pub fn us_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
        .and_hms_nano(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        );
    ndt.timestamp_nanos() / 1_000
}

pub fn ns_since_epoch(from: &Timestamp) -> i64 {
    let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
        .and_hms_nano(
            from.hour as u32,
            from.minute as u32,
            from.second as u32,
            from.fraction,
        );
    ndt.timestamp_nanos()
}

pub fn epoch_to_timestamp<const UNIT_FACTOR: i64>(from: i64) -> Timestamp {
    let ndt = NaiveDateTime::from_timestamp(
        from / UNIT_FACTOR,
        ((from % UNIT_FACTOR) * (1_000_000_000 / UNIT_FACTOR))
            .try_into()
            .unwrap(),
    );
    let date = ndt.date();
    let time = ndt.time();
    Timestamp {
        year: date.year().try_into().unwrap(),
        month: date.month().try_into().unwrap(),
        day: date.day().try_into().unwrap(),
        hour: time.hour().try_into().unwrap(),
        minute: time.minute().try_into().unwrap(),
        second: time.second().try_into().unwrap(),
        fraction: time.nanosecond(),
    }
}

pub fn epoch_to_date(from: i32) -> Date {
    // Offset between between ce and unix epoch
    const OFFSET: i32 = 719_163;
    let nd = NaiveDate::from_num_days_from_ce(from + OFFSET);
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

pub trait TimePrimitive: ArrowPrimitiveType {
    const PRECISION_FACTOR: Self::Native;
    const STR_LEN: usize;

    fn insert_at(index: usize, from: Self::Native, to: &mut TextColumnSliceMut<u8>);
}

impl TimePrimitive for Time32MillisecondType {
    const PRECISION_FACTOR: i32 = 1_000;
    // Length of text representation of time. HH:MM::SS.fff
    const STR_LEN: usize = 12;

    fn insert_at(index: usize, from: Self::Native, to: &mut TextColumnSliceMut<u8>) {
        let unit_min = 60 * Self::PRECISION_FACTOR;
        let unit_hour = unit_min * 60;

        let hour = from / unit_hour;
        let minute = (from % unit_hour) / unit_min;
        let second = (from % unit_min) / Self::PRECISION_FACTOR;
        let fraction = from % Self::PRECISION_FACTOR;
        write!(
            to.set_mut(index, Self::STR_LEN),
            "{:02}:{:02}:{:02}.{:03}",
            hour,
            minute,
            second,
            fraction
        )
        .unwrap();
    }
}

impl TimePrimitive for Time64MicrosecondType {
    const PRECISION_FACTOR: i64 = 1_000_000;
    // Length of text representation of time. HH:MM::SS.ffffff
    const STR_LEN: usize = 15;

    fn insert_at(index: usize, from: Self::Native, to: &mut TextColumnSliceMut<u8>) {
        let unit_min = 60 * Self::PRECISION_FACTOR;
        let unit_hour = unit_min * 60;

        let hour = from / unit_hour;
        let minute = (from % unit_hour) / unit_min;
        let second = (from % unit_min) / Self::PRECISION_FACTOR;
        let fraction = from % Self::PRECISION_FACTOR;
        write!(
            to.set_mut(index, Self::STR_LEN),
            "{:02}:{:02}:{:02}.{:06}",
            hour,
            minute,
            second,
            fraction
        )
        .unwrap();
    }
}

impl TimePrimitive for Time64NanosecondType {
    const PRECISION_FACTOR: i64 = 1_000_000_000;
    // Length of text representation of time. HH:MM::SS.fffffff
    const STR_LEN: usize = 16;

    fn insert_at(index: usize, from: Self::Native, to: &mut TextColumnSliceMut<u8>) {
        let unit_min = 60 * Self::PRECISION_FACTOR;
        let unit_hour = unit_min * 60;

        let hour = from / unit_hour;
        let minute = (from % unit_hour) / unit_min;
        let second = (from % unit_min) / Self::PRECISION_FACTOR;
        let fraction = (from % Self::PRECISION_FACTOR) / 100;
        write!(
            to.set_mut(index, Self::STR_LEN),
            "{:02}:{:02}:{:02}.{:07}",
            hour,
            minute,
            second,
            fraction
        )
        .unwrap();
    }
}

impl<P> WriteStrategy for NullableTimeAsText<P>
where
    P: TimePrimitive,
{
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: false,
            kind: BufferKind::Text {
                max_str_len: P::STR_LEN,
            },
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
