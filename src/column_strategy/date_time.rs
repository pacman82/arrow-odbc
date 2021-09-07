use std::convert::TryInto;

use arrow::datatypes::{
    Date32Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use chrono::NaiveDate;
use odbc_api::sys::{Date, Timestamp};

use super::Conversion;

/// Converts an ODBC data to an Arrow Date32
pub struct DateConversion;

impl Conversion for DateConversion {
    type Odbc = Date;
    type Arrow = Date32Type;

    fn convert(&self, from: &Self::Odbc) -> i32 {
        days_since_epoch(from)
    }
}

/// Transform date to days since unix epoch as i32
fn days_since_epoch(date: &Date) -> i32 {
    let unix_epoch = NaiveDate::from_ymd(1970, 1, 1);
    let date = NaiveDate::from_ymd(date.year as i32, date.month as u32, date.day as u32);
    let duration = date.signed_duration_since(unix_epoch);
    duration.num_days().try_into().unwrap()
}

/// Converts an ODBC Timestamp to an Arrow seconds timestamp
pub struct TimestampSecConversion;

impl Conversion for TimestampSecConversion {
    type Odbc = Timestamp;
    type Arrow = TimestampSecondType;

    fn convert(&self, from: &Self::Odbc) -> i64 {
        let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
            .and_hms(from.hour as u32, from.minute as u32, from.second as u32);
        ndt.timestamp()
    }
}
/// Converts an ODBC Timestamp to an Arrow Milliseconds timestamp
pub struct TimestampMsConversion;

impl Conversion for TimestampMsConversion {
    type Odbc = Timestamp;
    type Arrow = TimestampMillisecondType;

    fn convert(&self, from: &Self::Odbc) -> i64 {
        let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
            .and_hms_nano(
                from.hour as u32,
                from.minute as u32,
                from.second as u32,
                from.fraction,
            );
        ndt.timestamp_millis()
    }
}

/// Converts an ODBC Timestamp to an Arrow Miroseconds timestamp
pub struct TimestampUsConversion;

impl Conversion for TimestampUsConversion {
    type Odbc = Timestamp;
    type Arrow = TimestampMicrosecondType;

    fn convert(&self, from: &Self::Odbc) -> i64 {
        let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
            .and_hms_nano(
                from.hour as u32,
                from.minute as u32,
                from.second as u32,
                from.fraction,
            );
        ndt.timestamp_nanos() * 1_000
    }
}

/// Converts an ODBC Timestamp to an Arrow nanoseconds timestamp
pub struct TimestampNsConversion;

impl Conversion for TimestampNsConversion {
    type Odbc = Timestamp;
    type Arrow = TimestampNanosecondType;

    fn convert(&self, from: &Self::Odbc) -> i64 {
        let ndt = NaiveDate::from_ymd(from.year as i32, from.month as u32, from.day as u32)
            .and_hms_nano(
                from.hour as u32,
                from.minute as u32,
                from.second as u32,
                from.fraction,
            );
        ndt.timestamp_nanos()
    }
}
