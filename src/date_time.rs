use std::convert::TryInto;

use chrono::NaiveDate;
use odbc_api::sys::{Date, Timestamp};

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
