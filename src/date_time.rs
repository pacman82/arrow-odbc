use std::convert::TryInto;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use odbc_api::sys::{Date, Time, Timestamp};

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

pub fn seconds_to_time(from: i32) -> Time {
    let hour = from / 3_600;
    let minute = (from - hour * 3_600) / 60;
    let second = from - hour * 3_600 - minute * 60;
    Time {
        hour: hour.try_into().unwrap(),
        minute: minute.try_into().unwrap(),
        second: second.try_into().unwrap(),
    }
}
