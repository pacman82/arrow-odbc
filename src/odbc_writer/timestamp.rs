//! Logic for inserting timestamp from Arrow Arrays into ODBC databases.

use std::{io::Write, marker::PhantomData, sync::Arc};

use arrow::{
    array::{timezone::Tz, Array, ArrowPrimitiveType, PrimitiveArray},
    datatypes::{
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    },
};
use chrono::{DateTime, Datelike, TimeZone, Timelike};
use log::debug;
use odbc_api::{
    buffers::{AnySliceMut, BufferDesc},
    sys::Timestamp,
};

use super::{WriteStrategy, WriterError, map_arrow_to_odbc::MapArrowToOdbc};

pub fn insert_timestamp_strategy(
    is_nullable: bool,
    time_unit: &TimeUnit,
    time_zone: Option<Arc<str>>,
) -> Result<Box<dyn WriteStrategy>, WriterError> {
    let ws = match (time_unit, time_zone) {
        (TimeUnit::Second, None) => {
            TimestampSecondType::map_with(is_nullable, epoch_to_timestamp_s)
        }
        (TimeUnit::Millisecond, None) => {
            TimestampMillisecondType::map_with(is_nullable, epoch_to_timestamp_ms)
        }
        (TimeUnit::Microsecond, None) => {
            TimestampMicrosecondType::map_with(is_nullable, epoch_to_timestamp_us)
        }
        (TimeUnit::Nanosecond, None) => TimestampNanosecondType::map_with(is_nullable, |ns| {
            // Drop the last to digits of precision, since we bind it with precision 7 and not 9.
            epoch_to_timestamp_ns((ns / 100) * 100)
        }),
        (TimeUnit::Second, Some(tz)) => {
            Box::new(TimestampTzToText::<TimestampSecondType>::new(tz)?)
        }
        (TimeUnit::Millisecond, Some(tz)) => {
            Box::new(TimestampTzToText::<TimestampMillisecondType>::new(tz)?)
        }
        (TimeUnit::Microsecond, Some(tz)) => {
            Box::new(TimestampTzToText::<TimestampMicrosecondType>::new(tz)?)
        }
        (TimeUnit::Nanosecond, Some(tz)) => {
            Box::new(TimestampTzToText::<TimestampNanosecondType>::new(tz)?)
        }
    };
    Ok(ws)
}

pub fn epoch_to_timestamp_ns(from: i64) -> Timestamp {
    let ndt = DateTime::from_timestamp_nanos(from);
    datetime_to_timestamp(ndt)
}

pub fn epoch_to_timestamp_us(from: i64) -> Timestamp {
    let ndt =
        DateTime::from_timestamp_micros(from).expect("Timestamp must be in range for microseconds");
    datetime_to_timestamp(ndt)
}

pub fn epoch_to_timestamp_ms(from: i64) -> Timestamp {
    let ndt =
        DateTime::from_timestamp_millis(from).expect("Timestamp must be in range for milliseconds");
    datetime_to_timestamp(ndt)
}

pub fn epoch_to_timestamp_s(from: i64) -> Timestamp {
    let ndt = DateTime::from_timestamp_millis(from * 1_000)
        .expect("Timestamp must be in range for milliseconds");
    datetime_to_timestamp(ndt)
}

fn datetime_to_timestamp(ndt: DateTime<chrono::Utc>) -> Timestamp {
    let date = ndt.date_naive();
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

/// Strategy for writing a timestamp with timezone as text into the database. Microsoft SQL Server
/// supports this via `SQL_SS_TIMESTAMPOFFSET`, yet this is an extension of the ODBC standard. So
/// maybe for now we are safer just to write it as a string literal.
pub struct TimestampTzToText<P> {
    time_zone: Tz,
    _phantom: PhantomData<P>,
}

impl<P> TimestampTzToText<P> {
    pub fn new(time_zone: Arc<str>) -> Result<Self, WriterError> {
        let tz = time_zone.parse().map_err(|e| {
            debug!("Failed to parse time zone '{time_zone}'. Original error: {e}");
            WriterError::InvalidTimeZone { time_zone }
        })?;
        Ok(Self {
            time_zone: tz,
            _phantom: PhantomData,
        })
    }
}

impl<P> WriteStrategy for TimestampTzToText<P>
where
    P: ArrowPrimitiveType<Native=i64> + InserableAsTimestampWithTimeZone,
{
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text {
            max_str_len: P::FORMAT_WITH_TIME_ZONE_LEN,
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnySliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array
            .as_any()
            .downcast_ref::<PrimitiveArray<P>>()
            .unwrap();
        let mut to = column_buf.as_text_view().unwrap();
        for (index, timestamp) in from.iter().enumerate() {
            if let Some(timestamp) = timestamp {
                let dt = P::to_regional_datetime(timestamp, &self.time_zone);
                write!(
                    to.set_mut(index + param_offset, P::FORMAT_WITH_TIME_ZONE_LEN),
                    "{}",
                    dt.format(P::FORMAT_STRING),
                )
                .unwrap();
            } else {
                to.set_cell(index + param_offset, None)
            }
        }
        Ok(())
    }
}

trait InserableAsTimestampWithTimeZone {
    /// Length of the string representation of a timestamp with time zone, e.g. "2023-10-01 12:34:56.789+02:00"
    const FORMAT_WITH_TIME_ZONE_LEN: usize;
    const FORMAT_STRING: &'static str;
    fn to_regional_datetime(epoch: i64, time_zone: &Tz) -> DateTime<Tz>;
}

impl InserableAsTimestampWithTimeZone for TimestampSecondType {
    const FORMAT_WITH_TIME_ZONE_LEN: usize = 25; // "YYYY-MM-DD HH:MM:SS+00:00"
    const FORMAT_STRING: &'static str = "%Y-%m-%d %H:%M:%S%Z";

    fn to_regional_datetime(epoch: i64, time_zone: &Tz) -> DateTime<Tz> {
        time_zone
            .timestamp_opt(epoch, 0)
            .earliest()
            .expect("Timestamp must be in range for the timezone")
    }
}

impl InserableAsTimestampWithTimeZone for TimestampMillisecondType {
    const FORMAT_WITH_TIME_ZONE_LEN: usize = 29; // "YYYY-MM-DD HH:MM:SS.fff+00:00"
    const FORMAT_STRING: &'static str = "%Y-%m-%d %H:%M:%S.%3f%Z";

    fn to_regional_datetime(epoch: i64, time_zone: &Tz) -> DateTime<Tz> {
        let epoch_sec = epoch / 1_000;
        let nano = (epoch % 1_000) * 1_000_000; // Convert milliseconds to nanoseconds
        time_zone
            .timestamp_opt(epoch_sec, nano as u32)
            .earliest()
            .expect("Timestamp must be in range for the timezone")
    }
}

impl InserableAsTimestampWithTimeZone for TimestampMicrosecondType {
    const FORMAT_WITH_TIME_ZONE_LEN: usize = 32; // "YYYY-MM-DD HH:MM:SS.fff+00:00"
    const FORMAT_STRING: &'static str = "%Y-%m-%d %H:%M:%S.%6f%Z";

    fn to_regional_datetime(epoch: i64, time_zone: &Tz) -> DateTime<Tz> {
        let epoch_sec = epoch / 1_000_000;
        let nano = (epoch % 1_000_000) * 1_000; // Convert milliseconds to nanoseconds
        time_zone
            .timestamp_opt(epoch_sec, nano as u32)
            .earliest()
            .expect("Timestamp must be in range for the timezone")
    }
}

impl InserableAsTimestampWithTimeZone for TimestampNanosecondType {
    const FORMAT_WITH_TIME_ZONE_LEN: usize = 35; // "YYYY-MM-DD HH:MM:SS.fff+00:00"
    const FORMAT_STRING: &'static str = "%Y-%m-%d %H:%M:%S.%9f%Z";

    fn to_regional_datetime(epoch: i64, time_zone: &Tz) -> DateTime<Tz> {
        let epoch_sec = epoch / 1_000_000_000;
        let nano = epoch % 1_000_000_000; // Convert milliseconds to nanoseconds
        time_zone
            .timestamp_opt(epoch_sec, nano as u32)
            .earliest()
            .expect("Timestamp must be in range for the timezone")
    }
}