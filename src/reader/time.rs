use std::sync::Arc;

use arrow::array::{ArrayRef, Time32MillisecondBuilder};
use atoi::FromRadix10;
use odbc_api::{
    buffers::{AnySlice, BufferDesc},
    sys::Time,
};

use super::{MappingError, ReadStrategy};

pub fn seconds_since_midnight(time: &Time) -> i32 {
    (time.hour as i32 * 60 + time.minute as i32) * 60 + time.second as i32
}

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
                let num = time_text_i32(text);
                builder.append_value(num);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

fn time_text_i32(text: &[u8]) -> i32 {
    // HH:MM:SS.ffffff
    // 012345678901234
    let (hours, hours_digits) = i32::from_radix_10(&text[0..2]);
    debug_assert_eq!(2, hours_digits);
    debug_assert_eq!(b':', text[2]);
    let (min, min_digits) = i32::from_radix_10(&text[3..5]);
    debug_assert_eq!(2, min_digits);
    debug_assert_eq!(b':', text[5]);
    let (sec, sec_digits) = i32::from_radix_10(&text[6..8]);
    debug_assert_eq!(2, sec_digits);
    // check for fractional part
    let (frac, frac_digits) = if text.len() > 9 {
        i32::from_radix_10(&text[9..])
    } else {
        (0, 0)
    };
    let ms = match frac_digits {
        0 => 0,
        1 => frac * 100,
        2 => frac * 10,
        3 => frac,
        _ => unreachable!("We have only reserved space for 3 fractional digits in the text"),
    };
    ((hours * 60 + min) * 60 + sec) * 1000 + ms
}
