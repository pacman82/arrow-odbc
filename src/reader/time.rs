use odbc_api::sys::Time;

pub fn seconds_since_midnight(time: &Time) -> i32 {
    (time.hour as i32 * 60 + time.minute as i32) * 60 + time.second as i32
}
