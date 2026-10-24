use arrow::{
    datatypes::{Date32Type, Date64Type}, temporal_conversions::MILLISECONDS_IN_DAY,
};

use crate::date_time::epoch_to_date;

use super::{MapArrowToOdbc as _, WriteStrategy};

pub fn date_64(is_nullable: bool) -> Box<dyn WriteStrategy> {
    Date64Type::map_with(is_nullable, |ms_since_epoch| {
        let days_since_epoch = ms_since_epoch.div_euclid(MILLISECONDS_IN_DAY);
        let days_since_epoch: i32 = days_since_epoch.try_into().unwrap();
        epoch_to_date(days_since_epoch)
    })
}

pub fn date_32(is_nullable: bool) -> Box<dyn WriteStrategy> {
    Date32Type::map_with(is_nullable, epoch_to_date)
}
