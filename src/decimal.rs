use arrow::{
    array::{Array, Decimal128Array, Decimal256Array},
    datatypes::{ArrowPrimitiveType, Decimal256Type},
};
use odbc_api::{BindParamDesc, buffers::BoxColumBufferRefMut};

use crate::{WriterError, odbc_writer::WriteStrategy};

pub struct NullableDecimal128AsText {
    precision: u8,
    scale: i8,
}

impl NullableDecimal128AsText {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self { precision, scale }
    }
}

pub struct NullableDecimal256AsText {
    precision: u8,
    scale: i8,
}

impl NullableDecimal256AsText {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self { precision, scale }
    }
}

/// Length of a text representation of a decimal
fn len_text(scale: i8, precision: u8) -> usize {
    match scale {
        // Precision digits + (- scale zeroes) + sign
        i8::MIN..=-1 => (precision as i32 - scale as i32 + 1).try_into().unwrap(),
        // Precision digits + sign
        0 => precision as usize + 1,
        // Precision digits + radix character (`.`) + sign
        1.. => precision as usize + 1 + 1,
    }
}

impl WriteStrategy for NullableDecimal128AsText {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::decimal_as_text(self.precision, self.scale)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let length = len_text(self.scale, self.precision);

        let from = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        let mut to = column_buf.as_text().unwrap();

        for (index, cell) in from.iter().enumerate() {
            if let Some(value) = cell {
                let buf = to.set_mut(index + param_offset, length);
                write_i128_as_decimal(value, self.precision, self.scale, buf)
            } else {
                to.set_cell(index + param_offset, None)
            }
        }
        Ok(())
    }
}

impl WriteStrategy for NullableDecimal256AsText {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::decimal_as_text(self.precision, self.scale)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: BoxColumBufferRefMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
        let mut to = column_buf.as_text().unwrap();
        let length = len_text(self.scale, self.precision);

        for (index, cell) in from.iter().enumerate() {
            if let Some(value) = cell {
                let buf = to.set_mut(index + param_offset, length);
                write_i256_as_decimal(value, self.precision, self.scale, buf)
            } else {
                to.set_cell(index + param_offset, None)
            }
        }
        Ok(())
    }
}

fn write_i128_as_decimal(n: i128, precision: u8, scale: i8, text: &mut [u8]) {
    text[0] = if n.is_negative() { b'-' } else { b'+' };
    let mut n = n.unsigned_abs();

    // Number of digits + one decimal separator (`.`)
    let str_len: i32 = (len_text(scale, precision) - 1).try_into().unwrap();

    for index in (0..str_len).rev() {
        // In case of negative scale, fill the last digits with zeroes
        let char = if (str_len - index) <= -scale as i32 {
            b'0'
        // The separator will not be printed in case of scale <= 0 since index is never going to
        // reach `precision`.
        } else if index == precision as i32 - scale as i32 {
            b'.'
        // u128 division compiles to a slow library call. Once the remaining value fits into a
        // u64, divide that instead.
        } else if let Ok(small) = u64::try_from(n) {
            n = (small / 10).into();
            let digit = (small % 10) as u8;
            b'0' + digit
        } else {
            let digit = (n % 10) as u8;
            n /= 10;
            b'0' + digit
        };
        // +1 offset to make space for sign character
        text[index as usize + 1] = char;
    }
}

type I256 = <Decimal256Type as ArrowPrimitiveType>::Native;

fn write_i256_as_decimal(mut n: I256, precision: u8, scale: i8, text: &mut [u8]) {
    if n.lt(&I256::ZERO) {
        n = n.checked_mul(I256::MINUS_ONE).unwrap();
        text[0] = b'-';
    } else {
        text[0] = b'+';
    }

    // Number of digits + one decimal separator (`.`)
    let str_len: i32 = (len_text(scale, precision) - 1).try_into().unwrap();

    let ten = I256::from_i128(10);
    for index in (0..str_len).rev() {
        let char = if (str_len - index) <= -scale as i32 {
            b'0'
        // The separator will not be printed in case of scale == 0 since index is never going to
        // reach `precision`.
        } else if index == precision as i32 - scale as i32 {
            b'.'
        // i256 division is slow. Once the remaining value fits into a u64, divide that instead.
        } else if let Some(small) = n.to_i128().and_then(|n| u64::try_from(n).ok()) {
            n = I256::from_i128((small / 10).into());
            let digit = (small % 10) as u8;
            b'0' + digit
        } else {
            let digit = n.checked_rem(ten).unwrap().to_i128().unwrap() as u8;
            n = n.checked_div(ten).unwrap();
            b'0' + digit
        };
        // +1 offset to make space for sign character
        text[index as usize + 1] = char;
    }
}

#[cfg(test)]
mod tests {
    use super::{I256, len_text, write_i128_as_decimal, write_i256_as_decimal};

    #[test]
    fn decimal128_above_u64_max() {
        let mut text = vec![0; len_text(0, 20)];
        write_i128_as_decimal(18_446_744_073_709_551_616, 20, 0, &mut text);
        assert_eq!(text, "+18446744073709551616".as_bytes());
    }

    #[test]
    fn decimal128_negative_value_beyond_u64() {
        let mut text = vec![0; len_text(2, 20)];
        write_i128_as_decimal(-18_446_744_073_709_551_616, 20, 2, &mut text);
        assert_eq!(text, "-184467440737095516.16".as_bytes());
    }

    #[test]
    fn decimal128_i128_min() {
        let mut text = vec![0; len_text(0, 39)];
        write_i128_as_decimal(i128::MIN, 39, 0, &mut text);
        assert_eq!(text, "-170141183460469231731687303715884105728".as_bytes());
    }

    #[test]
    fn decimal256_above_i128_max() {
        let value = I256::from_i128(i128::MAX).checked_add(I256::ONE).unwrap();
        let mut text = vec![0; len_text(0, 40)];
        write_i256_as_decimal(value, 40, 0, &mut text);
        assert_eq!(text, "+0170141183460469231731687303715884105728".as_bytes());
    }

    #[test]
    fn decimal256_full_50_digit_precision() {
        let value = I256::from_i128(12_345_678_901_234_567_890_123_456_789_012_345_678)
            .checked_mul(I256::from_i128(1_000_000_000_000))
            .unwrap()
            .checked_add(I256::from_i128(901_234_567_890))
            .unwrap();
        let mut text = vec![0; len_text(10, 50)];
        write_i256_as_decimal(value, 50, 10, &mut text);
        assert_eq!(
            text,
            "+1234567890123456789012345678901234567890.1234567890".as_bytes()
        );
    }
}

#[cfg(feature = "internal_benches")]
mod bench {
    use odbc_api::buffers::TextColumn;
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    use super::{I256, len_text, write_i128_as_decimal, write_i256_as_decimal};

    const ROWS: usize = 10_000;

    #[divan::bench(args = [(18, 2), (38, 0)])]
    fn decimal128(bencher: divan::Bencher, (precision, scale): (u8, i8)) {
        let values: Vec<_> = random_values(precision)
            .into_iter()
            .map(|value| value.map(|value| value.to_i128().unwrap()))
            .collect();
        let length = len_text(scale, precision);
        let mut column = TextColumn::<u8>::new(ROWS, length);
        bencher.bench_local(|| {
            for (index, value) in values.iter().enumerate() {
                if let Some(value) = value {
                    write_i128_as_decimal(*value, precision, scale, column.set_mut(index, length));
                } else {
                    column.set_value(index, None);
                }
            }
            divan::black_box(&mut column);
        });
    }

    #[divan::bench(args = [(18, 2), (50, 10)])]
    fn decimal256(bencher: divan::Bencher, (precision, scale): (u8, i8)) {
        let values = random_values(precision);
        let length = len_text(scale, precision);
        let mut column = TextColumn::<u8>::new(ROWS, length);
        bencher.bench_local(|| {
            for (index, value) in values.iter().enumerate() {
                if let Some(value) = value {
                    write_i256_as_decimal(*value, precision, scale, column.set_mut(index, length));
                } else {
                    column.set_value(index, None);
                }
            }
            divan::black_box(&mut column);
        });
    }

    fn random_values(precision: u8) -> Vec<Option<I256>> {
        let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_F00D_CAFE);
        (0..ROWS)
            .map(|_| {
                let mut value = I256::ZERO;
                for _ in 0..rng.random_range(1..=precision) {
                    value = value
                        .checked_mul(I256::from_i128(10))
                        .unwrap()
                        .checked_add(I256::from_i128(rng.random_range(0..10)))
                        .unwrap();
                }
                if rng.random_bool(0.5) {
                    value = value.checked_mul(I256::MINUS_ONE).unwrap();
                }
                (!rng.random_bool(0.01)).then_some(value)
            })
            .collect()
    }
}
