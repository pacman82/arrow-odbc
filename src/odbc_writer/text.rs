use arrow::array::{Array, GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray};
use odbc_api::{
    BindParamDesc,
    buffers::{BoxColumBufferRefMut, TextColumnSliceMut},
};

use super::{WriteStrategy, WriterError};

#[cfg(not(target_os = "windows"))]
pub type Utf8ToNativeText = Utf8ToNarrow;

#[cfg(target_os = "windows")]
pub type Utf8ToNativeText = Utf8ToWide;

#[cfg(not(target_os = "windows"))]
pub type LargeUtf8ToNativeText = LargeUtf8ToNarrow;

#[cfg(target_os = "windows")]
pub type LargeUtf8ToNativeText = LargeUtf8ToWide;

#[cfg_attr(target_os = "windows", allow(dead_code))]
pub struct Utf8ToNarrow;

impl WriteStrategy for Utf8ToNarrow {
    fn buffer_desc(&self) -> BindParamDesc {
        // This is very mindful of memory, but maybe we can look ahead and figure out how large the
        // strings we insert are in order to avoid reallocations and rebinding.
        BindParamDesc::text(1)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let to = to.as_text().unwrap();
        insert_into_narrow_slice(from, to, param_offset)?;
        Ok(())
    }
}

#[cfg_attr(target_os = "windows", allow(dead_code))]
pub struct LargeUtf8ToNarrow;

impl WriteStrategy for LargeUtf8ToNarrow {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::text(1)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let to = to.as_text().unwrap();
        insert_into_narrow_slice(from, to, param_offset)?;
        Ok(())
    }
}

#[cfg_attr(target_os = "windows", allow(dead_code))]
fn insert_into_narrow_slice<'a, O>(
    from: &GenericStringArray<O>,
    mut to: TextColumnSliceMut<u8>,
    param_offset: usize,
) -> Result<(), WriterError>
where
    O: OffsetSizeTrait,
{
    let max_element_len = max_element_byte_len(from);
    // Ensure the buffer is large enough to hold all elements of the current batch. In case of
    // reallocation, we need to copy all the values from the previous batches. `param_offset`
    // tells us how many rows are already written in the buffer from previous batches.
    to.ensure_max_element_length(max_element_len, param_offset)
        .map_err(WriterError::RebindBuffer)?;
    for (row_index, element) in from.iter().enumerate() {
        // Total number of rows written into the inserter (`to`). This includes the values from the
        // current batch (`row_index`), as well as the ones from the previous batches
        // (`param_offset`). In case of reallocation, we need to copy all these values. Also, this
        // is the index of the element we currently want to write.
        let num_rows_written_so_far = param_offset + row_index;
        if let Some(text) = element {
            to.set_cell(num_rows_written_so_far, Some(text.as_bytes()))
        } else {
            to.set_cell(num_rows_written_so_far, None);
        }
    }
    Ok(())
}

#[cfg_attr(target_os = "linux", allow(dead_code))]
pub struct Utf8ToWide;

impl WriteStrategy for Utf8ToWide {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::wide_text(1)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let to = to.as_wide_text().unwrap();
        insert_into_wide_slice(from, to, param_offset)?;
        Ok(())
    }
}

#[cfg_attr(target_os = "linux", allow(dead_code))]
pub struct LargeUtf8ToWide;

impl WriteStrategy for LargeUtf8ToWide {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::wide_text(1)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let to = to.as_wide_text().unwrap();
        insert_into_wide_slice(from, to, param_offset)?;
        Ok(())
    }
}

#[cfg_attr(target_os = "linux", allow(dead_code))]
fn insert_into_wide_slice<'a, O>(
    from: &GenericStringArray<O>,
    mut to: TextColumnSliceMut<u16>,
    at: usize,
) -> Result<(), WriterError>
where
    O: OffsetSizeTrait,
{
    // We must first encode the utf8 input to utf16. We reuse this buffer for that in order to avoid
    // allocations.
    let mut utf_16 = Vec::new();
    let max_utf_8_byte_len = max_element_byte_len(from);
    // We use the fact that the size in bytes of a utf-8 encoded character is >= the code points of
    // the same character in utf-16. We use this estimated upper bound to avoid reallocations, mid
    // batch. `at` is the number of rows which have been written into the buffer by previous
    // batches, but have not yet been transmitted. This value is needed to ensure they are copied
    // into the newly allocated buffer.
    to.ensure_max_element_length(max_utf_8_byte_len, at)
        .map_err(WriterError::RebindBuffer)?;
    for (row_index, element) in from.iter().enumerate() {
        // Total number of rows written into the inserter (`to`). This includes the values from the
        // current batch (`row_index`), as well as the ones from the previous batches (`at`). In
        // case of reallocation, we need to copy all these values. Also, this is the index of the
        // element we currently want to write.
        let num_rows_written_so_far = at + row_index;
        if let Some(text) = element {
            utf_16.extend(text.encode_utf16());
            to.set_cell(num_rows_written_so_far, Some(&utf_16));
            utf_16.clear();
        } else {
            to.set_cell(num_rows_written_so_far, None);
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn max_element_byte_len<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> usize {
    array
        .value_offsets()
        .windows(2)
        .map(|w| (w[1] - w[0]).as_usize())
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::max_element_byte_len;
    use arrow::array::StringArray;

    #[test]
    fn max_element_byte_len_returns_longest_element_size() {
        let array = StringArray::from(vec!["a".repeat(10), "a".repeat(15), "a".repeat(12)]);
        assert_eq!(15, max_element_byte_len(&array));
    }
}
