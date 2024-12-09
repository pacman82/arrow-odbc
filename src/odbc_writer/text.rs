use arrow::array::{Array, LargeStringArray, StringArray};
use odbc_api::buffers::{AnySliceMut, BufferDesc, TextColumnSliceMut};

use super::{WriteStrategy, WriterError};

#[cfg(not(target_os = "windows"))]
pub type Utf8ToNativeText = Utf8ToNarrow;

#[cfg(target_os = "windows")]
pub type Utf8ToNativeText = Utf8ToWide;

#[cfg(not(target_os = "windows"))]
pub type LargeUtf8ToNativeText = LargeUtf8ToNarrow;

#[cfg(target_os = "windows")]
pub type LargeUtf8ToNativeText = LargeUtf8ToWide;

pub struct Utf8ToNarrow;

impl WriteStrategy for Utf8ToNarrow {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text { max_str_len: 1 }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let to = to.as_text_view().unwrap();
        insert_into_narrow_slice(from.iter(), to, param_offset)?;
        Ok(())
    }
}

pub struct LargeUtf8ToNarrow;

impl WriteStrategy for LargeUtf8ToNarrow {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::Text { max_str_len: 1 }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let to = to.as_text_view().unwrap();
        insert_into_narrow_slice(from.iter(), to, param_offset)?;
        Ok(())
    }
}

fn insert_into_narrow_slice<'a>(
    from: impl Iterator<Item = Option<&'a str>>,
    mut to: TextColumnSliceMut<u8>,
    param_offset: usize,
) -> Result<(), WriterError> {
    for (row_index, element) in from.enumerate() {
        if let Some(text) = element {
            to.ensure_max_element_length(text.len(), row_index)
                .map_err(WriterError::RebindBuffer)?;
            to.set_cell(param_offset + row_index, Some(text.as_bytes()))
        } else {
            to.set_cell(param_offset + row_index, None);
        }
    }
    Ok(())
}

pub struct Utf8ToWide;

impl WriteStrategy for Utf8ToWide {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::WText { max_str_len: 1 }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let to = to.as_w_text_view().unwrap();
        insert_into_wide_slice(from.iter(), to, param_offset)?;
        Ok(())
    }
}

pub struct LargeUtf8ToWide;

impl WriteStrategy for LargeUtf8ToWide {
    fn buffer_desc(&self) -> BufferDesc {
        BufferDesc::WText { max_str_len: 1 }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let to = to.as_w_text_view().unwrap();
        insert_into_wide_slice(from.iter(), to, param_offset)?;
        Ok(())
    }
}

fn insert_into_wide_slice<'a>(
    from: impl Iterator<Item = Option<&'a str>>,
    mut to: TextColumnSliceMut<u16>,
    at: usize,
) -> Result<(), WriterError> {
    // We must first encode the utf8 input to utf16. We reuse this buffer for that in order to avoid
    // allocations.
    let mut utf_16 = Vec::new();
    for (row_index, element) in from.enumerate() {
        if let Some(text) = element {
            utf_16.extend(text.encode_utf16());
            to.ensure_max_element_length(utf_16.len(), at + row_index)
                .map_err(WriterError::RebindBuffer)?;
            to.set_cell(at + row_index, Some(&utf_16));
            utf_16.clear();
        } else {
            to.set_cell(at + row_index, None);
        }
    }
    Ok(())
}
