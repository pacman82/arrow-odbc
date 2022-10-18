use arrow::array::{Array, StringArray};
use odbc_api::buffers::{BufferDescription, BufferKind, AnySliceMut};

use super::{WriteStrategy, WriterError};

#[cfg(not(target_os = "windows"))]
pub type Utf8ToNativeText = Utf8ToNarrow;

#[cfg(target_os = "windows")]
pub type Utf8ToNativeText = Utf8ToWide;

pub struct Utf8ToNarrow;

impl WriteStrategy for Utf8ToNarrow {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Text { max_str_len: 1 },
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let mut to = to.as_text_view().unwrap();
        for (row_index, element) in from.iter().enumerate() {
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
}

pub struct Utf8ToWide;

impl WriteStrategy for Utf8ToWide {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::WText { max_str_len: 1 },
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnySliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let mut to = to.as_w_text_view().unwrap();
        // We must first encode the utf8 input to utf16. We reuse this buffer for that in order to
        // avoid allocations.
        let mut utf_16 = Vec::new();
        for (row_index, element) in from.iter().enumerate() {
            if let Some(text) = element {
                utf_16.extend(text.encode_utf16());
                to.ensure_max_element_length(utf_16.len(), row_index)
                    .map_err(WriterError::RebindBuffer)?;
                to.set_cell(param_offset + row_index, Some(&utf_16));
                utf_16.clear();
            } else {
                to.set_cell(param_offset + row_index, None);
            }
        }
        Ok(())
    }
}
