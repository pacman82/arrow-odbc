use arrow::array::{Array, StringArray};
use odbc_api::{
    buffers::{AnyColumnSliceMut, BufferDescription, BufferKind},
    U16String,
};

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
        to: AnyColumnSliceMut<'_>,
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
        to: AnyColumnSliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<StringArray>().unwrap();
        let mut to = to.as_w_text_view().unwrap();
        for (row_index, element) in from.iter().enumerate() {
            if let Some(text) = element {
                to.ensure_max_element_length(text.len(), row_index)
                    .map_err(WriterError::RebindBuffer)?;
                // This allocation of the U16String might not be necessary if we could directly
                // write the UTF-16 encoded characters to the target buffer during encoding.
                let text = U16String::from_str(text);
                to.set_cell(param_offset + row_index, Some(text.as_slice()))
            } else {
                to.set_cell(param_offset + row_index, None);
            }
        }
        Ok(())
    }
}
