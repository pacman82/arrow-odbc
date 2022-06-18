use arrow::array::{Array, BinaryArray};
use odbc_api::buffers::{AnyColumnSliceMut, BufferDescription, BufferKind};

use crate::WriterError;

use super::WriteStrategy;

pub struct VariadicBinary;

impl WriteStrategy for VariadicBinary {
    fn buffer_description(&self) -> BufferDescription {
        BufferDescription {
            nullable: true,
            kind: BufferKind::Binary { length: 1 },
        }
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: AnyColumnSliceMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut to = to.as_bin_view().unwrap();
        for (row_index, element) in from.iter().enumerate() {
            if let Some(bytes) = element {
                to.ensure_max_element_length(bytes.len(), row_index)
                    .map_err(WriterError::RebindBuffer)?;
                to.set_cell(param_offset + row_index, Some(bytes))
            } else {
                to.set_cell(param_offset + row_index, None);
            }
        }
        Ok(())
    }
}
