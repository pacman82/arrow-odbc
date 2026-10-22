use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray};
use odbc_api::{BindParamDesc, buffers::BoxColumBufferRefMut};

use super::{WriteStrategy, WriterError};

pub struct FixedSizeBinary {
    length: usize,
}

impl FixedSizeBinary {
    pub fn new(length: usize) -> Self {
        Self { length }
    }
}

impl WriteStrategy for FixedSizeBinary {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::binary(self.length)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut to = to.as_binary().unwrap();
        for (row_index, element) in from.iter().enumerate() {
            to.set_cell(param_offset + row_index, element);
        }
        Ok(())
    }
}

pub struct VariadicBinary {
    buffer_start_size: usize,
}

impl VariadicBinary {
    pub fn new(buffer_start_size: usize) -> Self {
        VariadicBinary { buffer_start_size }
    }
}

impl WriteStrategy for VariadicBinary {
    fn buffer_desc(&self) -> BindParamDesc {
        BindParamDesc::binary(self.buffer_start_size)
    }

    fn write_rows(
        &self,
        param_offset: usize,
        to: BoxColumBufferRefMut<'_>,
        from: &dyn Array,
    ) -> Result<(), WriterError> {
        let from = from.as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut to = to.as_binary().unwrap();
        if from.null_count() != from.len() {
            let max_element_len = from.iter().flatten().map(<[u8]>::len).max().unwrap_or(0);
            to.ensure_max_element_length(max_element_len, param_offset)
                .map_err(WriterError::RebindBuffer)?;
        }
        for (row_index, element) in from.iter().enumerate() {
            if let Some(bytes) = element {
                to.set_cell(param_offset + row_index, Some(bytes))
            } else {
                to.set_cell(param_offset + row_index, None);
            }
        }
        Ok(())
    }
}
