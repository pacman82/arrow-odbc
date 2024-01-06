use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use odbc_api::buffers::ColumnarAnyBuffer;

use super::{to_record_batch::ToRecordBatch, MappingError};

pub struct ConcurrentConverter {
    converter: ToRecordBatch,
}

impl ConcurrentConverter {
    pub fn new(converter: ToRecordBatch) -> Self {
        Self { converter }
    }

    pub fn schema(&self) -> SchemaRef {
        self.converter.schema().clone()
    }

    pub fn buffer_to_record_batch(
        &self,
        odbc_buffer: &ColumnarAnyBuffer,
    ) -> Result<RecordBatch, MappingError> {
        self.converter.buffer_to_record_batch(odbc_buffer)
    }
}
