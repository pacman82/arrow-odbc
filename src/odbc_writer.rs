use std::cmp::min;

use thiserror::Error;

use arrow::{
    array::Array,
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use odbc_api::{
    buffers::{AnyColumnBuffer, AnyColumnSliceMut, BufferDescription},
    handles::StatementImpl,
    ColumnarBulkInserter, Prepared,
};

use self::utf8_to_narrow::Utf8ToNarrow;

mod utf8_to_narrow;

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("Failure to bind the array parameter buffers to the statement.")]
    BindParameterBuffers(#[source] odbc_api::Error),
    #[error("Failure to execute the sql statement, sending the data to the database.")]
    ExecuteStatment(#[source] odbc_api::Error),
    #[error("An error occured rebinding a parameter buffer to the sql statement.")]
    RebindBuffer(#[source] odbc_api::Error),
    #[error("The arrow data type {0} is not supported for insertion.")]
    UnsupportedArrowDataType(DataType),
}

/// Inserts batches from an [`crate::arrow::RecordBatchReader`] into a database.
pub struct OdbcWriter<'o> {
    /// Prepared statement with bound array parameter buffers. Data is copied into these buffers
    /// until they are full. Then we execute the statement. This is repeated until we run out of
    /// data.
    pub inserter: ColumnarBulkInserter<StatementImpl<'o>, AnyColumnBuffer>,
    /// For each field in the arrow schema we decide on which buffer to use to send the parameters
    /// to the database, and need to remember how to copy the data from an arrow array to an odbc
    /// mutable buffer slice for any column.
    strategies: Vec<Box<dyn WriteStrategy>>,
}

impl<'o> OdbcWriter<'o> {
    /// Construct a new ODBC writer using an alredy existing prepared statement.
    pub fn new(
        row_capacity: usize,
        schema: SchemaRef,
        statment: Prepared<'o>,
    ) -> Result<Self, WriterError> {
        let strategies: Vec<_> = schema
            .fields()
            .iter()
            .map(field_to_write_strategy)
            .collect::<Result<_, _>>()?;
        let descriptions = strategies.iter().map(|cws| cws.buffer_description());
        let inserter = statment
            .into_any_column_inserter(row_capacity, descriptions)
            .map_err(WriterError::BindParameterBuffers)?;

        Ok(Self {
            inserter,
            strategies,
        })
    }

    pub fn write_batch(&mut self, record_batch: &RecordBatch) -> Result<(), WriterError> {
        let capacity = self.inserter.capacity();
        let mut remanining_rows = record_batch.num_rows();
        // The record batch may contain more rows than the capacity of our writer can hold. So we
        // need to be able to fill the buffers multiple times and send them to the database in
        // between.
        while remanining_rows != 0 {
            let chunk_size = min(capacity - self.inserter.num_rows(), remanining_rows);
            let param_offset = self.inserter.num_rows();
            self.inserter.set_num_rows(param_offset + chunk_size);
            let chunk = record_batch.slice(record_batch.num_rows() - remanining_rows, chunk_size);
            for (index, (array, strategy)) in chunk
                .columns()
                .iter()
                .zip(self.strategies.iter())
                .enumerate()
            {
                strategy.write_rows(param_offset, self.inserter.column_mut(index), array)?
            }

            // If we used up all capacity we send the parameters to the database and reset the
            // parameter buffers.
            if self.inserter.num_rows() == capacity {
                self.flush()?;
            }
            remanining_rows -= chunk_size;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), WriterError> {
        self.inserter
            .execute()
            .map_err(WriterError::ExecuteStatment)?;
        self.inserter.clear();
        Ok(())
    }
}

trait WriteStrategy {
    fn buffer_description(&self) -> BufferDescription;

    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError>;
}

fn field_to_write_strategy(field: &Field) -> Result<Box<dyn WriteStrategy>, WriterError> {
    let strategy = match field.data_type() {
        DataType::Utf8 => Box::new(Utf8ToNarrow),
        unsupported => return Err(WriterError::UnsupportedArrowDataType(unsupported.clone())),
        // arrow::datatypes::DataType::Null => todo!(),
        // arrow::datatypes::DataType::Boolean => todo!(),
        // arrow::datatypes::DataType::Int8 => todo!(),
        // arrow::datatypes::DataType::Int16 => todo!(),
        // arrow::datatypes::DataType::Int32 => todo!(),
        // arrow::datatypes::DataType::Int64 => todo!(),
        // arrow::datatypes::DataType::UInt8 => todo!(),
        // arrow::datatypes::DataType::UInt16 => todo!(),
        // arrow::datatypes::DataType::UInt32 => todo!(),
        // arrow::datatypes::DataType::UInt64 => todo!(),
        // arrow::datatypes::DataType::Float16 => todo!(),
        // arrow::datatypes::DataType::Float32 => todo!(),
        // arrow::datatypes::DataType::Float64 => todo!(),
        // arrow::datatypes::DataType::Timestamp(_, _) => todo!(),
        // arrow::datatypes::DataType::Date32 => todo!(),
        // arrow::datatypes::DataType::Date64 => todo!(),
        // arrow::datatypes::DataType::Time32(_) => todo!(),
        // arrow::datatypes::DataType::Time64(_) => todo!(),
        // arrow::datatypes::DataType::Duration(_) => todo!(),
        // arrow::datatypes::DataType::Interval(_) => todo!(),
        // arrow::datatypes::DataType::Binary => todo!(),
        // arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
        // arrow::datatypes::DataType::LargeBinary => todo!(),
        // arrow::datatypes::DataType::LargeUtf8 => todo!(),
        // arrow::datatypes::DataType::List(_) => todo!(),
        // arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
        // arrow::datatypes::DataType::LargeList(_) => todo!(),
        // arrow::datatypes::DataType::Struct(_) => todo!(),
        // arrow::datatypes::DataType::Union(_, _, _) => todo!(),
        // arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
        // arrow::datatypes::DataType::Decimal(_, _) => todo!(),
        // arrow::datatypes::DataType::Map(_, _) => todo!(),
    };
    Ok(strategy)
}
