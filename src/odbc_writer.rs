use std::cmp::min;

use thiserror::Error;

use arrow::{
    array::Array,
    datatypes::{DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef, UInt8Type, Float32Type, Float64Type},
    error::ArrowError,
    record_batch::RecordBatch,
};
use odbc_api::{
    buffers::{AnyColumnBuffer, AnyColumnSliceMut, BufferDescription},
    handles::StatementImpl,
    ColumnarBulkInserter, Prepared,
};

use self::{boolean::boolean_to_bit, identical::identical, text::Utf8ToNativeText};

mod boolean;
mod identical;
mod text;

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
    #[error("An error occured extracting a record batch from an error reader.")]
    ReadingRecordBatch(#[source] ArrowError),
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

    pub fn write_all(
        &mut self,
        reader: impl Iterator<Item = Result<RecordBatch, ArrowError>>,
    ) -> Result<(), WriterError> {
        for result in reader {
            let record_batch = result.map_err(WriterError::ReadingRecordBatch)?;
            self.write_batch(&record_batch)?;
        }
        self.flush()?;
        Ok(())
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

pub trait WriteStrategy {
    fn buffer_description(&self) -> BufferDescription;

    /// # Parameters
    ///
    /// * `param_offset`: Start writing parameters at that position. Number of rows in the parameter
    ///   buffer before inserting the current chunk.
    /// * `column_buf`: Buffer to write the data into
    /// * `array`: Buffer to read the data from
    fn write_rows(
        &self,
        param_offset: usize,
        column_buf: AnyColumnSliceMut<'_>,
        array: &dyn Array,
    ) -> Result<(), WriterError>;
}

fn field_to_write_strategy(field: &Field) -> Result<Box<dyn WriteStrategy>, WriterError> {
    let strategy = match field.data_type() {
        DataType::LargeUtf8 | DataType::Utf8 => Box::new(Utf8ToNativeText {}),
        DataType::Boolean => boolean_to_bit(field.is_nullable()),
        DataType::Int8 => identical::<Int8Type>(field.is_nullable()),
        DataType::Int16 => identical::<Int16Type>(field.is_nullable()),
        DataType::Int32 => identical::<Int32Type>(field.is_nullable()),
        DataType::Int64 => identical::<Int64Type>(field.is_nullable()),
        DataType::UInt8 => identical::<UInt8Type>(field.is_nullable()),
        DataType::Float16 => todo!(),
        DataType::Float32 => identical::<Float32Type>(field.is_nullable()),
        DataType::Float64 => identical::<Float64Type>(field.is_nullable()),
        DataType::Timestamp(_, _) => todo!(),
        DataType::Date32 => todo!(),
        DataType::Date64 => todo!(),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Decimal(_, _) => todo!(),
        unsupported @ (DataType::Null
        // We could support u64 with upstream changes, but best if user supplies the sql data type.
        | DataType::UInt64
        // We could support u32 with upstream changes, but best if user supplies the sql data type.
        | DataType::UInt32
        // We could support u16 with upstream changes, but best if user supplies the sql data type.
        | DataType::UInt16
        | DataType::Interval(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Union(_, _, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)) => {
            return Err(WriterError::UnsupportedArrowDataType(unsupported.clone()))
        }
    };
    Ok(strategy)
}
