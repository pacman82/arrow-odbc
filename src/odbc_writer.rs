use std::cmp::min;

use thiserror::Error;

use arrow::{
    array::Array,
    datatypes::{
        DataType, Field, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, TimeUnit, TimestampSecondType, UInt8Type, Schema, TimestampMillisecondType, TimestampMicrosecondType
    },
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use odbc_api::{
    buffers::{AnyColumnBuffer, AnyColumnSliceMut, BufferDescription},
    handles::StatementImpl,
    ColumnarBulkInserter, Connection, Prepared,
};

use crate::date_time::{epoch_sec_to_timestamp, epoch_ms_to_timestamp, epoch_us_to_timestamp};

use self::{boolean::boolean_to_bit, map_arrow_to_odbc::MapArrowToOdbc, text::Utf8ToNativeText};

mod boolean;
mod map_arrow_to_odbc;
mod text;

/// Consumes the batches in the reader and inserts it into a table on a database.
pub fn insert_into_table(
    connection: &Connection,
    batches: &mut impl RecordBatchReader,
    table_name: &str,
    batch_size: usize,
) -> Result<(), WriterError> {
    let schema = batches.schema();
    let mut inserter = OdbcWriter::with_connection(connection, schema.as_ref(), table_name, batch_size)?;
    inserter.write_all(batches)
}

fn insert_statement_text(table: &str, column_names: &[&'_ str]) -> String {
    // Generate statement text from table name and headline
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({}) VALUES ({});", table, columns, values)
}

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
    #[error("An error occurred preparing SQL statement: {0}", sql)]
    PreparingInsertStatement {
        #[source]
        source: odbc_api::Error,
        sql: String,
    },
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
        schema: &Schema,
        statement: Prepared<'o>,
    ) -> Result<Self, WriterError> {
        let strategies: Vec<_> = schema
            .fields()
            .iter()
            .map(field_to_write_strategy)
            .collect::<Result<_, _>>()?;
        let descriptions = strategies.iter().map(|cws| cws.buffer_description());
        let inserter = statement
            .into_any_column_inserter(row_capacity, descriptions)
            .map_err(WriterError::BindParameterBuffers)?;

        Ok(Self {
            inserter,
            strategies,
        })
    }

    pub fn with_connection(connection: &'o Connection<'o>, schema: &Schema, table_name: &str, row_capacity: usize) -> Result<Self, WriterError> {
        let fields = schema.fields();
        let num_columns = fields.len();
        let column_names: Vec<_> = (0..num_columns)
            .map(|i| fields[i].name().as_str())
            .collect();
        let sql = insert_statement_text(table_name, &column_names);
        let statement = connection
            .prepare(&sql)
            .map_err(|source| WriterError::PreparingInsertStatement { source, sql })?;
        Self::new(row_capacity, schema, statement)
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
    let is_nullable = field.is_nullable();
    let strategy = match field.data_type() {
        DataType::LargeUtf8 | DataType::Utf8 => Box::new(Utf8ToNativeText {}),
        DataType::Boolean => boolean_to_bit(is_nullable),
        DataType::Int8 => Int8Type::identical(is_nullable),
        DataType::Int16 => Int16Type::identical(is_nullable),
        DataType::Int32 => Int32Type::identical(is_nullable),
        DataType::Int64 => Int64Type::identical(is_nullable),
        DataType::UInt8 => UInt8Type::identical(is_nullable),
        DataType::Float16 => Float16Type::map_with(is_nullable, |half| half.to_f32()),
        DataType::Float32 => Float32Type::identical(is_nullable),
        DataType::Float64 => Float64Type::identical(is_nullable),
        DataType::Timestamp(TimeUnit::Second, None) => TimestampSecondType::map_with(is_nullable, epoch_sec_to_timestamp),
        DataType::Timestamp(TimeUnit::Millisecond, None) => TimestampMillisecondType::map_with(is_nullable, epoch_ms_to_timestamp),
        DataType::Timestamp(TimeUnit::Microsecond, None) => TimestampMicrosecondType::map_with(is_nullable, epoch_us_to_timestamp),
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
