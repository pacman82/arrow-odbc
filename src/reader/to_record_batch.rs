use std::sync::Arc;

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use log::info;
use odbc_api::{buffers::ColumnarAnyBuffer, AsyncResultSetMetadata, ResultSetMetadata, Sleep};

use crate::{
    arrow_schema_from, schema::arrow_schema_from_async, BufferAllocationOptions, ColumnFailure,
    Error,
};

use super::{choose_column_strategy, choose_column_strategy_async, MappingError, ReadStrategy};

/// Transforms batches fetched from an ODBC data source in a
/// [`odbc_api::bufferers::ColumnarAnyBuffer`] into arrow tables of the specified schemas. It also
/// allocates the buffers to hold the ODBC batches with the matching buffer descriptions.
pub struct ToRecordBatch {
    /// Must contain one item for each field in [`Self::schema`]. Encapsulates all the column type
    /// specific decisions which go into filling an Arrow array from an ODBC data source.
    column_strategies: Vec<Box<dyn ReadStrategy + Send>>,
    /// Arrow schema describing the arrays we want to fill from the Odbc data source.
    schema: SchemaRef,
}

impl ToRecordBatch {
    pub fn new(
        cursor: &mut impl ResultSetMetadata,
        schema: Option<SchemaRef>,
        buffer_allocation_options: BufferAllocationOptions,
    ) -> Result<Self, Error> {
        // Infer schema if not given by the user
        let schema = if let Some(schema) = schema {
            schema
        } else {
            Arc::new(arrow_schema_from(cursor)?)
        };

        let column_strategies: Vec<Box<dyn ReadStrategy + Send>> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let col_index = (index + 1).try_into().unwrap();
                choose_column_strategy(field, cursor, col_index, buffer_allocation_options)
                    .map_err(|cause| cause.into_crate_error(field.name().clone(), index))
            })
            .collect::<Result<_, _>>()?;

        Ok(ToRecordBatch {
            column_strategies,
            schema,
        })
    }

    /// Async version of `new`
    pub async fn new_from_async<S: Sleep>(
        cursor: &mut impl AsyncResultSetMetadata,
        schema: Option<SchemaRef>,
        buffer_allocation_options: BufferAllocationOptions,
        sleep: impl Fn() -> S,
    ) -> Result<Self, Error> {
        // Infer schema if not given by the user
        let schema = if let Some(schema) = schema {
            schema
        } else {
            Arc::new(arrow_schema_from_async(cursor, &sleep).await?)
        };

        let mut column_strategies = Vec::with_capacity(schema.fields().len());

        // note that we don't try and call these in parallel or out of order since once we start
        // calling one of the ODBC functions asynchronously, we can't call another one until the first returns
        for (index, field) in schema.fields().iter().enumerate() {
            let col_index = (index + 1).try_into().unwrap();
            let strategy = choose_column_strategy_async(
                field,
                cursor,
                col_index,
                buffer_allocation_options,
                &sleep,
            )
            .await
            .map_err(|cause| cause.into_crate_error(field.name().clone(), index))?;

            column_strategies.push(strategy);
        }

        Ok(ToRecordBatch {
            column_strategies,
            schema,
        })
    }

    /// Logs buffer description and sizes
    pub fn row_size_in_bytes(&self) -> usize {
        let mut total_bytes = 0;
        for (read, field) in self.column_strategies.iter().zip(self.schema.fields()) {
            let name = field.name();
            let desc = read.buffer_desc();
            let bytes_per_row = desc.bytes_per_row();
            info!("Column '{name}'\nBytes used per row: {bytes_per_row}");
            total_bytes += bytes_per_row;
        }
        info!("Total memory usage per row for single transit buffer: {total_bytes}");
        total_bytes
    }

    pub fn allocate_buffer(
        &self,
        max_batch_size: usize,
        fallibale_allocations: bool,
    ) -> Result<ColumnarAnyBuffer, Error> {
        let descs = self.column_strategies.iter().map(|cs| cs.buffer_desc());

        let row_set_buffer = if fallibale_allocations {
            ColumnarAnyBuffer::try_from_descs(max_batch_size, descs)
                .map_err(|err| map_allocation_error(err, &self.schema))?
        } else {
            ColumnarAnyBuffer::from_descs(max_batch_size, descs)
        };
        Ok(row_set_buffer)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn buffer_to_record_batch(
        &self,
        odbc_buffer: &ColumnarAnyBuffer,
    ) -> Result<RecordBatch, MappingError> {
        let arrow_columns = self
            .column_strategies
            .iter()
            .enumerate()
            .map(|(index, strat)| {
                let column_view = odbc_buffer.column(index);
                strat.fill_arrow_array(column_view)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let record_batch = RecordBatch::try_new(self.schema.clone(), arrow_columns).unwrap();
        Ok(record_batch)
    }
}

fn map_allocation_error(error: odbc_api::Error, schema: &Schema) -> Error {
    match error {
        odbc_api::Error::TooLargeColumnBufferSize {
            buffer_index,
            num_elements,
            element_size,
        } => Error::ColumnFailure {
            name: schema.field(buffer_index as usize).name().clone(),
            index: buffer_index as usize,
            source: ColumnFailure::TooLarge {
                num_elements,
                element_size,
            },
        },
        _ => {
            panic!("Unexpected error in upstream ODBC api error library")
        }
    }
}
