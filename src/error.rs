use thiserror::Error;

use crate::reader::ColumnFailure;

/// A variation of things which can go wrong then creating an [`crate::OdbcReader`].
#[derive(Error, Debug)]
pub enum Error {
    /// Failure to retrieve the number of columns from the result set.
    #[error("Unable to retrieve number of columns in result set.\n{0}")]
    UnableToRetrieveNumCols(odbc_api::Error),
    /// Indicates that the error is related to a specify column.
    #[error(
        "There is a problem with the SQL type of the column with name: {} and index {}:\n{source}",
        name,
        index
    )]
    ColumnFailure {
        // Name of the erroneous column
        name: String,
        // Zero based index of the erroneous column
        index: usize,
        // Cause of the error
        source: ColumnFailure,
    },
    /// Failure during constructing an OdbcReader, if it turns out the buffer memory size limit is
    /// too small.
    #[error(
        "The Odbc buffer is limited to a size of {max_bytes_per_batch} bytes. Yet a single row \
        does require up to {bytes_per_row}. This means the buffer is not large enough to hold a \
        single row of data. Please note that the buffers in ODBC must always be able to hold the \
        largest possible value of variadic types. You should either set a higher upper bound for \
        the buffer size, or limit the length of the variadic columns."
    )]
    OdbcBufferTooSmall{
        max_bytes_per_batch: usize,
        bytes_per_row: usize,
    },
}
