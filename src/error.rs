use thiserror::Error;

use crate::column_strategy::ColumnFailure;

/// A variation of things which can go wrong then creating an [`OdbcReader`].
#[derive(Error, Debug)]
pub enum Error {
    /// Failure to retrieve the number of columns from the result set.
    #[error("Unable to retrieve number of columns in result set.\n{0}")]
    UnableToRetrieveNumCols(odbc_api::Error),
    /// Indicates that the error is related to a specify column.
    #[error(
        "There is a problem with the SQL type of the column with name: {} and index {}",
        name,
        index
    )]
    ColumnFailure {
        // Name of the erroneous column
        name: String,
        // Index of the erroneous column
        index: usize,
        // Cause of the error
        source: ColumnFailure,
    },
}
