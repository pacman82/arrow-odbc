//! Fill Apache Arrow arrays from ODBC data sources.
//!
//! ## Usage
//!
//! ```no_run
//! use arrow_odbc::{odbc_api::Environment, OdbcReader};
//!
//! const CONNECTION_STRING: &str = "\
//!     Driver={ODBC Driver 17 for SQL Server};\
//!     Server=localhost;\
//!     UID=SA;\
//!     PWD=My@Test@Password1;\
//! ";
//!
//! fn main() -> Result<(), anyhow::Error> {
//!     // Your application is fine if you spin up only one Environment.
//!     let odbc_environment = Environment::new()?;
//!     
//!     // Connect with database.
//!     let connection = odbc_environment.connect_with_connection_string(CONNECTION_STRING)?;
//!
//!     // This SQL statement does not require any arguments.
//!     let parameters = ();
//!
//!     // Execute query and create result set
//!     let cursor = connection
//!         .execute("SELECT * FROM MyTable", parameters)?
//!         .expect("SELECT statement must produce a cursor");
//!
//!     // Each batch shall only consist of maximum 10.000 rows.
//!     let max_batch_size = 10_000;
//!
//!     // Read result set as arrow batches. Infer Arrow types automatically using the meta
//!     // information of `cursor`.
//!     let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)?;
//!
//!     for batch in arrow_record_batches {
//!         // ... process batch ...
//!     }
//!
//!     Ok(())
//! }
//!
//!
//!
//! ```
mod error;
mod odbc_reader;
mod odbc_writer;
mod read_strategy;
mod schema;
mod date_time;

// Rexport odbc_api and arrow to make it easier for downstream crates to depend to avoid version
// mismatches
pub use arrow;
pub use odbc_api;

pub use self::{
    error::Error,
    odbc_reader::OdbcReader,
    odbc_writer::{OdbcWriter, WriterError, insert_into_table},
    read_strategy::{BufferAllocationOptions, ColumnFailure},
    schema::arrow_schema_from,
};
