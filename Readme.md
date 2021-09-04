# odbc-arrow

[![Licence](https://img.shields.io/crates/l/odbc-api)](https://github.com/pacman82/odbc-arrow/blob/master/License)

Fill Apache Arrow arrays from ODBC data sources. This crate is build on top of the [`arrow`](https://crates.io/crates/arrow) and [`odbc-api`](https://crates.io/crates/odbc-api) crate and enables you to read the data of an ODBC data source as sequence of Apache Arrow record batches.

## About Arrow

> [Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead.

## About ODBC

[ODBC](https://docs.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc) (Open DataBase Connectivity) is a standard which enables you to access data from a wide variaty of data sources using SQL.

## Usage

```rust
use odbc_arrow::{odbc_api::Environment, OdbcReader};

const CONNECTION_STRING: &str = "\
    Driver={ODBC Driver 17 for SQL Server};\
    Server=localhost;\
    UID=SA;\
    PWD=My@Test@Password1;\
";

fn main() -> Result<(), anyhow::Error> {
    // Your application is fine if you spin up only one Environment.
    let odbc_environment = unsafe {
        Environment::new().unwrap()
    };
    
    // Connect with database.
    let connection = odbc_environment.connect_with_connection_string(CONNECTION_STRING)?;

    // This SQL statement does not require any arguments.
    let parameters = ();

    // Execute query and create result set
    let cursor = connection
        .execute("SELECT * FROM MyTable", parameters)?
        .expect("SELECT statement must produce a cursor");

    // Each batch shall only consist of maximum 10.000 rows.
    let max_batch_size = 10_000;

    // Read result set as arrow batches. Infer Arrow types automatically using the meta
    // information of `cursor`.
    let arrow_record_batches = OdbcReader::new(cursor, max_batch_size)?;

    for batch in arrow_record_batches {
        // ... process batch ...
    }
    Ok(())
}
```
