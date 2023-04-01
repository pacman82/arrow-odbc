# arrow-odbc

[![Docs](https://docs.rs/arrow-odbc/badge.svg)](https://docs.rs/arrow-odbc/)
[![Licence](https://img.shields.io/crates/l/arrow-odbc)](https://github.com/pacman82/arrow-odbc/blob/master/License)
[![Crates.io](https://img.shields.io/crates/v/arrow-odbc)](https://crates.io/crates/arrow-odbc)

Fill Apache Arrow arrays from ODBC data sources. `arrow-odbc` is build on top of the [`arrow`](https://crates.io/crates/arrow) and [`odbc-api`](https://crates.io/crates/odbc-api) crates and enables you to read the data of an ODBC data source as sequence of Apache Arrow record batches. `arrow-odbc` can also be used to insert the contens of Arrow record batches into a database table.

## About Arrow

> [Apache Arrow](https://arrow.apache.org/) defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead.

## About ODBC

[ODBC](https://docs.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc) (Open DataBase Connectivity) is a standard which enables you to access data from a wide variaty of data sources using SQL.

## Usage

```rust
use arrow_odbc::{odbc_api::{Environment, ConnectionOptions}, OdbcReader};

const CONNECTION_STRING: &str = "\
    Driver={ODBC Driver 17 for SQL Server};\
    Server=localhost;\
    UID=SA;\
    PWD=My@Test@Password1;\
";

fn main() -> Result<(), anyhow::Error> {

    let odbc_environment = Environment::new()?;
    
    // Connect with database.
    let connection = odbc_environment.connect_with_connection_string(
        CONNECTION_STRING,
        ConnectionOptions::default(),
    )?;

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

## Matching of ODBC to Arrow types then querying

| ODBC                     | Arrow                |
| ------------------------ | -------------------- |
| Numeric(p <= 38)         | Decimal128           |
| Decimal(p <= 38, s >= 0) | Decimal128           |
| Integer                  | Int32                |
| SmallInt                 | Int16                |
| Real                     | Float32              |
| Float(p <=24)            | Float32              |
| Double                   | Float64              |
| Float(p > 24)            | Float64              |
| Date                     | Date32               |
| LongVarbinary            | Binary               |
| Timestamp(p = 0)         | TimestampSecond      |
| Timestamp(p: 1..3)       | TimestampMilliSecond |
| Timestamp(p: 4..6)       | TimestampMicroSecond |
| Timestamp(p >= 7 )       | TimestampNanoSecond  |
| BigInt                   | Int64                |
| TinyInt                  | Int8                 |
| Bit                      | Boolean              |
| Varbinary                | Binary               |
| Binary                   | FixedSizedBinary     |
| All others               | Utf8                 |

## Matching of Arrow to ODBC types then inserting

| Arrow                 | ODBC               |
| --------------------- | ------------------ |
| Utf8                  | VarChar            |
| LargeUtf8             | VarChar            |
| Decimal128(p, s = 0)  | VarChar(p + 1)     |
| Decimal128(p, s != 0) | VarChar(p + 2)     |
| Decimal128(p, s < 0)  | VarChar(p - s + 1) |
| Decimal256(p, s = 0)  | VarChar(p + 1)     |
| Decimal256(p, s != 0) | VarChar(p + 2)     |
| Decimal256(p, s < 0)  | VarChar(p - s + 1) |
| Int8                  | TinyInt            |
| Int16                 | SmallInt           |
| Int32                 | Integer            |
| Int64                 | BigInt             |
| Float16               | Real               |
| Float32               | Real               |
| Float64               | Double             |
| Timestamp s           | Timestamp(7)       |
| Timestamp ms          | Timestamp(7)       |
| Timestamp us          | Timestamp(7)       |
| Timestamp ns          | Timestamp(7)       |
| Date32                | Date               |
| Date64                | Date               |
| Time32 s              | Time               |
| Time32 ms             | VarChar(12)        |
| Time64 us             | VarChar(15)        |
| Time64 ns             | VarChar(16)        |
| Binary                | Varbinary          |
| FixedBinary(l)        | Varbinary(l)       |
| All others            | Unsupported        |

The mapping for insertion is not the optimal yet, but before spending a lot of work on improving it I was curious that usecase would pop up for users. So if something does not work, but maybe could provided a better mapping of Arrow to ODBC types, feel free to open an issue. If you do so please give a lot of context of what you are trying to do.

## Supported Arrow types

Appart from the afformentioned Arrow types `Uint8` is also supported if specifying the Arrow schema directly.
