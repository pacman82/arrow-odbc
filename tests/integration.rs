use std::sync::Arc;

use arrow::{array::{Array, BooleanArray, Float32Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, UInt8Array}, datatypes::{DataType, Field, Schema}};
use lazy_static::lazy_static;

use odbc_arrow::{
    arrow::array::Float64Array,
    odbc_api::{
        sys::{AttrConnectionPooling, AttrCpMatch},
        Connection, Environment,
    },
    Error, OdbcReader,
};

use stdext::function_name;

/// Connection string to our Microsoft SQL Database. Boot it up with docker-compose up
const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = unsafe {
        // Enable connection pooling. Let driver decide wether the attributes of two connection
        // are similar enough to change the attributes of a pooled one, to fit the requested
        // connection, or if it is cheaper to create a new Connection from scratch.
        Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
        let mut env = Environment::new().unwrap();
        // Strict is the default, and is set here to be explicit about it.
        env.set_connection_pooling_matching(AttrCpMatch::Strict).unwrap();
        env
    };
}

/// Fill a record batch with non nullable Integer 32 Bit directly from the datasource
#[test]
fn fetch_nullable_32bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(NULL),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(array_vals.is_valid(0));
    assert!(array_vals.is_null(1));
    assert!(array_vals.is_valid(2));
    assert_eq!([1, 0, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 32 Bit directly from the datasource
#[test]
fn fetch_32bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 16 Bit directly from the datasource
#[test]
fn fetch_16bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["SMALLINT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 8 Bit directly from the datasource
#[test]
fn fetch_8bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TINYINT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 8 Bit usigned integer
#[test]
fn fetch_8bit_unsigned_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TINYINT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    // Specify Uint8 manually, since inference of the arrow type from the sql type would yield a
    // signed 8 bit integer.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt8, false)]));

    let mut reader = OdbcReader::with_arrow_schema(cursor, max_batch_size, schema).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Observe that an explicitly specified Uint16 triggers an unsupported error
#[test]
fn unsupported_16bit_unsigned_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["SMALLINT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    // Specify Uint16 manually, since inference of the arrow type from the sql type would yield a
    // signed 16 bit integer.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt16, false)]));

    let result = OdbcReader::with_arrow_schema(cursor, max_batch_size, schema);

    assert!(matches!(
        result,
        Err(Error::UnsupportedArrowType(DataType::UInt16))
    ))
}

/// Fill a record batch with non nullable Boolean from Bits
#[test]
fn fetch_boolean() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(0),(1)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(true, array_vals.value(0));
    assert_eq!(false, array_vals.value(1));
    assert_eq!(true, array_vals.value(2));
}

/// Fill a record batch with nullable Booleans from Bits
#[test]
fn fetch_nullable_boolean() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(NULL),(0)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(array_vals.is_valid(0));
    assert_eq!(true, array_vals.value(0));
    assert!(array_vals.is_null(1));
    assert!(array_vals.is_valid(2));
    assert_eq!(false, array_vals.value(2));
}

/// Fill a record batch with non nullable `f32` directly from the datasource
#[test]
fn fetch_32bit_floating_point() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!([1., 2., 3.], array_vals.values());
}

/// Fill a record batch with non nullable `f64` directly from the datasource
#[test]
fn fetch_64bit_floating_point() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DOUBLE PRECISION NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!([1., 2., 3.], array_vals.values());
}

/// Fill a record batch with non nullable `i64` directly from the datasource
#[test]
fn fetch_64bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIGINT NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable `i64` directly from the datasource
#[test]
fn fetch_varchar() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES ('Hello'),('Bonjour'),(NULL)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!("Hello", array_vals.value(0));
    assert_eq!("Bonjour", array_vals.value(1));
    assert!(array_vals.is_null(2));
}

/// Like [`fetch_32bit_floating_point`], but utilizing a prepared query instead of a one shot.
#[test]
fn prepared_query() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();
    let cursor = prepared.execute(()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    // Instantiate reader with Arrow schema and ODBC cursor
    let mut reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    // Batch for batch copy values from ODBC buffer into arrow batches
    let arrow_batch = reader.next().unwrap().unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = arrow_batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!([1., 2., 3.], array_vals.values());
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"];
    let cols = column_types
        .iter()
        .zip(column_names)
        .map(|(ty, name)| format!("{} {}", name, ty))
        .collect::<Vec<_>>()
        .join(", ");

    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1),{});",
        table_name, cols
    );
    conn.execute(drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
}
