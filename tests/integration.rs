use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, DecimalArray,
        FixedSizeBinaryArray, Float16Array, Float32Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt8Array, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use chrono::NaiveDate;
use float_eq::assert_float_eq;
use half::f16;
use lazy_static::lazy_static;

use arrow_odbc::{
    arrow::array::Float64Array,
    arrow_schema_from,
    odbc_api::{
        sys::{AttrConnectionPooling, AttrCpMatch},
        Connection, Environment,
    },
    BufferAllocationOptions, ColumnFailure, Error, OdbcReader, OdbcWriter, WriterError,
};
use odbc_api::{buffers::TextRowSet, Cursor, IntoParameter};

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

    let array_any = fetch_arrow_data(table_name, "INTEGER", "(1),(NULL),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(array_vals.is_valid(0));
    assert!(array_vals.is_null(1));
    assert!(array_vals.is_valid(2));
    assert_eq!([1, 0, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 32 Bit directly from the datasource
#[test]
fn fetch_32bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "INTEGER NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 16 Bit directly from the datasource
#[test]
fn fetch_16bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "SMALLINT NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 8 Bit directly from the datasource
#[test]
fn fetch_8bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "TINYINT NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch with non nullable Integer 8 Bit usigned integer. Since that type would never
/// interferred from the Database automatically it must be specified explicitly in a schema
#[test]
fn fetch_8bit_unsigned_integer_explicit_schema() {
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
        Err(Error::ColumnFailure {
            source: ColumnFailure::UnsupportedArrowType(DataType::UInt16),
            index: 0,
            name: _
        })
    ))
}

/// Fill a record batch with non nullable Boolean from Bits
#[test]
fn fetch_boolean() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "BIT NOT NULL", "(1),(0),(1)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<BooleanArray>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert!(array_vals.value(0));
    assert!(!array_vals.value(1));
    assert!(array_vals.value(2));
}

/// Fill a record batch with nullable Booleans from Bits
#[test]
fn fetch_nullable_boolean() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "BIT", "(1),(NULL),(0)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<BooleanArray>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert!(array_vals.is_valid(0));
    assert!(array_vals.value(0));
    assert!(array_vals.is_null(1));
    assert!(array_vals.is_valid(2));
    assert!(!array_vals.value(2));
}

/// Fill a record batch with non nullable `f32` directly from the datasource
#[test]
fn fetch_32bit_floating_point() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "REAL NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_float_eq!(&[1., 2., 3.][..], array_vals.values(), abs_all <= 000.1);
}

/// Fill a record batch with non nullable `f64` directly from the datasource
#[test]
fn fetch_64bit_floating_point() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "DOUBLE PRECISION NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Float64Array>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_float_eq!(&[1., 2., 3.][..], array_vals.values(), abs_all <= 000.1);
}

/// Fill a record batch with non nullable `i64` directly from the datasource
#[test]
fn fetch_64bit_integer() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "BIGINT NOT NULL", "(1),(2),(3)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Int64Array>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!([1, 2, 3], array_vals.values());
}

/// Fill a record batch of Strings from a varchar source column
#[test]
fn fetch_varchar() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "VARCHAR(50)", "('Hello'),('Bonjour'),(NULL)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<StringArray>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!("Hello", array_vals.value(0));
    assert_eq!("Bonjour", array_vals.value(1));
    assert!(array_vals.is_null(2));
}

/// Fill a record batch of Strings from a nvarchar source column
#[test]
fn fetch_nvarchar() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "NVARCHAR(50)", "('Hello'),('Bonjour'),(NULL)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<StringArray>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!("Hello", array_vals.value(0));
    assert_eq!("Bonjour", array_vals.value(1));
    assert!(array_vals.is_null(2));
}

/// Fill a record batch of Dates
#[test]
fn fetch_dates() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "DATE", "('2021-04-09'),(NULL),('2002-09-30')").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Date32Array>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9)),
        array_vals.value_as_date(0)
    );
    assert!(array_vals.is_null(1));
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30)),
        array_vals.value_as_date(2)
    );
}

/// Fill a record batch of non nullable Dates
#[test]
fn fetch_non_null_dates() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "DATE NOT NULL", "('2021-04-09'),('2002-09-30')").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<Date32Array>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9)),
        array_vals.value_as_date(0)
    );
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30)),
        array_vals.value_as_date(1)
    );
}

/// Fill a record batch of non nullable timestamps with milliseconds precision
#[test]
fn fetch_non_null_date_time() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(
        table_name,
        "DATETIME NOT NULL",
        "('2021-04-09 18:57:50.12'),('2002-09-30 12:43:17.45')",
    )
    .unwrap();

    let array_vals = array_any
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9).and_hms_milli(18, 57, 50, 120)),
        array_vals.value_as_datetime(0)
    );
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30).and_hms_milli(12, 43, 17, 450)),
        array_vals.value_as_datetime(1)
    );
}

/// Fill a record batch of nullable timestamps with milliseconds precision
#[test]
fn fetch_date_time_us() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(
        table_name,
        "DATETIME2(6)",
        "('2021-04-09 18:57:50'),(NULL),('2002-09-30 12:43:17')",
    )
    .unwrap();

    let array_vals = array_any
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9).and_hms_milli(18, 57, 50, 0)),
        array_vals.value_as_datetime(0)
    );
    assert!(array_vals.is_null(1));
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30).and_hms_milli(12, 43, 17, 00)),
        array_vals.value_as_datetime(2)
    );
}

/// Fill a record batch of nullable timestamps with milliseconds precision
#[test]
fn fetch_date_time_ms() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(
        table_name,
        "DATETIME",
        "('2021-04-09 18:57:50'),(NULL),('2002-09-30 12:43:17')",
    )
    .unwrap();

    let array_vals = array_any
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9).and_hms_milli(18, 57, 50, 0)),
        array_vals.value_as_datetime(0)
    );
    assert!(array_vals.is_null(1));
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30).and_hms_milli(12, 43, 17, 00)),
        array_vals.value_as_datetime(2)
    );
}

/// Fill a record batch of non nullable timestamps with nanoseconds precision
#[test]
fn fetch_non_null_date_time_ns() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(
        table_name,
        "DATETIME2 NOT NULL",
        "('2021-04-09 18:57:50.1234567'),('2002-09-30 12:43:17.456')",
    )
    .unwrap();

    let array_vals = array_any
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!(
        Some(NaiveDate::from_ymd(2021, 4, 9).and_hms_nano(18, 57, 50, 123_456_700)),
        array_vals.value_as_datetime(0)
    );
    assert_eq!(
        Some(NaiveDate::from_ymd(2002, 9, 30).and_hms_nano(12, 43, 17, 456_000_000)),
        array_vals.value_as_datetime(1)
    );
}

/// Fill a record batch of Dates
#[test]
fn fetch_decimals() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any =
        fetch_arrow_data(table_name, "DECIMAL(5,2) NOT NULL", "(123.45),(678.90)").unwrap();

    let array_vals = array_any.as_any().downcast_ref::<DecimalArray>().unwrap();

    // Assert that the correct values are found within the arrow batch
    assert_eq!("123.45", array_vals.value_as_string(0));
    assert_eq!("678.90", array_vals.value_as_string(1));
}

/// Fetch variable sized binary data binary data
#[test]
fn fetch_varbinary_data() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some values (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(30) NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?)", table_name);
    // Use prepared query and arguments for insertion, since literal representation depends a lot
    // on the DB under test.
    let mut insert = conn.prepare(&sql).unwrap();
    insert.execute(&b"Hello".into_parameter()).unwrap();
    insert.execute(&b"World".into_parameter()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {} ORDER BY id", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

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
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b"Hello", array_vals.value(0));
    assert_eq!(b"World", array_vals.value(1));
}

/// Fetch fixed sized binary data binary data
#[test]
fn fetch_fixed_sized_binary_data() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some values (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BINARY(5) NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?)", table_name);
    // Use prepared query and arguments for insertion, since literal representation depends a lot
    // on the DB under test.
    let mut insert = conn.prepare(&sql).unwrap();
    insert.execute(&b"Hello".into_parameter()).unwrap();
    insert.execute(&b"World".into_parameter()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {} ORDER BY id", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

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
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(b"Hello", array_vals.value(0));
    assert_eq!(b"World", array_vals.value(1));
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
    assert_float_eq!(&[1., 2., 3.][..], array_vals.values(), abs_all <= 000.1);
}

#[test]
fn infer_schema() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL NOT NULL"]).unwrap();

    // Prepare query to get metadata
    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 1 entries.
    let max_batch_size = 1;

    // Instantiate reader with Arrow schema and ODBC cursor
    let reader = OdbcReader::new(cursor, max_batch_size).unwrap();

    let actual = reader.schema();
    let expected = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    assert_eq!(expected, actual)
}

#[test]
fn fetch_schema_for_table() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL NOT NULL"]).unwrap();

    // Prepare query to get metadata
    let sql = format!("SELECT a FROM {}", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();

    // Now that we have prepared statement, we want to use it to query metadata.
    let schema = arrow_schema_from(&mut prepared).unwrap();

    assert_eq!(
        "Field { \
            name: \"a\", \
            data_type: Float32, \
            nullable: false, \
            dict_id: 0, \
            dict_is_ordered: false, \
            metadata: None \
        }",
        schema.to_string()
    )
}

/// Allocating octet length bytes is not enough if the column on the database is encoded in UTF-16
/// since all codepoints in range from U+0800 to U+FFFF take three bytes in UTF-8 but only two bytes
/// in UTF-16. We test this with the 'Trade Mark Sign' (`™`) (U+2122).
///
/// For this test to be meaningful it must run on a Linux platform, since only then we do query the
/// wide column with a narrow buffer and therfore convert UTF-16 to UTF-8 within the dbms.
#[test]
fn should_allocate_enough_memory_for_wchar_column_bound_to_u8() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "NCHAR(1) NOT NULL", "('™')").unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = array_any.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!("™", array_vals.value(0));
}

/// Allocating octet length bytes is not enough if the column on the database is encoded in UTF-8
/// since all codepoints in range from U+0000 to U+007F take two bytes in UTF-16 but only one byte
/// in UTF-8. We test this with the letter a (U+0061).
///
/// For this test to be meaningful it must run on a windows platform, since only then we do query the
/// wide column with a narrow buffer and therfore convert UTF-8 to UTF-16 within the dbms.
#[test]
fn should_allocate_enough_memory_for_varchar_column_bound_to_u16() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    let array_any = fetch_arrow_data(table_name, "CHAR(1) NOT NULL", "('Ü')").unwrap();

    // Assert that the correct values are found within the arrow batch
    let array_vals = array_any.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!("Ü", array_vals.value(0));
}

/// Often than VARCHAR(MAX) is used the actual values in these columns are in the range of 100kb and
/// not several kb. Sadly if we allocate the buffers, we have to assume the largest possible element
/// this test verifies that users can specify sensible upper limits using their domain knowledge
/// about the table in order to prevent out of memory issues.
#[test]
fn should_allow_to_fetch_from_varchar_max() {
    // Given
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // When
    let max_batch_size = 100;
    let schema = None;
    let buffer_allocation_options = BufferAllocationOptions {
        max_text_size: Some(1024),
        ..Default::default()
    };
    let result = OdbcReader::with(cursor, max_batch_size, schema, buffer_allocation_options);

    // Then
    // In particular we do **not** get either a zero sized column or out of memory error.
    assert!(result.is_ok())
}

/// If column limits are too small and truncation occurs, we expect an error to be raised.
#[test]
fn should_error_for_truncation() {
    // Given a column with one value of length 9
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    let sql = format!("INSERT INTO {table_name} (a) VALUES ('123456789')");
    conn.execute(&sql, ()).unwrap();
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // When fetching that value with a text limit of 5
    let max_batch_size = 1;
    let schema = None;
    let buffer_allocation_options = BufferAllocationOptions {
        max_text_size: Some(5),
        ..Default::default()
    };
    let mut reader =
        OdbcReader::with(cursor, max_batch_size, schema, buffer_allocation_options).unwrap();
    let result = reader.next().unwrap();

    // Then we get an error, rather than the truncation only occurring as a warning.
    assert!(result.is_err())
}

/// Inserts the values in the literal into the database and returns them as an Arrow array.
fn fetch_arrow_data(
    table_name: &str,
    column_type: &str,
    literal: &str,
) -> Result<ArrayRef, anyhow::Error> {
    // Setup a table on the database
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &[column_type]).unwrap();
    // Insert values using literals
    let sql = format!("INSERT INTO {table_name} (a) VALUES {literal}");
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.

    // Batches will contain at most 100 entries.
    let max_batch_size = 100;

    let mut reader = OdbcReader::new(cursor, max_batch_size)?;

    // Batch for batch copy values from ODBC buffer into arrow batches
    let record_batch = reader.next().unwrap()?;

    Ok(record_batch.column(0).clone())
}

#[test]
fn should_allow_to_fetch_from_varbinary_max() {
    // Given
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(MAX)"]).unwrap();
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // When
    let max_batch_size = 100;
    let schema = None;
    let buffer_allocation_options = BufferAllocationOptions {
        max_binary_size: Some(1024),
        ..Default::default()
    };
    let result = OdbcReader::with(cursor, max_batch_size, schema, buffer_allocation_options);

    // Then
    // In particular we do **not** get either a zero sized column or out of memory error.
    assert!(result.is_ok())
}

#[test]
fn fallibale_allocations() {
    // Given
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(4096)"]).unwrap();
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    // When
    let max_batch_size = 100_000_000;
    let schema = None;
    let buffer_allocation_options = BufferAllocationOptions {
        fallibale_allocations: true,
        ..Default::default()
    };
    let result = OdbcReader::with(cursor, max_batch_size, schema, buffer_allocation_options);

    // Then
    // In particular we do **not** get either a zero sized column or out of memory error.
    assert!(result.is_err());
    assert!(matches!(
        result.err().unwrap(),
        Error::ColumnFailure {
            name: _,
            index: 0,
            source: ColumnFailure::TooLarge {
                num_elements: 100_000_000,
                element_size: 4096
            }
        }
    ));
}

#[test]
fn insert_does_not_support_list_type() {
    // Given a table and a db connection.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(4096)"]).unwrap();

    // When we try to create an OdbcWriter inserting an Arrow List
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::List(Box::new(Field::new("b", DataType::Utf8, true))),
        true,
    )]));

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let result = OdbcWriter::new(10, schema, prepared);

    // Then we recive an unsupported error
    assert!(matches!(
        result,
        Err(WriterError::UnsupportedArrowDataType(_))
    ))
}

/// Insert String data into database
#[test]
fn insert_text() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(4096)"]).unwrap();
    let array = StringArray::from(vec![Some("Hello"), None, Some("World")]);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "Hello\nNULL\nWorld";
    assert_eq!(expected, actual);
}

/// This test is most relevant on windows platforms, the UTF-8 is not the default encoding and text
/// should be encoded as UTF-16
#[test]
fn insert_non_ascii_text() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();
    let array = StringArray::from(vec![Some("Frühstück µ")]);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "Frühstück µ";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_booleans() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT"]).unwrap();
    let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch.clone(), batch]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\nNULL\n0\n1\nNULL\n0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_non_nullable_booleans() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT"]).unwrap();
    let array = BooleanArray::from(vec![Some(true), Some(false), Some(false)]);
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch.clone(), batch]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\n0\n0\n1\n0\n0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_int8() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TINYINT"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, true)]));
    let array1 = Int8Array::from(vec![Some(1), None, Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let array2 = Int8Array::from(vec![Some(4), None, Some(6)]);
    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array2)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1, batch2]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\nNULL\n3\n4\nNULL\n6";
    assert_eq!(expected, actual);
}

#[test]
fn insert_non_nullable_int8() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TINYINT"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int8, false)]));
    let array1 = Int8Array::from(vec![Some(1), Some(2), Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let array2 = Int8Array::from(vec![Some(4), Some(5), Some(6)]);
    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array2)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1, batch2]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\n2\n3\n4\n5\n6";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_int16() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["SMALLINT"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int16, true)]));
    let array1 = Int16Array::from(vec![Some(1), None, Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\nNULL\n3";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_int32() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let array1 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\nNULL\n3";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_int64() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIGINT"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let array1 = Int64Array::from(vec![Some(1), None, Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\nNULL\n3";
    assert_eq!(expected, actual);
}

#[test]
fn insert_non_nullable_unsigned_int8() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["SMALLINT"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt8, false)]));
    let array1 = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1\n2\n3";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_f32() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));
    let array1 = Float32Array::from(vec![Some(1.), None, Some(3.)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1.0\nNULL\n3.0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_f64() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["FLOAT(25)"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
    let array1 = Float64Array::from(vec![Some(1.), None, Some(3.)]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1.0\nNULL\n3.0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_nullable_f16() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float16, true)]));
    let array1: Float16Array = [Some(f16::from_f32(1.0)), None, Some(f16::from_f32(3.0))]
        .into_iter()
        .collect();
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1.0\nNULL\n3.0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_non_nullable_f16() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["REAL"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float16, false)]));
    let array1: Float16Array = [
        Some(f16::from_f32(1.0)),
        Some(f16::from_f32(2.0)),
        Some(f16::from_f32(3.0)),
    ]
    .into_iter()
    .collect();
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch1]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1.0\n2.0\n3.0";
    assert_eq!(expected, actual);
}

#[test]
fn insert_timestamp_with_seconds_precisions() {
    // Given a table and a record batch reader returning a batch with a text column.
    let table_name = function_name!().rsplit_once(':').unwrap().1;
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2(0)"]).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Timestamp(TimeUnit::Second, None), false)]));
    // Corresponds to single element array with entry 1970-05-09T14:25:11+0:00
    let array = TimestampSecondArray::from_vec(vec![11111111], None);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let reader = StubBatchReader::new(schema, vec![batch]);

    // When
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    let prepared = conn.prepare(&insert).unwrap();
    let row_capacity = 5;
    let mut writer = OdbcWriter::new(row_capacity, reader.schema(), prepared).unwrap();
    writer.write_all(reader).unwrap();

    // Then
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "1970-05-09 14:25:11";
    assert_eq!(expected, actual);
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
fn setup_empty_table(
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

/// Query the table and prints it contents to a string
pub fn table_to_string(conn: &Connection<'_>, table_name: &str, column_names: &[&str]) -> String {
    let cols = column_names.join(", ");
    let query = format!("SELECT {} FROM {}", cols, table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    cursor_to_string(cursor)
}

pub fn cursor_to_string(mut cursor: impl Cursor) -> String {
    let batch_size = 20;
    let mut buffer = TextRowSet::for_cursor(batch_size, &mut cursor, Some(8192)).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let mut text = String::new();

    while let Some(row_set) = row_set_cursor.fetch().unwrap() {
        for row_index in 0..row_set.num_rows() {
            if row_index != 0 {
                text.push('\n');
            }
            for col_index in 0..row_set.num_cols() {
                if col_index != 0 {
                    text.push(',');
                }
                text.push_str(
                    row_set
                        .at_as_str(col_index, row_index)
                        .unwrap()
                        .unwrap_or("NULL"),
                );
            }
        }
    }

    text
}

/// An arrow batch reader emitting predefined batches. Used to test insertion.
struct StubBatchReader {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl StubBatchReader {
    pub fn new(schema: SchemaRef, mut batches: Vec<RecordBatch>) -> Self {
        // We pop elements from the end, so we revert order of the batches. This way we do not
        // betray, the expectation that the batches will be emitted in the same order as constructed
        // in the `Vec` given to us.
        batches.reverse();
        Self { schema, batches }
    }
}

impl Iterator for StubBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.pop().map(Ok)
    }
}

impl RecordBatchReader for StubBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
