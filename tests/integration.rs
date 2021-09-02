use std::sync::Arc;

use lazy_static::lazy_static;

use odbc2arrow::{
    arrow::{
        array::Float64Array,
        datatypes::{DataType, Field, Schema},
    },
    odbc_api::{
        buffers::{BufferDescription, BufferKind, ColumnarRowSet},
        sys::{AttrConnectionPooling, AttrCpMatch},
        Connection, Cursor, Environment,
    },
    OdbcReader,
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
    let arrow_schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

    // Setup ODBC buffer to bind to cursor
    let max_rows = 100;
    let description = [BufferDescription {
        kind: BufferKind::F64,
        nullable: false,
    }];
    let row_set_buffer = ColumnarRowSet::new(max_rows, description.iter().copied());
    let row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();

    // Instantiate reader with Arrow schema and ODBC cursor
    let mut reader = OdbcReader {
        schema: Arc::new(arrow_schema),
        cursor: row_set_cursor,
    };

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

/// Like [`fetch_64bit_floating_point`], but utilizing a prepared query instead of a one shot.
#[test]
fn prepared_query() {
    let table_name = function_name!().rsplit_once(':').unwrap().1;

    // Setup a table on the database with some floats (so we can fetch them)
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DOUBLE PRECISION NOT NULL"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&sql, ()).unwrap();

    // Query column with values to get a cursor
    let sql = format!("SELECT a FROM {}", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();
    let cursor = prepared.execute(()).unwrap().unwrap();

    // Now that we have a cursor, we want to iterate over its rows and fill an arrow batch with it.
    let arrow_schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

    // Setup ODBC buffer to bind to cursor
    let max_rows = 100;
    let description = [BufferDescription {
        kind: BufferKind::F64,
        nullable: false,
    }];
    let row_set_buffer = ColumnarRowSet::new(max_rows, description.iter().copied());
    let row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();

    // Instantiate reader with Arrow schema and ODBC cursor
    let mut reader = OdbcReader {
        schema: Arc::new(arrow_schema),
        cursor: row_set_cursor,
    };

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
