//! Generation of an insert statement for a given arrow table schema

use std::borrow::Cow;

use arrow::datatypes::Schema;
use odbc_api::Connection;

/// Creates an SQL insert statement from an arrow schema. The resulting statement will have one
/// placeholer (`?`) for each column in the statement.
///
/// **Note:**
///
/// If the column name contains any character which would make it not a valid qualifier for transact
/// SQL it will be wrapped in double quotes (`"`) within the insert schema. Valid names consist of
/// alpha numeric characters, `@`, `$`, `#` and `_`.
///
/// # Example
///
/// ```
/// use arrow_odbc::{
///     insert_statement_from_schema, QuoteDefensively,
///     arrow::datatypes::{Field, DataType, Schema},
/// };
///
/// let field_a = Field::new("a", DataType::Int64, false);
/// let field_b = Field::new("b", DataType::Boolean, false);
///
/// let schema = Schema::new(vec![field_a, field_b]);
/// let sql = insert_statement_from_schema(&schema, "MyTable", &QuoteDefensively);
///
/// assert_eq!("INSERT INTO MyTable (a, b) VALUES (?, ?)", sql)
/// ```
///
/// This function is automatically invoked by [`crate::OdbcWriter::with_connection`].
pub fn insert_statement_from_schema(
    schema: &Schema,
    table_name: &str,
    quoting: &dyn Quote,
) -> String {
    let fields = schema.fields();
    let num_columns = fields.len();
    let column_names: Vec<_> = (0..num_columns)
        .map(|i| fields[i].name().as_str())
        .collect();
    insert_statement_text(table_name, &column_names, quoting)
}

/// Controls if and how column names are quoted during insert statement generation.
pub trait Quote {
    /// Wraps column name in quotes, if need be.
    fn quote_column_name<'a>(&self, column_name: &'a str) -> Cow<'a, str>;
}

pub fn quoting_from_connection(conn: &Connection) -> Result<Box<dyn Quote>, odbc_api::Error> {
    let quoting: Box<dyn Quote> = if let Some(char) = conn.identifier_quote_char()? {
        Box::new(QuoteOffensively::new(char))
    } else {
        Box::new(QuoteDefensively)
    };
    Ok(quoting)
}

/// Quotes only if column name contains special characters (`@`, `$`, `#` and `_`). Will not quote
/// column name for which quoting is already detected. I.e. the column name starts with is wrapped
/// in quotes (`\``), double quotes (`"`) or square brackets (`[`, `]`).
///
/// This strategy is intended for situation there we are not sure wethere or not the DBMS supports
/// quoting identifiers at all.
pub struct QuoteDefensively;

impl Quote for QuoteDefensively {
    fn quote_column_name<'a>(&self, column_name: &'a str) -> Cow<'a, str> {
        let contains_invalid_characters = || column_name.contains(|c| !valid_in_column_name(c));
        let needs_quotes = contains_invalid_characters() && !is_quoted(column_name);
        if needs_quotes {
            Cow::Owned(format!("\"{column_name}\""))
        } else {
            Cow::Borrowed(column_name)
        }
    }
}

/// Quotes any column name by default, unless we suspect the column is already quoted.
///
/// If the DBMS supports quoting there is litte reason not to quote every identifier.
///
/// The column is always quoted using the quoting character unless it is already wrapped in
/// quotes (`\``), double quotes (`"`), square brackets (`[`, `]`).
pub struct QuoteOffensively {
    quoting_character: char,
}

impl QuoteOffensively {
    pub fn new(quoting_character: char) -> Self {
        QuoteOffensively { quoting_character }
    }
}

impl Quote for QuoteOffensively {
    fn quote_column_name<'a>(&self, column_name: &'a str) -> Cow<'a, str> {
        if is_quoted(column_name) {
            Cow::Borrowed(column_name)
        } else {
            let q = self.quoting_character;
            Cow::Owned(format!("{q}{column_name}{q}"))
        }
    }
}

/// Generates an insert statement using the table and column names.
///
/// `INSERT INTO <table> (<column_names 0>, <column_names 1>, ...) VALUES (?, ?, ...)`
fn insert_statement_text(table: &str, column_names: &[&'_ str], quoting: &dyn Quote) -> String {
    // Generate statement text from table name and headline
    let column_names = column_names
        .iter()
        .map(|cn| quoting.quote_column_name(cn))
        .collect::<Vec<_>>();
    let columns = column_names.join(", ");
    let values = column_names
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    // Do not finish the statement with a semicolon. There is anecodtical evidence of IBM db2 not
    // allowing the command, because it expects now multiple statements.
    // See: <https://github.com/pacman82/arrow-odbc/issues/63>
    format!("INSERT INTO {table} ({columns}) VALUES ({values})")
}

/// Check if this character is allowed in an unquoted column name
fn valid_in_column_name(c: char) -> bool {
    // See:
    // <https://stackoverflow.com/questions/4200351/what-characters-are-valid-in-an-sql-server-database-name>
    c.is_alphanumeric() || c == '@' || c == '$' || c == '#' || c == '_'
}

// We do not want to apply quoting in case the string is already quoted. See:
// <https://github.com/pacman82/arrow-odbc-py/issues/162>
//
// Another approach would have been to apply quoting after detecting keywords. Yet the list of
// reserved keywords is large. There is also the issue with different databases having different
// quoting rules. So the strategy choosen here is to apply quoting in less situations and not more,
// so the user has more control over the final statement. This crate is about arrow and odbc, less
// so about SQL dialects and statement construction.
fn is_quoted(identifier: &str) -> bool {
    (identifier.starts_with('"') && identifier.ends_with('"'))
        || identifier.starts_with('[') && identifier.ends_with(']')
        || identifier.starts_with('`') && identifier.ends_with('`')
}
