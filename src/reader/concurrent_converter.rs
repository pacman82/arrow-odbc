use super::to_record_batch::ToRecordBatch;


/// Conversion is the step of coping the content from an unbound ODBC buffer into an Arrow Table.
/// Doing it concurrently means to do it in a dedicated system thread. The input from this thread
/// comes from the thread filling the ODBC buffers by fetching data from the database. The output
/// would then be send to what would typically be the main thread running the application.
pub struct ConcurrentConverter{
    pub converter: ToRecordBatch
}