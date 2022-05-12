# Changelog

## 0.13.0

* `panic` is now default behaviour on allocation errors. Activate `fallibale_allocations` in the `BufferAllocationOptions` in order to get a recoverable error instead.

## 0.12.0

* Update odbc-api `>= 0.39, < 0.40` -> `>= 0.40 < 0.41`

## 0.11.0

* Update odbc-api `>= 0.38, < 0.39` -> `>= 0.39, < 0.40`
* Support for fetching values from `VARCHAR(max)` and `VARBINARY(max)` columns, through specifying upper limits using `BufferAllocationOptions` in `OdbcReader::with`.

## 0.10.0

* Update odbc-api `>= 0.36, < 0.37` -> `>= 0.38, < 0.39`
* Recoverable errors if allocation for binary or text columns fails.

## 0.9.2

* Update arrow `>= 7.0.0, < 10` -> `>= 7.0.0, < 13`

## 0.9.1

* Update arrow `>= 7.0.0, < 10` -> `>= 7.0.0, < 12`

## 0.9.0

* Update odbc-api `>= 0.33.0, < 0.36` -> `0.36 < 0.37`

## 0.8.5

* Update arrow `>= 7.0.0, < 10` -> `>= 7.0.0, < 11`

## 0.8.4

* Update odbc-api `>= 0.33.0, < 0.35` -> `>= 0.33.0, < 0.36`

## 0.8.3

* Update arrow `>= 7.0.0, < 8` -> `>= 7.0.0, < 10`

## 0.8.2

* Update odbc-api `>= 0.31.0, < 0.33` -> `>= 0.33.0, < 0.35`

## 0.8.1

* Update arrow `>= 6.1.0, < 7` -> `>= 7.0.0, < 8`

## 0.8.0

* Use Rust edition 2021
* Update arrow `>= 6.1.0, < 7` -> `>= 7.0.0, < 8`
* Update odbc-api `>= 0.31.0, < 0.33` -> `>= 0.33.0, < 0.34`

## 0.7.2

* Fix: Formatting of error message for `ZeroSizedColumn`.

## 0.7.1

* `Error::ColumnFailure` now prints also the errors cause.

## 0.7.0

* `InvalidDisplaySize` replaced with `ZeroSizedColumn`.
* Refactored error handling, to have separate variant for column specific errors.

## 0.6.4

* Base allocations of text columns on column size instead of octet length.

## 0.6.3

* Fixed an issue there not enough memory to hold the maximum string size has been allocated, if querying a VARCHAR column on windows or an NVARCHAR column on a non-windows platform.

## 0.6.2

* Update arrow v6.0.0 -> `>= 6.1.0, < 7`
* Update odbc-api v0.31.0 -> `>= 0.31.0, < 0.33`

## 0.6.1

* Fix: There had been issue causing an overflow for timestamps with Microseconds precision.

## 0.6.0

* Update odbc-api v0.30.0 -> v0.31.0

## 0.5.0

* Update arrow v6.0.0 -> v6.1.0
* Update odbc-api v0.29.0 -> v0.30.0
* Introduced `arrow_schema_from` to support inferring arrow schemas without creating an `OdbcReader`.

## 0.4.1

* Estimate memory usage of text columns more accuratly.

## 0.4.0

* Udpate arrow v5.4.0 -> v6.0.0

## 0.3.0

* Update arrow v5.4.0 -> v5.5.0
* Update odbc-api v0.28.0 -> v0.29.0

## 0.2.1

* Updated code examples to odbc-api use safe Environment construction introduced in `odbc-api` version 0.28.3

## 0.2.0

* `odbc-api` version 0.28.0
* `arrow` version 5.4.0

## 0.1.2

* Support fixed sized binary types.

## 0.1.1

* Add Readme path to manifest

## 0.1.0

Initial release

Allows for fetching arrow batches from ODBC data sources

* `arrow` version 5.3.0
* `odbc-api` version 0.27.3
