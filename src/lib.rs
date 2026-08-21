mod csv_spans;
mod native_csv;
mod rust_sbdf;
mod sbdf_index;
mod type_rules;

use arrow_array::{
    builder::PrimitiveBuilder,
    types::{ArrowPrimitiveType, Float32Type, Float64Type},
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_cast::parse::Parser;
use arrow_csv::reader::{Format as CsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use csv_spans::{CsvSpan, CsvSpanIter};
use memmap2::{Mmap, MmapOptions};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::ParquetMetaData;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict,
    PyFloat, PyInt, PyString, PyTime, PyTimeAccess,
};
use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_int, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(any(test, feature = "planned-offset-prototype"))]
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::{channel, sync_channel};
use std::sync::Arc;
#[cfg(any(test, feature = "planned-offset-prototype"))]
use std::sync::{Condvar, Mutex};
use std::thread;

#[cfg(all(unix, any(test, feature = "planned-offset-prototype")))]
use std::os::unix::fs::FileExt;
#[cfg(all(windows, any(test, feature = "planned-offset-prototype")))]
use std::os::windows::fs::FileExt;

const UNIX_EPOCH_DAYS_FROM_YEAR_ONE: i64 = 719_162;
const MILLIS_PER_DAY: i64 = 86_400_000;
const UNIX_EPOCH_MILLIS_FROM_YEAR_ONE: i64 = UNIX_EPOCH_DAYS_FROM_YEAR_ONE * MILLIS_PER_DAY;
const TARGET_PARQUET_BATCH_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PREFETCH_BATCH_BYTES: u64 = 32 * 1024 * 1024;
const SMALL_PARQUET_PARALLEL_BYTES: u64 = 128 * 1024 * 1024;
const LARGE_PARQUET_PARALLEL_BYTES: u64 = 768 * 1024 * 1024;
const MIN_PARALLEL_ROW_GROUP_BYTES: u64 = 4 * 1024 * 1024;
const MAX_PARALLEL_ROW_GROUPS_PER_FILE: usize = 256;
const TARGET_CSV_INPUT_BYTES: usize = 32 * 1024 * 1024;
const TARGET_CSV_DECODED_BYTES: usize = 64 * 1024 * 1024;
const NATIVE_CSV_MIN_COLUMNS: usize = 16;
const FRAGMENT_COPY_BUFFER_BYTES: usize = 256 * 1024;
#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
const MAX_PENDING_ENCODED_BYTES: usize = 256 * 1024 * 1024;
const MAX_PARALLEL_WORKERS: usize = 8;
static WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValueType {
    Bool,
    Int,
    Long,
    Float,
    Double,
    DateTime,
    Date,
    Time,
    TimeSpan,
    String,
    Binary,
}

impl ValueType {
    fn from_name(name: &str) -> PyResult<Self> {
        match name {
            "Boolean" => Ok(Self::Bool),
            "Integer" => Ok(Self::Int),
            "LongInteger" => Ok(Self::Long),
            "SingleReal" => Ok(Self::Float),
            "Real" => Ok(Self::Double),
            "DateTime" => Ok(Self::DateTime),
            "Date" => Ok(Self::Date),
            "Time" => Ok(Self::Time),
            "TimeSpan" => Ok(Self::TimeSpan),
            "String" => Ok(Self::String),
            "Binary" => Ok(Self::Binary),
            other => Err(PyValueError::new_err(format!(
                "unknown Spotfire type: {other}"
            ))),
        }
    }

    fn infer(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Ok(Self::Bool);
        }
        if value.is_instance_of::<PyInt>() {
            return Ok(Self::Long);
        }
        if value.is_instance_of::<PyFloat>() {
            return Ok(Self::Double);
        }
        if value.is_instance_of::<PyDateTime>() {
            return Ok(Self::DateTime);
        }
        if value.is_instance_of::<PyTime>() {
            return Ok(Self::Time);
        }
        if value.is_instance_of::<PyDelta>() {
            return Ok(Self::TimeSpan);
        }
        if value.is_instance_of::<PyDate>() {
            return Ok(Self::Date);
        }
        if value.is_instance_of::<PyString>() {
            return Ok(Self::String);
        }
        if value.is_instance_of::<PyBytes>() {
            return Ok(Self::Binary);
        }
        Err(PyTypeError::new_err(format!(
            "unsupported value type for SBDF export: {}",
            value.get_type().name()?
        )))
    }

    fn sbdf_type_id(self) -> u8 {
        match self {
            Self::Bool => 0x01,
            Self::Int => 0x02,
            Self::Long => 0x03,
            Self::Float => 0x04,
            Self::Double => 0x05,
            Self::DateTime => 0x06,
            Self::Date => 0x07,
            Self::Time => 0x08,
            Self::TimeSpan => 0x09,
            Self::String => 0x0a,
            Self::Binary => 0x0c,
        }
    }

    fn primitive_width(self) -> Option<usize> {
        match self {
            Self::Bool => Some(1),
            Self::Int | Self::Float => Some(4),
            Self::Long
            | Self::Double
            | Self::DateTime
            | Self::Date
            | Self::Time
            | Self::TimeSpan => Some(8),
            Self::String | Self::Binary => None,
        }
    }

    fn spotfire_name(self) -> &'static str {
        match self {
            Self::Bool => "Boolean",
            Self::Int => "Integer",
            Self::Long => "LongInteger",
            Self::Float => "SingleReal",
            Self::Double => "Real",
            Self::DateTime => "DateTime",
            Self::Date => "Date",
            Self::Time => "Time",
            Self::TimeSpan => "TimeSpan",
            Self::String => "String",
            Self::Binary => "Binary",
        }
    }
}

enum ColumnBuffer {
    Bool(Vec<u8>),
    Int(Vec<i32>),
    Long(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    TimeLike(Vec<i64>),
    String {
        _values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
    StringArena {
        values: Vec<u8>,
        offsets: Vec<usize>,
        lengths: Vec<c_int>,
    },
    Binary {
        _values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
}

fn primitive_value_view<T>(values: &[T]) -> rust_sbdf::ValueView<'_> {
    let width = std::mem::size_of::<T>();
    let length = std::mem::size_of_val(values);
    // SAFETY: a slice is contiguous and remains borrowed for the returned view's lifetime.
    let bytes = unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), length) };
    rust_sbdf::ValueView::Primitive {
        bytes,
        count: values.len(),
        width,
    }
}

impl ColumnBuffer {
    fn value_view(&self) -> rust_sbdf::ValueView<'_> {
        match self {
            Self::Bool(values) => primitive_value_view(values),
            Self::Int(values) => primitive_value_view(values),
            Self::Long(values) => primitive_value_view(values),
            Self::Float(values) => primitive_value_view(values),
            Self::Double(values) => primitive_value_view(values),
            Self::TimeLike(values) => primitive_value_view(values),
            Self::String { ptrs, lengths, .. } | Self::Binary { ptrs, lengths, .. } => {
                rust_sbdf::ValueView::Pointers {
                    pointers: ptrs,
                    lengths,
                }
            }
            Self::StringArena {
                values,
                offsets,
                lengths,
            } => rust_sbdf::ValueView::Arena {
                bytes: values,
                offsets,
                lengths,
            },
        }
    }

    fn clear_retain_capacity(&mut self) {
        match self {
            Self::Bool(values) => values.clear(),
            Self::Int(values) => values.clear(),
            Self::Long(values) => values.clear(),
            Self::Float(values) => values.clear(),
            Self::Double(values) => values.clear(),
            Self::TimeLike(values) => values.clear(),
            Self::String {
                _values,
                ptrs,
                lengths,
            }
            | Self::Binary {
                _values,
                ptrs,
                lengths,
            } => {
                _values.clear();
                ptrs.clear();
                lengths.clear();
            }
            Self::StringArena {
                values,
                offsets,
                lengths,
            } => {
                values.clear();
                offsets.clear();
                lengths.clear();
            }
        }
    }
}

enum NativeColumnBuffer<'a> {
    Owned(ColumnBuffer),
    BorrowedPrimitive {
        data: *const c_void,
        count: c_int,
        _array: &'a dyn Array,
    },
    BorrowedArray {
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
        _array: &'a dyn Array,
    },
}

impl NativeColumnBuffer<'_> {
    fn value_view(&self, value_type: ValueType) -> PyResult<rust_sbdf::ValueView<'_>> {
        match self {
            Self::Owned(buffer) => Ok(buffer.value_view()),
            Self::BorrowedPrimitive { data, count, .. } => {
                let count = usize::try_from(*count)
                    .map_err(|_| PyValueError::new_err("negative Arrow value count"))?;
                let width = value_type.primitive_width().ok_or_else(|| {
                    PyTypeError::new_err("variable-width Arrow buffer cannot be primitive")
                })?;
                let length = count
                    .checked_mul(width)
                    .ok_or_else(|| PyValueError::new_err("Arrow primitive buffer size overflow"))?;
                // SAFETY: `_array` keeps the Arrow allocation alive and the builder only creates
                // this variant for a contiguous primitive values buffer of `count * width` bytes.
                let bytes = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), length) };
                Ok(rust_sbdf::ValueView::Primitive {
                    bytes,
                    count,
                    width,
                })
            }
            Self::BorrowedArray { ptrs, lengths, .. } => Ok(rust_sbdf::ValueView::Pointers {
                pointers: ptrs,
                lengths,
            }),
        }
    }

    #[cfg(test)]
    fn borrows_arrow_payload(&self) -> bool {
        !matches!(self, Self::Owned(_))
    }
}

fn is_missing(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_none() {
        return Ok(true);
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(v.is_nan());
    }
    Ok(false)
}

fn days_since_epoch(year: i32, month: u8, day: u8) -> i64 {
    let adjust = if month <= 2 { 1 } else { 0 };
    let y = year - adjust;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe - 306) as i64
}

fn millis_from_datetime(value: &Bound<'_, PyDateTime>) -> i64 {
    let days = days_since_epoch(value.get_year(), value.get_month(), value.get_day());
    let day_ms = (value.get_hour() as i64 * 3600
        + value.get_minute() as i64 * 60
        + value.get_second() as i64)
        * 1000
        + (value.get_microsecond() as i64 / 1000);
    days * MILLIS_PER_DAY + day_ms
}

fn millis_from_date(value: &Bound<'_, PyDate>) -> i64 {
    days_since_epoch(value.get_year(), value.get_month(), value.get_day()) * MILLIS_PER_DAY
}

fn millis_from_time(value: &Bound<'_, PyTime>) -> i64 {
    (value.get_hour() as i64 * 3600 + value.get_minute() as i64 * 60 + value.get_second() as i64)
        * 1000
        + (value.get_microsecond() as i64 / 1000)
}

fn millis_from_timedelta(value: &Bound<'_, PyDelta>) -> i64 {
    value.get_days() as i64 * MILLIS_PER_DAY
        + value.get_seconds() as i64 * 1000
        + value.get_microseconds() as i64 / 1000
}

fn sbdf_millis_from_unix_days(days_since_unix_epoch: i64) -> i64 {
    (days_since_unix_epoch + UNIX_EPOCH_DAYS_FROM_YEAR_ONE) * MILLIS_PER_DAY
}

fn sbdf_millis_from_unix_millis(millis_since_unix_epoch: i64) -> i64 {
    millis_since_unix_epoch + UNIX_EPOCH_MILLIS_FROM_YEAR_ONE
}

fn build_column_buffer(
    value_type: ValueType,
    values: &[Bound<'_, PyAny>],
    invalids: &[u8],
) -> PyResult<ColumnBuffer> {
    match value_type {
        ValueType::Bool => Ok(ColumnBuffer::Bool(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        u8::from(value.extract::<bool>().unwrap_or(false))
                    }
                })
                .collect(),
        )),
        ValueType::Int => Ok(ColumnBuffer::Int(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        value.extract::<i32>().unwrap_or(0)
                    }
                })
                .collect(),
        )),
        ValueType::Long => Ok(ColumnBuffer::Long(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        value.extract::<i64>().unwrap_or(0)
                    }
                })
                .collect(),
        )),
        ValueType::Float => Ok(ColumnBuffer::Float(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0.0
                    } else {
                        value.extract::<f32>().unwrap_or(0.0)
                    }
                })
                .collect(),
        )),
        ValueType::Double => Ok(ColumnBuffer::Double(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0.0
                    } else {
                        value.extract::<f64>().unwrap_or(0.0)
                    }
                })
                .collect(),
        )),
        ValueType::DateTime => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_datetime(value.cast::<PyDateTime>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::Date => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_date(value.cast::<PyDate>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::Time => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_time(value.cast::<PyTime>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::TimeSpan => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_timedelta(value.cast::<PyDelta>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::String => {
            let values_buf: Vec<Vec<u8>> = values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        Vec::new()
                    } else {
                        value
                            .cast::<PyString>()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .as_bytes()
                            .to_vec()
                    }
                })
                .collect();
            let ptrs = values_buf
                .iter()
                .map(|value| value.as_ptr().cast::<c_char>())
                .collect();
            let lengths = values_buf
                .iter()
                .map(|value| value.len() as c_int)
                .collect();
            Ok(ColumnBuffer::String {
                _values: values_buf,
                ptrs,
                lengths,
            })
        }
        ValueType::Binary => {
            let values_buf: Vec<Vec<u8>> = values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        Vec::new()
                    } else {
                        value.cast::<PyBytes>().unwrap().as_bytes().to_vec()
                    }
                })
                .collect();
            let ptrs = values_buf
                .iter()
                .map(|value| value.as_ptr().cast::<c_char>())
                .collect();
            let lengths = values_buf
                .iter()
                .map(|value| value.len() as c_int)
                .collect();
            Ok(ColumnBuffer::Binary {
                _values: values_buf,
                ptrs,
                lengths,
            })
        }
    }
}

fn ensure_supported_arrow_type(column_name: &str, data_type: &DataType) -> PyResult<ValueType> {
    match data_type {
        DataType::Boolean => Ok(ValueType::Bool),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok(ValueType::Int)
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok(ValueType::Long),
        DataType::Float32 => Ok(ValueType::Float),
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            Ok(ValueType::Double)
        }
        DataType::Timestamp(_, _) => Ok(ValueType::DateTime),
        DataType::Date32 | DataType::Date64 => Ok(ValueType::Date),
        DataType::Time32(_) | DataType::Time64(_) => Ok(ValueType::Time),
        DataType::Duration(_) | DataType::Interval(_) => Ok(ValueType::TimeSpan),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ValueType::String),
        DataType::Binary | DataType::LargeBinary => Ok(ValueType::Binary),
        DataType::Null => Err(PyValueError::new_err(format!(
            "column '{column_name}' has Arrow type Null; provide column_types or cast upstream"
        ))),
        other if other.is_nested() => Err(PyValueError::new_err(format!(
            "nested Parquet types are not supported for SBDF export. column '{column_name}' has Arrow type '{other:?}'"
        ))),
        other => Err(PyValueError::new_err(format!(
            "automatic Spotfire type mapping is not available for column '{column_name}' with Arrow type '{other:?}'"
        ))),
    }
}

fn infer_arrow_schema(
    schema: &arrow_schema::Schema,
) -> PyResult<(Vec<String>, HashMap<String, ValueType>)> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let column_types = schema
        .fields()
        .iter()
        .map(|field| {
            ensure_supported_arrow_type(field.name(), field.data_type())
                .map(|vt| (field.name().clone(), vt))
        })
        .collect::<PyResult<HashMap<_, _>>>()?;
    Ok((columns, column_types))
}

fn get_column_types(
    columns: &[String],
    provided: Option<HashMap<String, String>>,
    schema: &arrow_schema::Schema,
) -> PyResult<HashMap<String, ValueType>> {
    if let Some(column_types) = provided {
        return columns
            .iter()
            .map(|column| {
                column_types
                    .get(column)
                    .ok_or_else(|| {
                        PyValueError::new_err(format!("missing type for column '{column}'"))
                    })
                    .and_then(|value| {
                        ValueType::from_name(value).map(|typed| (column.clone(), typed))
                    })
            })
            .collect();
    }
    infer_arrow_schema(schema).map(|(_, inferred)| inferred)
}

fn timestamp_array_to_millis(array: &dyn Array, unit: TimeUnit) -> PyResult<Vec<i64>> {
    match unit {
        TimeUnit::Second => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read timestamp(second) column"))?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) * 1_000
                    }
                })
                .collect())
        }
        TimeUnit::Millisecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err("failed to read timestamp(millisecond) column")
                })?;
            Ok((0..typed.len())
                .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) })
                .collect())
        }
        TimeUnit::Microsecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err("failed to read timestamp(microsecond) column")
                })?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) / 1_000
                    }
                })
                .collect())
        }
        TimeUnit::Nanosecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err("failed to read timestamp(nanosecond) column")
                })?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) / 1_000_000
                    }
                })
                .collect())
        }
    }
}

fn time_array_to_millis(array: &dyn Array, unit: TimeUnit) -> PyResult<Vec<i64>> {
    match unit {
        TimeUnit::Second => {
            let typed = array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(second) column"))?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) as i64 * 1_000
                    }
                })
                .collect())
        }
        TimeUnit::Millisecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(millisecond) column"))?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) as i64
                    }
                })
                .collect())
        }
        TimeUnit::Microsecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(microsecond) column"))?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) / 1_000
                    }
                })
                .collect())
        }
        TimeUnit::Nanosecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(nanosecond) column"))?;
            Ok((0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        0
                    } else {
                        typed.value(i) / 1_000_000
                    }
                })
                .collect())
        }
    }
}

fn checked_native_count(column_name: &str, len: usize) -> PyResult<c_int> {
    c_int::try_from(len).map_err(|_| {
        PyValueError::new_err(format!(
            "column '{column_name}' exceeds the maximum SBDF slice length"
        ))
    })
}

fn checked_value_length(column_name: &str, len: usize) -> PyResult<c_int> {
    c_int::try_from(len).map_err(|_| {
        PyValueError::new_err(format!(
            "value in column '{column_name}' exceeds the maximum SBDF value length"
        ))
    })
}

fn build_native_column_buffer<'a>(
    column_name: &str,
    array: &'a dyn Array,
) -> PyResult<(NativeColumnBuffer<'a>, Option<Vec<u8>>)> {
    checked_native_count(column_name, array.len())?;
    let invalids = (array.null_count() > 0).then(|| {
        (0..array.len())
            .map(|index| u8::from(array.is_null(index)))
            .collect::<Vec<_>>()
    });

    let buffer = match array.data_type() {
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read boolean column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Bool(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            u8::from(typed.value(i))
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Int8 => {
            let typed = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                PyTypeError::new_err(format!("failed to read int8 column '{column_name}'"))
            })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i32
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Int16 => {
            let typed = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                PyTypeError::new_err(format!("failed to read int16 column '{column_name}'"))
            })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i32
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Int32 => {
            let typed = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                PyTypeError::new_err(format!("failed to read int32 column '{column_name}'"))
            })?;
            if typed.null_count() == 0 {
                NativeColumnBuffer::BorrowedPrimitive {
                    data: typed.values().as_ptr().cast(),
                    count: checked_native_count(column_name, typed.len())?,
                    _array: array,
                }
            } else {
                NativeColumnBuffer::Owned(ColumnBuffer::Int(
                    (0..typed.len())
                        .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) })
                        .collect(),
                ))
            }
        }
        DataType::UInt8 => {
            let typed = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                PyTypeError::new_err(format!("failed to read uint8 column '{column_name}'"))
            })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i32
                        }
                    })
                    .collect(),
            ))
        }
        DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read uint16 column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i32
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Int64 => {
            let typed = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                PyTypeError::new_err(format!("failed to read int64 column '{column_name}'"))
            })?;
            if typed.null_count() == 0 {
                NativeColumnBuffer::BorrowedPrimitive {
                    data: typed.values().as_ptr().cast(),
                    count: checked_native_count(column_name, typed.len())?,
                    _array: array,
                }
            } else {
                NativeColumnBuffer::Owned(ColumnBuffer::Long(
                    (0..typed.len())
                        .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) })
                        .collect(),
                ))
            }
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read uint32 column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Long(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i64
                        }
                    })
                    .collect(),
            ))
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read uint64 column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::Long(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            typed.value(i) as i64
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read float32 column '{column_name}'"))
                })?;
            if typed.null_count() == 0 {
                NativeColumnBuffer::BorrowedPrimitive {
                    data: typed.values().as_ptr().cast(),
                    count: checked_native_count(column_name, typed.len())?,
                    _array: array,
                }
            } else {
                NativeColumnBuffer::Owned(ColumnBuffer::Float(
                    (0..typed.len())
                        .map(|i| {
                            if typed.is_null(i) {
                                0.0
                            } else {
                                typed.value(i)
                            }
                        })
                        .collect(),
                ))
            }
        }
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            if let Some(typed) = array.as_any().downcast_ref::<Float64Array>() {
                if typed.null_count() == 0 {
                    NativeColumnBuffer::BorrowedPrimitive {
                        data: typed.values().as_ptr().cast(),
                        count: checked_native_count(column_name, typed.len())?,
                        _array: array,
                    }
                } else {
                    NativeColumnBuffer::Owned(ColumnBuffer::Double(
                        (0..typed.len())
                            .map(|i| {
                                if typed.is_null(i) {
                                    0.0
                                } else {
                                    typed.value(i)
                                }
                            })
                            .collect(),
                    ))
                }
            } else {
                return Err(PyTypeError::new_err(format!(
                    "column '{column_name}' requires an explicit cast before SBDF export"
                )));
            }
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read date32 column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::TimeLike(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            sbdf_millis_from_unix_days(typed.value(i) as i64)
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Date64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read date64 column '{column_name}'"))
                })?;
            NativeColumnBuffer::Owned(ColumnBuffer::TimeLike(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            sbdf_millis_from_unix_millis(typed.value(i))
                        }
                    })
                    .collect(),
            ))
        }
        DataType::Timestamp(unit, _) => NativeColumnBuffer::Owned(ColumnBuffer::TimeLike(
            timestamp_array_to_millis(array, *unit)?
                .into_iter()
                .map(sbdf_millis_from_unix_millis)
                .collect(),
        )),
        DataType::Time32(unit) | DataType::Time64(unit) => {
            NativeColumnBuffer::Owned(ColumnBuffer::TimeLike(time_array_to_millis(array, *unit)?))
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read utf8 column '{column_name}'"))
                })?;
            let values = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        b"".as_slice()
                    } else {
                        typed.value(i).as_bytes()
                    }
                })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast()).collect();
            let lengths = values
                .iter()
                .map(|value| checked_value_length(column_name, value.len()))
                .collect::<PyResult<_>>()?;
            NativeColumnBuffer::BorrowedArray {
                ptrs,
                lengths,
                _array: array,
            }
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!(
                        "failed to read large utf8 column '{column_name}'"
                    ))
                })?;
            let values = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        b"".as_slice()
                    } else {
                        typed.value(i).as_bytes()
                    }
                })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast()).collect();
            let lengths = values
                .iter()
                .map(|value| checked_value_length(column_name, value.len()))
                .collect::<PyResult<_>>()?;
            NativeColumnBuffer::BorrowedArray {
                ptrs,
                lengths,
                _array: array,
            }
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!("failed to read binary column '{column_name}'"))
                })?;
            let values = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        b""
                    } else {
                        typed.value(i)
                    }
                })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast()).collect();
            let lengths = values
                .iter()
                .map(|value| checked_value_length(column_name, value.len()))
                .collect::<PyResult<_>>()?;
            NativeColumnBuffer::BorrowedArray {
                ptrs,
                lengths,
                _array: array,
            }
        }
        DataType::LargeBinary => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    PyTypeError::new_err(format!(
                        "failed to read large binary column '{column_name}'"
                    ))
                })?;
            let values = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        b""
                    } else {
                        typed.value(i)
                    }
                })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast()).collect();
            let lengths = values
                .iter()
                .map(|value| checked_value_length(column_name, value.len()))
                .collect::<PyResult<_>>()?;
            NativeColumnBuffer::BorrowedArray {
                ptrs,
                lengths,
                _array: array,
            }
        }
        other => {
            return Err(PyTypeError::new_err(format!(
                "unsupported Arrow type '{other:?}' for column '{column_name}'"
            )))
        }
    };
    Ok((buffer, invalids))
}

fn write_record_batch_to_sbdf(
    writer: &mut StreamingSbdfWriter,
    batch: &RecordBatch,
) -> PyResult<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if batch.num_columns() != writer.columns.len() {
        return Err(PyValueError::new_err(
            "record batch columns do not match writer schema",
        ));
    }

    let mut output = writer.take_output_buffer();
    let encode_result = encode_record_batch_into(
        &mut output,
        &writer.columns,
        &writer.column_types,
        writer.encoding_strategy(),
        batch,
    );
    writer.finish_output_buffer(output, encode_result)
}

fn encode_record_batch_into(
    output: &mut Vec<u8>,
    columns: &[String],
    column_types: &[ValueType],
    encoding: rust_sbdf::EncodingStrategy,
    batch: &RecordBatch,
) -> PyResult<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if batch.num_columns() != columns.len() {
        return Err(PyValueError::new_err(
            "record batch columns do not match writer schema",
        ));
    }
    rust_sbdf::begin_table_slice(output, columns.len()).map_err(PyRuntimeError::new_err)?;
    for (index, column_name) in columns.iter().enumerate() {
        let array = batch.column(index).as_ref();
        let (buffer, invalids) = build_native_column_buffer(column_name, array)?;
        let value_type = column_types[index];
        rust_sbdf::encode_column_slice(
            output,
            value_type.sbdf_type_id(),
            buffer.value_view(value_type)?,
            encoding,
            invalids.as_deref(),
        )
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to encode Rust SBDF column '{column_name}': {error}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn with_record_batch_column_inputs<R>(
    batch: &RecordBatch,
    config: &SbdfEncodingConfig,
    consume: impl FnOnce(&[rust_sbdf::ColumnInput<'_>]) -> Result<R, String>,
) -> Result<R, String> {
    if batch.num_columns() != config.columns.len() {
        return Err("record batch columns do not match planned SBDF schema".to_string());
    }
    let mut buffers = Vec::with_capacity(config.columns.len());
    let mut invalids = Vec::with_capacity(config.columns.len());
    for (index, column_name) in config.columns.iter().enumerate() {
        let (buffer, markers) =
            build_native_column_buffer(column_name, batch.column(index).as_ref())
                .map_err(|error| error.to_string())?;
        buffers.push(buffer);
        invalids.push(markers);
    }
    let inputs = buffers
        .iter()
        .zip(invalids.iter())
        .zip(config.column_types.iter())
        .map(|((buffer, markers), value_type)| {
            Ok(rust_sbdf::ColumnInput {
                type_id: value_type.sbdf_type_id(),
                values: buffer
                    .value_view(*value_type)
                    .map_err(|error| error.to_string())?,
                strategy: config.encoding_strategy(),
                invalids: markers.as_deref(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    consume(&inputs)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn plan_record_batch(
    batch: &RecordBatch,
    config: &SbdfEncodingConfig,
) -> Result<rust_sbdf::EncodedLayoutPlan, String> {
    with_record_batch_column_inputs(batch, config, rust_sbdf::plan_table_slice)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn encode_planned_record_batch<W: Write>(
    output: &mut W,
    batch: &RecordBatch,
    config: &SbdfEncodingConfig,
    plan: &rust_sbdf::EncodedLayoutPlan,
) -> Result<(), String> {
    with_record_batch_column_inputs(batch, config, |inputs| {
        rust_sbdf::encode_planned_table_slice(output, inputs, plan)
    })
}

fn write_native_csv_batch_to_sbdf(
    writer: &mut StreamingSbdfWriter,
    batch: &native_csv::NativeCsvBatch,
) -> PyResult<()> {
    if batch.rows() == 0 {
        return Ok(());
    }
    if batch.buffers.len() != writer.columns.len() {
        return Err(PyValueError::new_err(
            "native CSV batch columns do not match writer schema",
        ));
    }

    let mut output = writer.take_output_buffer();
    let encode_result = encode_native_csv_batch_into(
        &mut output,
        &writer.columns,
        &writer.column_types,
        writer.encoding_strategy(),
        batch,
    );
    writer.finish_output_buffer(output, encode_result)
}

fn encode_native_csv_batch_into(
    output: &mut Vec<u8>,
    columns: &[String],
    column_types: &[ValueType],
    encoding: rust_sbdf::EncodingStrategy,
    batch: &native_csv::NativeCsvBatch,
) -> PyResult<()> {
    if batch.rows() == 0 {
        return Ok(());
    }
    if batch.buffers.len() != columns.len() {
        return Err(PyValueError::new_err(
            "native CSV batch columns do not match writer schema",
        ));
    }
    rust_sbdf::begin_table_slice(output, columns.len()).map_err(PyRuntimeError::new_err)?;
    for (index, column_name) in columns.iter().enumerate() {
        let value_type = column_types[index];
        rust_sbdf::encode_column_slice(
            output,
            value_type.sbdf_type_id(),
            batch.buffers[index].value_view(),
            encoding,
            batch.invalids(index),
        )
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to encode Rust SBDF column '{column_name}': {error}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn with_native_csv_column_inputs<R>(
    batch: &native_csv::NativeCsvBatch,
    config: &SbdfEncodingConfig,
    consume: impl FnOnce(&[rust_sbdf::ColumnInput<'_>]) -> Result<R, String>,
) -> Result<R, String> {
    if batch.buffers.len() != config.columns.len() {
        return Err("native CSV batch columns do not match planned SBDF schema".to_string());
    }
    let inputs = batch
        .buffers
        .iter()
        .zip(config.column_types.iter())
        .enumerate()
        .map(|(index, (buffer, value_type))| rust_sbdf::ColumnInput {
            type_id: value_type.sbdf_type_id(),
            values: buffer.value_view(),
            strategy: config.encoding_strategy(),
            invalids: batch.invalids(index),
        })
        .collect::<Vec<_>>();
    consume(&inputs)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn plan_native_csv_batch(
    batch: &native_csv::NativeCsvBatch,
    config: &SbdfEncodingConfig,
) -> Result<rust_sbdf::EncodedLayoutPlan, String> {
    with_native_csv_column_inputs(batch, config, rust_sbdf::plan_table_slice)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn encode_planned_native_csv_batch<W: Write>(
    output: &mut W,
    batch: &native_csv::NativeCsvBatch,
    config: &SbdfEncodingConfig,
    plan: &rust_sbdf::EncodedLayoutPlan,
) -> Result<(), String> {
    with_native_csv_column_inputs(batch, config, |inputs| {
        rust_sbdf::encode_planned_table_slice(output, inputs, plan)
    })
}

fn load_parquet_reader_metadata(parquet_path: &str) -> PyResult<ArrowReaderMetadata> {
    let input = File::open(parquet_path).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "failed to open Parquet file '{parquet_path}': {error}"
        ))
    })?;
    ArrowReaderMetadata::load(&input, Default::default()).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "failed to read Parquet metadata '{parquet_path}': {error}"
        ))
    })
}

struct ParquetInputPlan {
    source_path: PathBuf,
    metadata: ArrowReaderMetadata,
    batch_size: usize,
}

fn select_parquet_workers(
    requested_workers: usize,
    total_row_groups: usize,
    max_row_groups_per_file: usize,
    total_uncompressed_bytes: u64,
) -> usize {
    if requested_workers <= 1 || total_row_groups <= 1 || max_row_groups_per_file <= 1 {
        return 1;
    }

    let available_workers = requested_workers.min(max_row_groups_per_file);
    if total_uncompressed_bytes <= SMALL_PARQUET_PARALLEL_BYTES {
        return available_workers.min(2);
    }

    let average_row_group_bytes = total_uncompressed_bytes.div_ceil(total_row_groups as u64);
    if total_uncompressed_bytes >= LARGE_PARQUET_PARALLEL_BYTES
        && average_row_group_bytes >= MIN_PARALLEL_ROW_GROUP_BYTES
        && max_row_groups_per_file <= MAX_PARALLEL_ROW_GROUPS_PER_FILE
    {
        return available_workers.min(3);
    }

    1
}

fn effective_parquet_batch_size(requested_rows: usize, metadata: &ParquetMetaData) -> usize {
    let total_rows = metadata.file_metadata().num_rows().max(0) as u64;
    let total_uncompressed_bytes = metadata
        .row_groups()
        .iter()
        .map(|row_group| row_group.total_byte_size().max(0) as u64)
        .sum::<u64>();
    cap_parquet_batch_size(requested_rows, total_rows, total_uncompressed_bytes)
}

fn should_prefetch_parquet_batch(batch_rows: usize, metadata: &ParquetMetaData) -> bool {
    let total_rows = metadata.file_metadata().num_rows().max(0) as u64;
    let total_uncompressed_bytes = metadata
        .row_groups()
        .iter()
        .map(|row_group| row_group.total_byte_size().max(0) as u64)
        .sum::<u64>();
    estimated_batch_bytes(batch_rows, total_rows, total_uncompressed_bytes)
        .is_none_or(|bytes| bytes <= MAX_PREFETCH_BATCH_BYTES)
}

fn estimated_batch_bytes(
    batch_rows: usize,
    total_rows: u64,
    total_uncompressed_bytes: u64,
) -> Option<u64> {
    if total_rows == 0 || total_uncompressed_bytes == 0 {
        return None;
    }
    Some(
        total_uncompressed_bytes
            .div_ceil(total_rows)
            .saturating_mul(batch_rows as u64),
    )
}

fn cap_parquet_batch_size(
    requested_rows: usize,
    total_rows: u64,
    total_uncompressed_bytes: u64,
) -> usize {
    if total_rows == 0 || total_uncompressed_bytes == 0 {
        return requested_rows;
    }

    let estimated_bytes_per_row = total_uncompressed_bytes.div_ceil(total_rows).max(1);
    let byte_limited_rows = (TARGET_PARQUET_BATCH_BYTES / estimated_bytes_per_row).max(1);
    requested_rows.min(byte_limited_rows as usize)
}

fn consume_with_one_batch_prefetch<I, T, F>(producer_iter: I, mut consume: F) -> Result<(), String>
where
    I: Iterator<Item = Result<T, String>> + Send + 'static,
    T: Send + 'static,
    F: FnMut(T) -> Result<(), String>,
{
    // A rendezvous channel keeps exactly one batch ahead: while the caller consumes batch N,
    // the producer may decode N+1, but it cannot start N+2 until N+1 is received.
    let (sender, receiver) = sync_channel(0);
    let producer = thread::Builder::new()
        .name("sbdf-batch-prefetch".to_string())
        .spawn(move || {
            for item in producer_iter {
                let reached_error = item.is_err();
                if sender.send(item).is_err() || reached_error {
                    break;
                }
            }
        })
        .map_err(|error| format!("failed to start Parquet prefetch thread: {error}"))?;

    let mut consume_result = Ok(());
    while let Ok(item) = receiver.recv() {
        match item {
            Ok(value) => {
                if let Err(error) = consume(value) {
                    consume_result = Err(error);
                    break;
                }
            }
            Err(error) => {
                consume_result = Err(error);
                break;
            }
        }
    }
    drop(receiver);

    let join_result = producer.join();
    if consume_result.is_ok() && join_result.is_err() {
        return Err("Parquet prefetch thread panicked".to_string());
    }
    consume_result
}

fn consume_batches<I, T, F>(producer_iter: I, mut consume: F, prefetch: bool) -> Result<(), String>
where
    I: Iterator<Item = Result<T, String>> + Send + 'static,
    T: Send + 'static,
    F: FnMut(T) -> Result<(), String>,
{
    if prefetch {
        return consume_with_one_batch_prefetch(producer_iter, consume);
    }
    for item in producer_iter {
        consume(item?)?;
    }
    Ok(())
}

#[derive(Clone)]
struct SbdfEncodingConfig {
    columns: Arc<Vec<String>>,
    column_types: Arc<Vec<ValueType>>,
    encoding_rle: bool,
    adaptive_encoding: bool,
}

impl SbdfEncodingConfig {
    fn new(
        columns: Vec<String>,
        typed_columns: &HashMap<String, ValueType>,
        encoding_rle: bool,
        adaptive_encoding: bool,
    ) -> PyResult<Self> {
        let column_types = columns
            .iter()
            .map(|column| {
                typed_columns.get(column).copied().ok_or_else(|| {
                    PyValueError::new_err(format!("missing type for column '{column}'"))
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            columns: Arc::new(columns),
            column_types: Arc::new(column_types),
            encoding_rle,
            adaptive_encoding,
        })
    }

    fn writer(&self, path: &Path, write_preamble: bool) -> PyResult<StreamingSbdfWriter> {
        StreamingSbdfWriter::new_typed(
            path,
            self.columns.as_ref().clone(),
            self.column_types.as_ref().clone(),
            self.encoding_rle,
            self.adaptive_encoding,
            write_preamble,
        )
    }

    fn fingerprint(&self) -> u64 {
        let mut checksum = FNV_OFFSET_BASIS;
        for (column, value_type) in self.columns.iter().zip(self.column_types.iter()) {
            checksum = fnv1a_update(checksum, column.as_bytes());
            checksum = fnv1a_update(checksum, &[0]);
            checksum = fnv1a_update(checksum, value_type.spotfire_name().as_bytes());
            checksum = fnv1a_update(checksum, &[u8::from(self.encoding_rle)]);
            checksum = fnv1a_update(checksum, &[u8::from(self.adaptive_encoding)]);
        }
        checksum
    }

    fn encoding_strategy(&self) -> rust_sbdf::EncodingStrategy {
        if self.adaptive_encoding {
            rust_sbdf::EncodingStrategy::Adaptive
        } else if self.encoding_rle {
            rust_sbdf::EncodingStrategy::Rle
        } else {
            rust_sbdf::EncodingStrategy::Plain
        }
    }
}

struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn create(target: &Path) -> Result<Self, String> {
        let parent = target
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create SBDF output directory '{}': {error}",
                parent.display()
            )
        })?;
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("output.sbdf");
        for _ in 0..100 {
            let id = WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(".{name}.work-{}-{id}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(format!(
                        "failed to create SBDF temporary workspace '{}': {error}",
                        path.display()
                    ))
                }
            }
        }
        Err("failed to allocate a unique SBDF temporary workspace".to_string())
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug)]
struct FragmentResult {
    sequence: usize,
    path: PathBuf,
    row_count: usize,
    byte_len: u64,
    schema_checksum: u64,
}

struct SbdfAssembler {
    target: PathBuf,
    workspace: TempWorkspace,
    writer: StreamingSbdfWriter,
    manifest: File,
    schema_checksum: u64,
}

impl SbdfAssembler {
    fn new(target: &Path, config: &SbdfEncodingConfig) -> Result<Self, String> {
        let workspace = TempWorkspace::create(target)?;
        let partial = workspace.path.join("final.partial");
        let writer = config
            .writer(&partial, true)
            .map_err(|error| error.to_string())?;
        let manifest_path = workspace.path.join("manifest.tsv");
        let mut manifest = File::create(&manifest_path).map_err(|error| {
            format!(
                "failed to create fragment manifest '{}': {error}",
                manifest_path.display()
            )
        })?;
        writeln!(
            manifest,
            "sequence\trows\tbytes\tchecksum\tschema_checksum\tpath"
        )
        .map_err(|error| format!("failed to initialize fragment manifest: {error}"))?;
        Ok(Self {
            target: target.to_path_buf(),
            workspace,
            writer,
            manifest,
            schema_checksum: config.fingerprint(),
        })
    }

    fn append_fragment(&mut self, fragment: &FragmentResult) -> Result<(), String> {
        let mut input = File::open(&fragment.path).map_err(|error| {
            format!(
                "failed to open SBDF fragment '{}': {error}",
                fragment.path.display()
            )
        })?;
        let actual_len = input
            .metadata()
            .map_err(|error| format!("failed to stat SBDF fragment: {error}"))?
            .len();
        if actual_len != fragment.byte_len {
            return Err(format!(
                "SBDF fragment {} length changed: expected {}, found {actual_len}",
                fragment.sequence, fragment.byte_len
            ));
        }
        if fragment.schema_checksum != self.schema_checksum {
            return Err(format!(
                "SBDF fragment {} schema checksum mismatch",
                fragment.sequence
            ));
        }

        let mut buffer = vec![0u8; FRAGMENT_COPY_BUFFER_BYTES];
        let mut checksum = FNV_OFFSET_BASIS;
        let mut copied_len = 0u64;
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|error| format!("failed to read SBDF fragment: {error}"))?;
            if read == 0 {
                break;
            }
            copied_len = copied_len.saturating_add(read as u64);
            checksum = fnv1a_update(checksum, &buffer[..read]);
            self.writer
                .append_bytes(&buffer[..read])
                .map_err(|error| error.to_string())?;
        }
        if copied_len != fragment.byte_len {
            return Err(format!(
                "SBDF fragment {} changed while merging: expected {} bytes, copied {copied_len}",
                fragment.sequence, fragment.byte_len
            ));
        }
        writeln!(
            self.manifest,
            "{}\t{}\t{}\t{:016x}\t{:016x}\t{}",
            fragment.sequence,
            fragment.row_count,
            fragment.byte_len,
            checksum,
            fragment.schema_checksum,
            fragment.path.display()
        )
        .map_err(|error| format!("failed to update fragment manifest: {error}"))?;
        fs::remove_file(&fragment.path).map_err(|error| {
            format!(
                "failed to remove merged fragment '{}': {error}",
                fragment.path.display()
            )
        })?;
        Ok(())
    }

    fn finish(mut self) -> Result<(), String> {
        self.writer
            .finish(true)
            .map_err(|error| error.to_string())?;
        self.manifest
            .flush()
            .map_err(|error| format!("failed to flush fragment manifest: {error}"))?;
        let partial = self.workspace.path.join("final.partial");
        fs::rename(&partial, &self.target).map_err(|error| {
            format!(
                "failed to publish SBDF output '{}' from '{}': {error}",
                self.target.display(),
                partial.display()
            )
        })
    }
}

struct SequentialDirectSbdfWriter {
    target: PathBuf,
    workspace: TempWorkspace,
    output: BufWriter<File>,
    end_marker: Vec<u8>,
}

impl SequentialDirectSbdfWriter {
    fn new(target: &Path, config: &SbdfEncodingConfig) -> Result<Self, String> {
        let workspace = TempWorkspace::create(target)?;
        let partial = workspace.path.join("final.partial");
        let mut preamble = config
            .writer(&partial, true)
            .map_err(|error| error.to_string())?;
        preamble.finish(false).map_err(|error| error.to_string())?;

        let end_path = workspace.path.join("end-marker.partial");
        let mut end_writer = config
            .writer(&end_path, false)
            .map_err(|error| error.to_string())?;
        end_writer.finish(true).map_err(|error| error.to_string())?;
        let end_marker = fs::read(&end_path)
            .map_err(|error| format!("failed to read SBDF end marker: {error}"))?;
        fs::remove_file(&end_path)
            .map_err(|error| format!("failed to remove SBDF end marker file: {error}"))?;

        let file = OpenOptions::new()
            .append(true)
            .open(&partial)
            .map_err(|error| format!("failed to reopen sequential SBDF output: {error}"))?;
        Ok(Self {
            target: target.to_path_buf(),
            workspace,
            output: BufWriter::with_capacity(DIRECT_SINK_BUFFER_BYTES, file),
            end_marker,
        })
    }

    fn write_record_batch(
        &mut self,
        batch: &RecordBatch,
        config: &SbdfEncodingConfig,
    ) -> Result<(), String> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.num_columns() != config.columns.len() {
            return Err("record batch columns do not match sequential SBDF schema".to_string());
        }
        rust_sbdf::begin_table_slice_to_sink(&mut self.output, config.columns.len())?;
        for (index, column_name) in config.columns.iter().enumerate() {
            let value_type = config.column_types[index];
            let (buffer, invalids) =
                build_native_column_buffer(column_name, batch.column(index).as_ref())
                    .map_err(|error| error.to_string())?;
            rust_sbdf::encode_column_slice_to_sink(
                &mut self.output,
                value_type.sbdf_type_id(),
                buffer
                    .value_view(value_type)
                    .map_err(|error| error.to_string())?,
                config.encoding_strategy(),
                invalids.as_deref(),
            )?;
        }
        Ok(())
    }

    fn write_native_batch(
        &mut self,
        batch: &native_csv::NativeCsvBatch,
        config: &SbdfEncodingConfig,
    ) -> Result<(), String> {
        if batch.rows() == 0 {
            return Ok(());
        }
        if batch.buffers.len() != config.columns.len() {
            return Err("native CSV batch columns do not match sequential SBDF schema".to_string());
        }
        rust_sbdf::begin_table_slice_to_sink(&mut self.output, config.columns.len())?;
        for (index, column_name) in config.columns.iter().enumerate() {
            let value_type = config.column_types[index];
            rust_sbdf::encode_column_slice_to_sink(
                &mut self.output,
                value_type.sbdf_type_id(),
                batch.buffers[index].value_view(),
                config.encoding_strategy(),
                batch.invalids(index),
            )
            .map_err(|error| {
                format!("failed to encode Rust SBDF column '{column_name}': {error}")
            })?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), String> {
        self.output
            .write_all(&self.end_marker)
            .map_err(|error| format!("failed to write sequential SBDF end marker: {error}"))?;
        self.output
            .flush()
            .map_err(|error| format!("failed to flush sequential SBDF output: {error}"))?;
        drop(self.output);
        let partial = self.workspace.path.join("final.partial");
        fs::rename(&partial, &self.target).map_err(|error| {
            format!(
                "failed to publish SBDF output '{}' from '{}': {error}",
                self.target.display(),
                partial.display()
            )
        })
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
const DIRECT_SINK_BUFFER_BYTES: usize = 1024 * 1024;

fn fnv1a_update(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(FNV_PRIME);
    }
    checksum
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> Result<(), String> {
    while !bytes.is_empty() {
        #[cfg(unix)]
        let written = file.write_at(bytes, offset);
        #[cfg(windows)]
        let written = file.seek_write(bytes, offset);
        let written = written
            .map_err(|error| format!("failed positional SBDF write at offset {offset}: {error}"))?;
        if written == 0 {
            return Err(format!(
                "short positional SBDF write at offset {offset}: wrote zero bytes"
            ));
        }
        offset = offset
            .checked_add(written as u64)
            .ok_or_else(|| "SBDF output offset overflow".to_string())?;
        bytes = &bytes[written..];
    }
    Ok(())
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PositionalFileSink<'a> {
    file: &'a File,
    offset: u64,
    remaining: usize,
    written: usize,
    checksum: u64,
    buffer: Vec<u8>,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
impl<'a> PositionalFileSink<'a> {
    fn new(file: &'a File, offset: u64, planned_len: usize) -> Self {
        Self {
            file,
            offset,
            remaining: planned_len,
            written: 0,
            checksum: FNV_OFFSET_BASIS,
            buffer: Vec::with_capacity(planned_len.min(DIRECT_SINK_BUFFER_BYTES)),
        }
    }

    fn flush_buffer(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        write_all_at(self.file, &self.buffer, self.offset).map_err(std::io::Error::other)?;
        self.offset = self
            .offset
            .checked_add(self.buffer.len() as u64)
            .ok_or_else(|| std::io::Error::other("SBDF positional offset overflow"))?;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<(usize, u64), String> {
        if self.remaining != 0 {
            return Err(format!(
                "planned positional SBDF write is {} bytes short",
                self.remaining
            ));
        }
        self.flush_buffer()
            .map_err(|error| format!("failed to flush planned SBDF sink: {error}"))?;
        Ok((self.written, self.checksum))
    }
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
impl Write for PositionalFileSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "planned positional SBDF write exceeded its assigned range",
            ));
        }
        if self.buffer.len().saturating_add(bytes.len()) > DIRECT_SINK_BUFFER_BYTES {
            self.flush_buffer()?;
        }
        if bytes.len() >= DIRECT_SINK_BUFFER_BYTES {
            write_all_at(self.file, bytes, self.offset).map_err(std::io::Error::other)?;
            self.offset = self
                .offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("SBDF positional offset overflow"))?;
        } else {
            self.buffer.extend_from_slice(bytes);
        }
        self.remaining -= bytes.len();
        self.written = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("SBDF written length overflow"))?;
        self.checksum = fnv1a_update(self.checksum, bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_buffer()
    }
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
#[derive(Debug)]
struct EncodedBufferResult {
    sequence: usize,
    row_count: usize,
    bytes: Vec<u8>,
    schema_checksum: u64,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PendingByteBudget {
    maximum: usize,
    next_sequence: AtomicUsize,
    used: Mutex<usize>,
    available: Condvar,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
impl PendingByteBudget {
    fn new(maximum: usize, start_sequence: usize) -> Self {
        Self {
            maximum,
            next_sequence: AtomicUsize::new(start_sequence),
            used: Mutex::new(0),
            available: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>, sequence: usize, bytes: usize) -> PendingBytePermit {
        let mut used = self.used.lock().unwrap_or_else(|error| error.into_inner());
        while sequence != self.next_sequence.load(Ordering::Acquire)
            && used.saturating_add(bytes) > self.maximum
        {
            used = self
                .available
                .wait(used)
                .unwrap_or_else(|error| error.into_inner());
        }
        *used = used.saturating_add(bytes);
        PendingBytePermit {
            budget: Arc::clone(self),
            bytes,
        }
    }

    fn assigned(&self, sequence: usize) {
        let _ = self.next_sequence.compare_exchange(
            sequence,
            sequence.saturating_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.available.notify_all();
    }
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PendingBytePermit {
    budget: Arc<PendingByteBudget>,
    bytes: usize,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
impl Drop for PendingBytePermit {
    fn drop(&mut self) {
        let mut used = self
            .budget
            .used
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *used = used.saturating_sub(self.bytes);
        self.budget.available.notify_all();
    }
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct DirectSbdfAssembler {
    target: PathBuf,
    workspace: TempWorkspace,
    file: Arc<File>,
    manifest: File,
    end_marker: Vec<u8>,
    next_offset: u64,
    schema_checksum: u64,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
impl DirectSbdfAssembler {
    fn new(target: &Path, config: &SbdfEncodingConfig) -> Result<Self, String> {
        let workspace = TempWorkspace::create(target)?;
        let partial = workspace.path.join("final.partial");
        let mut preamble = config
            .writer(&partial, true)
            .map_err(|error| error.to_string())?;
        preamble.finish(false).map_err(|error| error.to_string())?;

        let end_path = workspace.path.join("end-marker.partial");
        let mut end_writer = config
            .writer(&end_path, false)
            .map_err(|error| error.to_string())?;
        end_writer.finish(true).map_err(|error| error.to_string())?;
        let end_marker = fs::read(&end_path)
            .map_err(|error| format!("failed to read SBDF end marker: {error}"))?;
        fs::remove_file(&end_path)
            .map_err(|error| format!("failed to remove SBDF end marker file: {error}"))?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&partial)
            .map_err(|error| format!("failed to reopen direct SBDF output: {error}"))?;
        let next_offset = file
            .metadata()
            .map_err(|error| format!("failed to stat direct SBDF output: {error}"))?
            .len();
        let manifest_path = workspace.path.join("manifest.tsv");
        let mut manifest = File::create(&manifest_path)
            .map_err(|error| format!("failed to create direct-write manifest: {error}"))?;
        writeln!(
            manifest,
            "sequence\trows\tbytes\tchecksum\tschema_checksum\toffset"
        )
        .map_err(|error| format!("failed to initialize direct-write manifest: {error}"))?;
        Ok(Self {
            target: target.to_path_buf(),
            workspace,
            file: Arc::new(file),
            manifest,
            end_marker,
            next_offset,
            schema_checksum: config.fingerprint(),
        })
    }

    fn assign(&mut self, result: &EncodedBufferResult) -> Result<u64, String> {
        if result.schema_checksum != self.schema_checksum {
            return Err(format!(
                "SBDF buffer {} schema checksum mismatch",
                result.sequence
            ));
        }
        let byte_len = u64::try_from(result.bytes.len())
            .map_err(|_| "encoded SBDF buffer length exceeds u64".to_string())?;
        let offset = self.next_offset;
        self.next_offset = offset
            .checked_add(byte_len)
            .ok_or_else(|| "SBDF output offset overflow".to_string())?;
        writeln!(
            self.manifest,
            "{}\t{}\t{}\t{:016x}\t{:016x}\t{}",
            result.sequence,
            result.row_count,
            byte_len,
            fnv1a_update(FNV_OFFSET_BASIS, &result.bytes),
            result.schema_checksum,
            offset
        )
        .map_err(|error| format!("failed to update direct-write manifest: {error}"))?;
        Ok(offset)
    }

    fn reserve_planned(&mut self, result: &PlannedBatchResult) -> Result<u64, String> {
        if result.schema_checksum != self.schema_checksum {
            return Err(format!(
                "planned SBDF batch {} schema checksum mismatch",
                result.sequence
            ));
        }
        let byte_len = u64::try_from(result.byte_len)
            .map_err(|_| "planned SBDF batch length exceeds u64".to_string())?;
        let offset = self.next_offset;
        self.next_offset = offset
            .checked_add(byte_len)
            .ok_or_else(|| "SBDF output offset overflow".to_string())?;
        Ok(offset)
    }

    fn record_planned_write(&mut self, result: &PlannedWriteResult) -> Result<(), String> {
        writeln!(
            self.manifest,
            "{}\t{}\t{}\t{:016x}\t{:016x}\t{}",
            result.sequence,
            result.row_count,
            result.byte_len,
            result.checksum,
            result.schema_checksum,
            result.offset
        )
        .map_err(|error| format!("failed to update direct-write manifest: {error}"))
    }

    fn finish(self) -> Result<(), String> {
        self.finish_with_sync(true)
    }

    fn finish_with_sync(mut self, sync_output: bool) -> Result<(), String> {
        write_all_at(&self.file, &self.end_marker, self.next_offset)?;
        let final_len = self
            .next_offset
            .checked_add(self.end_marker.len() as u64)
            .ok_or_else(|| "SBDF final length overflow".to_string())?;
        self.file
            .set_len(final_len)
            .map_err(|error| format!("failed to set final SBDF length: {error}"))?;
        if sync_output {
            self.file
                .sync_all()
                .map_err(|error| format!("failed to sync direct SBDF output: {error}"))?;
        }
        self.manifest
            .flush()
            .map_err(|error| format!("failed to flush direct-write manifest: {error}"))?;
        drop(self.file);
        let partial = self.workspace.path.join("final.partial");
        fs::rename(&partial, &self.target).map_err(|error| {
            format!(
                "failed to publish SBDF output '{}' from '{}': {error}",
                self.target.display(),
                partial.display()
            )
        })
    }
}

fn complete_fragment(
    sequence: usize,
    partial: PathBuf,
    row_count: usize,
    schema_checksum: u64,
) -> Result<FragmentResult, String> {
    let ready = partial.with_extension("ready");
    fs::rename(&partial, &ready).map_err(|error| {
        format!(
            "failed to publish fragment {} from '{}': {error}",
            sequence,
            partial.display()
        )
    })?;
    let byte_len = ready
        .metadata()
        .map_err(|error| {
            format!(
                "failed to stat published fragment {} at '{}': {error}",
                sequence,
                ready.display()
            )
        })?
        .len();
    Ok(FragmentResult {
        sequence,
        path: ready,
        row_count,
        byte_len,
        schema_checksum,
    })
}

struct ParquetRowGroupTask {
    source_path: PathBuf,
    metadata: ArrowReaderMetadata,
    row_group_index: usize,
    expected_rows: usize,
    batch_size: usize,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PlannedBatchResult {
    sequence: usize,
    row_count: usize,
    batches: Vec<RecordBatch>,
    plans: Vec<rust_sbdf::EncodedLayoutPlan>,
    byte_len: usize,
    resident_bytes: usize,
    schema_checksum: u64,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PlannedWriteResult {
    sequence: usize,
    row_count: usize,
    byte_len: usize,
    checksum: u64,
    schema_checksum: u64,
    offset: u64,
}

#[cfg_attr(feature = "planned-offset-prototype", allow(dead_code))]
fn encode_parquet_row_group_fragment(
    sequence: usize,
    task: ParquetRowGroupTask,
    partial: PathBuf,
    config: &SbdfEncodingConfig,
) -> Result<FragmentResult, String> {
    let result = (|| {
        let input = File::open(&task.source_path).map_err(|error| {
            format!(
                "failed to open Parquet file '{}' for row-group {}: {error}",
                task.source_path.display(),
                task.row_group_index
            )
        })?;
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(input, task.metadata)
            .with_row_groups(vec![task.row_group_index])
            .with_batch_size(task.batch_size)
            .build()
            .map_err(|error| {
                format!(
                    "failed to build Parquet row-group reader for '{}' row-group {}: {error}",
                    task.source_path.display(),
                    task.row_group_index
                )
            })?;
        let mut writer = config
            .writer(&partial, false)
            .map_err(|error| error.to_string())?;
        let mut row_count = 0usize;
        for batch in reader {
            let batch = batch.map_err(|error| {
                format!(
                    "failed to decode Parquet file '{}' row-group {}: {error}",
                    task.source_path.display(),
                    task.row_group_index
                )
            })?;
            row_count = row_count.saturating_add(batch.num_rows());
            write_record_batch_to_sbdf(&mut writer, &batch).map_err(|error| error.to_string())?;
        }
        if row_count != task.expected_rows {
            return Err(format!(
                "Parquet row-group {} in '{}' produced {row_count} rows; expected {}",
                task.row_group_index,
                task.source_path.display(),
                task.expected_rows
            ));
        }
        writer.finish(false).map_err(|error| error.to_string())?;
        complete_fragment(sequence, partial.clone(), row_count, config.fingerprint())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&partial);
    }
    result
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn encode_parquet_row_group_buffer(
    sequence: usize,
    task: ParquetRowGroupTask,
    config: &SbdfEncodingConfig,
) -> Result<EncodedBufferResult, String> {
    let input = File::open(&task.source_path).map_err(|error| {
        format!(
            "failed to open Parquet file '{}' for row-group {}: {error}",
            task.source_path.display(),
            task.row_group_index
        )
    })?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(input, task.metadata)
        .with_row_groups(vec![task.row_group_index])
        .with_batch_size(task.batch_size)
        .build()
        .map_err(|error| {
            format!(
                "failed to build Parquet row-group reader for '{}' row-group {}: {error}",
                task.source_path.display(),
                task.row_group_index
            )
        })?;
    let mut bytes = Vec::new();
    let mut row_count = 0usize;
    for batch in reader {
        let batch = batch.map_err(|error| {
            format!(
                "failed to decode Parquet file '{}' row-group {}: {error}",
                task.source_path.display(),
                task.row_group_index
            )
        })?;
        row_count = row_count.saturating_add(batch.num_rows());
        encode_record_batch_into(
            &mut bytes,
            &config.columns,
            &config.column_types,
            config.encoding_strategy(),
            &batch,
        )
        .map_err(|error| error.to_string())?;
    }
    if row_count != task.expected_rows {
        return Err(format!(
            "Parquet row-group {} in '{}' produced {row_count} rows; expected {}",
            task.row_group_index,
            task.source_path.display(),
            task.expected_rows
        ));
    }
    Ok(EncodedBufferResult {
        sequence,
        row_count,
        bytes,
        schema_checksum: config.fingerprint(),
    })
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn plan_parquet_row_group_batches(
    sequence: usize,
    task: ParquetRowGroupTask,
    config: &SbdfEncodingConfig,
) -> Result<PlannedBatchResult, String> {
    let input = File::open(&task.source_path).map_err(|error| {
        format!(
            "failed to open Parquet file '{}' for planned row-group {}: {error}",
            task.source_path.display(),
            task.row_group_index
        )
    })?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(input, task.metadata)
        .with_row_groups(vec![task.row_group_index])
        .with_batch_size(task.batch_size)
        .build()
        .map_err(|error| {
            format!(
                "failed to build planned Parquet row-group reader for '{}' row-group {}: {error}",
                task.source_path.display(),
                task.row_group_index
            )
        })?;
    let mut batches = Vec::new();
    let mut plans = Vec::new();
    let mut row_count = 0usize;
    let mut byte_len = 0usize;
    let mut resident_bytes = 0usize;
    for batch in reader {
        let batch = batch.map_err(|error| {
            format!(
                "failed to decode planned Parquet file '{}' row-group {}: {error}",
                task.source_path.display(),
                task.row_group_index
            )
        })?;
        let plan = plan_record_batch(&batch, config)?;
        row_count = row_count.saturating_add(batch.num_rows());
        byte_len = byte_len
            .checked_add(plan.byte_len())
            .ok_or_else(|| "planned Parquet row-group byte length overflow".to_string())?;
        resident_bytes = resident_bytes
            .saturating_add(
                batch
                    .columns()
                    .iter()
                    .map(|column| column.get_array_memory_size())
                    .sum::<usize>(),
            )
            .saturating_add(plan.resident_bytes());
        batches.push(batch);
        plans.push(plan);
    }
    if row_count != task.expected_rows {
        return Err(format!(
            "Parquet row-group {} in '{}' produced {row_count} rows; expected {}",
            task.row_group_index,
            task.source_path.display(),
            task.expected_rows
        ));
    }
    Ok(PlannedBatchResult {
        sequence,
        row_count,
        batches,
        plans,
        byte_len,
        resident_bytes,
        schema_checksum: config.fingerprint(),
    })
}

struct SequencedTask<T> {
    sequence: usize,
    payload: T,
}

#[allow(dead_code)]
enum ParallelEvent {
    Ready(FragmentResult),
    Error(String),
    ProducerDone(usize),
}

#[allow(dead_code)]
fn parallel_encode_tasks<I, T, F>(
    tasks: I,
    workers: usize,
    start_sequence: usize,
    assembler: &mut SbdfAssembler,
    encode: F,
) -> Result<usize, String>
where
    I: Iterator<Item = Result<SequencedTask<T>, String>> + Send + 'static,
    T: Send + 'static,
    F: Fn(SequencedTask<T>, PathBuf) -> Result<FragmentResult, String> + Send + Sync + 'static,
{
    let workers = workers.max(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let encode = Arc::new(encode);
    let (event_sender, event_receiver) = channel::<ParallelEvent>();
    let mut task_senders = Vec::with_capacity(workers);
    let mut worker_handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let (task_sender, task_receiver) = sync_channel::<SequencedTask<T>>(0);
        task_senders.push(task_sender);
        let event_sender = event_sender.clone();
        let cancelled = Arc::clone(&cancelled);
        let encode = Arc::clone(&encode);
        let workspace = assembler.workspace.path.clone();
        let handle = thread::Builder::new()
            .name(format!("sbdf-encoder-{worker_index}"))
            .spawn(move || {
                while let Ok(task) = task_receiver.recv() {
                    if cancelled.load(Ordering::Acquire) {
                        break;
                    }
                    let partial = workspace.join(format!("slice-{:012}.partial", task.sequence));
                    match encode(task, partial) {
                        Ok(fragment) => {
                            if event_sender.send(ParallelEvent::Ready(fragment)).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Release);
                            let _ = event_sender.send(ParallelEvent::Error(error));
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("failed to start SBDF encoder worker: {error}"))?;
        worker_handles.push(handle);
    }

    let producer_events = event_sender.clone();
    let producer_cancelled = Arc::clone(&cancelled);
    let producer = thread::Builder::new()
        .name("sbdf-task-producer".to_string())
        .spawn(move || {
            let mut next_sequence = start_sequence;
            for (dispatch_index, item) in tasks.enumerate() {
                if producer_cancelled.load(Ordering::Acquire) {
                    break;
                }
                let task = match item {
                    Ok(task) => task,
                    Err(error) => {
                        producer_cancelled.store(true, Ordering::Release);
                        let _ = producer_events.send(ParallelEvent::Error(error));
                        return;
                    }
                };
                if task.sequence != next_sequence {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(ParallelEvent::Error(format!(
                        "non-contiguous task sequence: expected {next_sequence}, found {}",
                        task.sequence
                    )));
                    return;
                }
                next_sequence += 1;
                let sender = &task_senders[dispatch_index % workers];
                if sender.send(task).is_err() {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(ParallelEvent::Error(
                        "SBDF encoder worker stopped before accepting a task".to_string(),
                    ));
                    return;
                }
            }
            drop(task_senders);
            let _ = producer_events.send(ParallelEvent::ProducerDone(next_sequence));
        })
        .map_err(|error| format!("failed to start SBDF task producer: {error}"))?;
    drop(event_sender);

    let mut next_write = start_sequence;
    let mut producer_done = None;
    let mut pending = BTreeMap::new();
    let mut first_error = None;

    while first_error.is_none() {
        if producer_done.is_some_and(|end| next_write == end) {
            break;
        }
        match event_receiver.recv() {
            Ok(ParallelEvent::Ready(fragment)) => {
                if pending.insert(fragment.sequence, fragment).is_some() {
                    first_error = Some("duplicate SBDF fragment sequence".to_string());
                    cancelled.store(true, Ordering::Release);
                    continue;
                }
                while let Some(fragment) = pending.remove(&next_write) {
                    if let Err(error) = assembler.append_fragment(&fragment) {
                        first_error = Some(error);
                        cancelled.store(true, Ordering::Release);
                        break;
                    }
                    next_write += 1;
                }
            }
            Ok(ParallelEvent::Error(error)) => {
                first_error = Some(error);
                cancelled.store(true, Ordering::Release);
            }
            Ok(ParallelEvent::ProducerDone(end)) => producer_done = Some(end),
            Err(_) => {
                first_error = Some("parallel SBDF pipeline ended unexpectedly".to_string());
                cancelled.store(true, Ordering::Release);
            }
        }
    }

    cancelled.store(first_error.is_some(), Ordering::Release);
    if producer.join().is_err() && first_error.is_none() {
        first_error = Some("SBDF task producer panicked".to_string());
    }
    for handle in worker_handles {
        if handle.join().is_err() && first_error.is_none() {
            first_error = Some("SBDF encoder worker panicked".to_string());
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let expected_end = producer_done.ok_or_else(|| "task producer did not finish".to_string())?;
    if next_write != expected_end || !pending.is_empty() {
        return Err(format!(
            "fragment sequence incomplete: wrote through {next_write}, expected {expected_end}"
        ));
    }
    Ok(expected_end)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
enum DirectAssignment {
    Write(u64, EncodedBufferResult),
    Cancel,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct DirectReady {
    result: EncodedBufferResult,
    assignment: SyncSender<DirectAssignment>,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
enum DirectParallelEvent {
    Ready(DirectReady),
    Written(usize),
    Error(String),
    ProducerDone(usize),
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn parallel_encode_buffers<I, T, F>(
    tasks: I,
    workers: usize,
    start_sequence: usize,
    assembler: &mut DirectSbdfAssembler,
    encode: F,
) -> Result<usize, String>
where
    I: Iterator<Item = Result<SequencedTask<T>, String>> + Send + 'static,
    T: Send + 'static,
    F: Fn(SequencedTask<T>) -> Result<EncodedBufferResult, String> + Send + Sync + 'static,
{
    let workers = workers.max(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let budget = Arc::new(PendingByteBudget::new(
        MAX_PENDING_ENCODED_BYTES,
        start_sequence,
    ));
    let encode = Arc::new(encode);
    let output = Arc::clone(&assembler.file);
    let (event_sender, event_receiver) = channel::<DirectParallelEvent>();
    let mut task_senders = Vec::with_capacity(workers);
    let mut worker_handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let (task_sender, task_receiver) = sync_channel::<SequencedTask<T>>(0);
        task_senders.push(task_sender);
        let event_sender = event_sender.clone();
        let cancelled = Arc::clone(&cancelled);
        let budget = Arc::clone(&budget);
        let encode = Arc::clone(&encode);
        let output = Arc::clone(&output);
        let handle = thread::Builder::new()
            .name(format!("sbdf-direct-encoder-{worker_index}"))
            .spawn(move || {
                while let Ok(task) = task_receiver.recv() {
                    if cancelled.load(Ordering::Acquire) {
                        break;
                    }
                    let result = match encode(task) {
                        Ok(result) => result,
                        Err(error) => {
                            cancelled.store(true, Ordering::Release);
                            let _ = event_sender.send(DirectParallelEvent::Error(error));
                            break;
                        }
                    };
                    let sequence = result.sequence;
                    let permit = budget.acquire(sequence, result.bytes.len());
                    let (assignment_sender, assignment_receiver) = sync_channel(0);
                    if event_sender
                        .send(DirectParallelEvent::Ready(DirectReady {
                            result,
                            assignment: assignment_sender,
                        }))
                        .is_err()
                    {
                        break;
                    }
                    match assignment_receiver.recv() {
                        Ok(DirectAssignment::Write(offset, result)) => {
                            if let Err(error) = write_all_at(&output, &result.bytes, offset) {
                                cancelled.store(true, Ordering::Release);
                                let _ = event_sender.send(DirectParallelEvent::Error(format!(
                                    "failed to write SBDF buffer {}: {error}",
                                    result.sequence
                                )));
                                break;
                            }
                            drop(permit);
                            if event_sender
                                .send(DirectParallelEvent::Written(result.sequence))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(DirectAssignment::Cancel) | Err(_) => break,
                    }
                }
            })
            .map_err(|error| format!("failed to start direct SBDF encoder worker: {error}"))?;
        worker_handles.push(handle);
    }

    let producer_events = event_sender.clone();
    let producer_cancelled = Arc::clone(&cancelled);
    let producer = thread::Builder::new()
        .name("sbdf-direct-task-producer".to_string())
        .spawn(move || {
            let mut next_sequence = start_sequence;
            for (dispatch_index, item) in tasks.enumerate() {
                if producer_cancelled.load(Ordering::Acquire) {
                    break;
                }
                let task = match item {
                    Ok(task) => task,
                    Err(error) => {
                        producer_cancelled.store(true, Ordering::Release);
                        let _ = producer_events.send(DirectParallelEvent::Error(error));
                        return;
                    }
                };
                if task.sequence != next_sequence {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(DirectParallelEvent::Error(format!(
                        "non-contiguous task sequence: expected {next_sequence}, found {}",
                        task.sequence
                    )));
                    return;
                }
                next_sequence += 1;
                if task_senders[dispatch_index % workers].send(task).is_err() {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(DirectParallelEvent::Error(
                        "direct SBDF encoder worker stopped before accepting a task".to_string(),
                    ));
                    return;
                }
            }
            drop(task_senders);
            let _ = producer_events.send(DirectParallelEvent::ProducerDone(next_sequence));
        })
        .map_err(|error| format!("failed to start direct SBDF task producer: {error}"))?;
    drop(event_sender);

    let mut next_assign = start_sequence;
    let mut producer_done = None;
    let mut pending = BTreeMap::<usize, DirectReady>::new();
    let mut written = std::collections::BTreeSet::new();
    let mut first_error = None;

    loop {
        if first_error.is_none()
            && producer_done
                .is_some_and(|end| next_assign == end && written.len() == end - start_sequence)
        {
            break;
        }
        let event = match event_receiver.recv() {
            Ok(event) => event,
            Err(_) => break,
        };
        match event {
            DirectParallelEvent::Ready(ready) if first_error.is_some() => {
                let _ = ready.assignment.send(DirectAssignment::Cancel);
            }
            DirectParallelEvent::Ready(ready) => {
                if pending.insert(ready.result.sequence, ready).is_some() {
                    first_error = Some("duplicate direct SBDF buffer sequence".to_string());
                    cancelled.store(true, Ordering::Release);
                    continue;
                }
                while let Some(ready) = pending.remove(&next_assign) {
                    let offset = match assembler.assign(&ready.result) {
                        Ok(offset) => offset,
                        Err(error) => {
                            first_error = Some(error);
                            cancelled.store(true, Ordering::Release);
                            let _ = ready.assignment.send(DirectAssignment::Cancel);
                            break;
                        }
                    };
                    budget.assigned(next_assign);
                    if ready
                        .assignment
                        .send(DirectAssignment::Write(offset, ready.result))
                        .is_err()
                    {
                        first_error = Some(
                            "direct SBDF worker stopped before receiving its offset".to_string(),
                        );
                        cancelled.store(true, Ordering::Release);
                        break;
                    }
                    next_assign += 1;
                }
            }
            DirectParallelEvent::Written(sequence) => {
                if !written.insert(sequence) {
                    first_error = Some("duplicate direct SBDF write completion".to_string());
                    cancelled.store(true, Ordering::Release);
                }
            }
            DirectParallelEvent::Error(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                cancelled.store(true, Ordering::Release);
                for (_, ready) in std::mem::take(&mut pending) {
                    let _ = ready.assignment.send(DirectAssignment::Cancel);
                }
            }
            DirectParallelEvent::ProducerDone(end) => producer_done = Some(end),
        }
    }

    cancelled.store(first_error.is_some(), Ordering::Release);
    if first_error.is_some() {
        while let Ok(event) = event_receiver.recv() {
            if let DirectParallelEvent::Ready(ready) = event {
                let _ = ready.assignment.send(DirectAssignment::Cancel);
            }
        }
    }
    if producer.join().is_err() && first_error.is_none() {
        first_error = Some("direct SBDF task producer panicked".to_string());
    }
    for handle in worker_handles {
        if handle.join().is_err() && first_error.is_none() {
            first_error = Some("direct SBDF encoder worker panicked".to_string());
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let expected_end = producer_done.ok_or_else(|| "task producer did not finish".to_string())?;
    if next_assign != expected_end || written.len() != expected_end - start_sequence {
        return Err(format!(
            "direct buffer sequence incomplete: assigned through {next_assign}, expected {expected_end}"
        ));
    }
    Ok(expected_end)
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
enum PlannedAssignment {
    Write(u64, PlannedBatchResult),
    Cancel,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
struct PlannedReady {
    result: PlannedBatchResult,
    assignment: SyncSender<PlannedAssignment>,
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
enum PlannedParallelEvent {
    Ready(PlannedReady),
    Written(PlannedWriteResult),
    Error(String),
    ProducerDone(usize),
}

#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn parallel_plan_record_batches<I, T, F>(
    tasks: I,
    workers: usize,
    start_sequence: usize,
    assembler: &mut DirectSbdfAssembler,
    config: SbdfEncodingConfig,
    plan: F,
) -> Result<usize, String>
where
    I: Iterator<Item = Result<SequencedTask<T>, String>> + Send + 'static,
    T: Send + 'static,
    F: Fn(SequencedTask<T>) -> Result<PlannedBatchResult, String> + Send + Sync + 'static,
{
    let workers = workers.max(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    let budget = Arc::new(PendingByteBudget::new(
        MAX_PENDING_ENCODED_BYTES,
        start_sequence,
    ));
    let plan = Arc::new(plan);
    let config = Arc::new(config);
    let output = Arc::clone(&assembler.file);
    let (event_sender, event_receiver) = channel::<PlannedParallelEvent>();
    let mut task_senders = Vec::with_capacity(workers);
    let mut worker_handles = Vec::with_capacity(workers);

    for worker_index in 0..workers {
        let (task_sender, task_receiver) = sync_channel::<SequencedTask<T>>(0);
        task_senders.push(task_sender);
        let event_sender = event_sender.clone();
        let cancelled = Arc::clone(&cancelled);
        let budget = Arc::clone(&budget);
        let plan = Arc::clone(&plan);
        let config = Arc::clone(&config);
        let output = Arc::clone(&output);
        let handle = thread::Builder::new()
            .name(format!("sbdf-planned-encoder-{worker_index}"))
            .spawn(move || {
                while let Ok(task) = task_receiver.recv() {
                    if cancelled.load(Ordering::Acquire) {
                        break;
                    }
                    let result = match plan(task) {
                        Ok(result) => result,
                        Err(error) => {
                            cancelled.store(true, Ordering::Release);
                            let _ = event_sender.send(PlannedParallelEvent::Error(error));
                            break;
                        }
                    };
                    let sequence = result.sequence;
                    let permit = budget.acquire(sequence, result.resident_bytes);
                    let (assignment_sender, assignment_receiver) = sync_channel(0);
                    if event_sender
                        .send(PlannedParallelEvent::Ready(PlannedReady {
                            result,
                            assignment: assignment_sender,
                        }))
                        .is_err()
                    {
                        break;
                    }
                    match assignment_receiver.recv() {
                        Ok(PlannedAssignment::Write(offset, result)) => {
                            let mut sink =
                                PositionalFileSink::new(&output, offset, result.byte_len);
                            let encode_result = result
                                .batches
                                .iter()
                                .zip(result.plans.iter())
                                .try_for_each(|(batch, layout)| {
                                    encode_planned_record_batch(&mut sink, batch, &config, layout)
                                })
                                .and_then(|()| sink.finish());
                            let (written, checksum) = match encode_result {
                                Ok(completion) => completion,
                                Err(error) => {
                                    cancelled.store(true, Ordering::Release);
                                    let _ =
                                        event_sender.send(PlannedParallelEvent::Error(format!(
                                            "failed to write planned SBDF batch {}: {error}",
                                            result.sequence
                                        )));
                                    break;
                                }
                            };
                            drop(permit);
                            if event_sender
                                .send(PlannedParallelEvent::Written(PlannedWriteResult {
                                    sequence: result.sequence,
                                    row_count: result.row_count,
                                    byte_len: written,
                                    checksum,
                                    schema_checksum: result.schema_checksum,
                                    offset,
                                }))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(PlannedAssignment::Cancel) | Err(_) => break,
                    }
                }
            })
            .map_err(|error| format!("failed to start planned SBDF encoder worker: {error}"))?;
        worker_handles.push(handle);
    }

    let producer_events = event_sender.clone();
    let producer_cancelled = Arc::clone(&cancelled);
    let producer = thread::Builder::new()
        .name("sbdf-planned-task-producer".to_string())
        .spawn(move || {
            let mut next_sequence = start_sequence;
            for (dispatch_index, item) in tasks.enumerate() {
                if producer_cancelled.load(Ordering::Acquire) {
                    break;
                }
                let task = match item {
                    Ok(task) => task,
                    Err(error) => {
                        producer_cancelled.store(true, Ordering::Release);
                        let _ = producer_events.send(PlannedParallelEvent::Error(error));
                        return;
                    }
                };
                if task.sequence != next_sequence {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(PlannedParallelEvent::Error(format!(
                        "non-contiguous planned task sequence: expected {next_sequence}, found {}",
                        task.sequence
                    )));
                    return;
                }
                next_sequence += 1;
                if task_senders[dispatch_index % workers].send(task).is_err() {
                    producer_cancelled.store(true, Ordering::Release);
                    let _ = producer_events.send(PlannedParallelEvent::Error(
                        "planned SBDF encoder worker stopped before accepting a task".to_string(),
                    ));
                    return;
                }
            }
            drop(task_senders);
            let _ = producer_events.send(PlannedParallelEvent::ProducerDone(next_sequence));
        })
        .map_err(|error| format!("failed to start planned SBDF task producer: {error}"))?;
    drop(event_sender);

    let mut next_assign = start_sequence;
    let mut producer_done = None;
    let mut pending = BTreeMap::<usize, PlannedReady>::new();
    let mut written = std::collections::BTreeSet::new();
    let mut first_error = None;
    loop {
        if first_error.is_none()
            && producer_done
                .is_some_and(|end| next_assign == end && written.len() == end - start_sequence)
        {
            break;
        }
        let event = match event_receiver.recv() {
            Ok(event) => event,
            Err(_) => break,
        };
        match event {
            PlannedParallelEvent::Ready(ready) if first_error.is_some() => {
                let _ = ready.assignment.send(PlannedAssignment::Cancel);
            }
            PlannedParallelEvent::Ready(ready) => {
                if pending.insert(ready.result.sequence, ready).is_some() {
                    first_error = Some("duplicate planned SBDF sequence".to_string());
                    cancelled.store(true, Ordering::Release);
                    continue;
                }
                while let Some(ready) = pending.remove(&next_assign) {
                    let offset = match assembler.reserve_planned(&ready.result) {
                        Ok(offset) => offset,
                        Err(error) => {
                            first_error = Some(error);
                            cancelled.store(true, Ordering::Release);
                            let _ = ready.assignment.send(PlannedAssignment::Cancel);
                            break;
                        }
                    };
                    budget.assigned(next_assign);
                    if ready
                        .assignment
                        .send(PlannedAssignment::Write(offset, ready.result))
                        .is_err()
                    {
                        first_error = Some(
                            "planned SBDF worker stopped before receiving its offset".to_string(),
                        );
                        cancelled.store(true, Ordering::Release);
                        break;
                    }
                    next_assign += 1;
                }
            }
            PlannedParallelEvent::Written(result) => {
                if !written.insert(result.sequence) {
                    first_error = Some("duplicate planned SBDF write completion".to_string());
                    cancelled.store(true, Ordering::Release);
                } else if let Err(error) = assembler.record_planned_write(&result) {
                    first_error = Some(error);
                    cancelled.store(true, Ordering::Release);
                }
            }
            PlannedParallelEvent::Error(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                cancelled.store(true, Ordering::Release);
                for (_, ready) in std::mem::take(&mut pending) {
                    let _ = ready.assignment.send(PlannedAssignment::Cancel);
                }
            }
            PlannedParallelEvent::ProducerDone(end) => producer_done = Some(end),
        }
    }

    cancelled.store(first_error.is_some(), Ordering::Release);
    if first_error.is_some() {
        while let Ok(event) = event_receiver.recv() {
            if let PlannedParallelEvent::Ready(ready) = event {
                let _ = ready.assignment.send(PlannedAssignment::Cancel);
            }
        }
    }
    if producer.join().is_err() && first_error.is_none() {
        first_error = Some("planned SBDF task producer panicked".to_string());
    }
    for handle in worker_handles {
        if handle.join().is_err() && first_error.is_none() {
            first_error = Some("planned SBDF encoder worker panicked".to_string());
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let expected_end = producer_done.ok_or_else(|| "task producer did not finish".to_string())?;
    if next_assign != expected_end || written.len() != expected_end - start_sequence {
        return Err(format!(
            "planned batch sequence incomplete: assigned through {next_assign}, expected {expected_end}"
        ));
    }
    Ok(expected_end)
}

#[derive(Clone)]
struct SharedMmap(Arc<Mmap>);

impl AsRef<[u8]> for SharedMmap {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

fn csv_arrow_type(value_type: ValueType) -> PyResult<DataType> {
    match value_type {
        ValueType::Bool => Ok(DataType::Boolean),
        ValueType::Int => Ok(DataType::Int32),
        ValueType::Long => Ok(DataType::Int64),
        ValueType::Float => Ok(DataType::Float32),
        ValueType::Double => Ok(DataType::Float64),
        ValueType::DateTime => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        ValueType::Date => Ok(DataType::Date32),
        ValueType::String => Ok(DataType::Utf8),
        ValueType::Time | ValueType::TimeSpan | ValueType::Binary => {
            Err(PyValueError::new_err(format!(
                "CSV parsing does not support explicit SBDF type '{}'",
                value_type.spotfire_name()
            )))
        }
    }
}

fn csv_schema_with_rules(
    inferred_schema: &Schema,
    provided: Option<&HashMap<String, String>>,
) -> PyResult<Schema> {
    if let Some(provided) = provided {
        for column in provided.keys() {
            if inferred_schema.field_with_name(column).is_err() {
                return Err(PyValueError::new_err(format!(
                    "column_types contains unknown CSV column '{column}'"
                )));
            }
        }
    }

    let fields = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            let explicit = provided
                .and_then(|types| types.get(field.name()))
                .map(|name| ValueType::from_name(name))
                .transpose()?;
            let ruled = explicit.or_else(|| type_rules::name_rule(field.name()));
            let data_type = match ruled {
                Some(value_type) => csv_arrow_type(value_type)?,
                None if field.data_type() == &DataType::Null => {
                    return Err(PyValueError::new_err(format!(
                        "CSV column '{}' contains only null values in the inference sample; provide column_types",
                        field.name()
                    )))
                }
                None => field.data_type().clone(),
            };
            Ok(Field::new(field.name(), data_type, true))
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

#[allow(clippy::too_many_arguments)]
fn decode_native_csv_to_writer<R: Read>(
    input: R,
    source_name: &str,
    writer: &mut StreamingSbdfWriter,
    columns: &[String],
    column_types: &[ValueType],
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(false)
        .from_reader(input);
    let mut record = csv::ByteRecord::new();
    let mut batch = native_csv::NativeCsvBatch::new(column_types.to_vec(), batch_size);
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;

    loop {
        let has_record = reader
            .read_byte_record(&mut record)
            .map_err(|error| format!("failed to parse CSV record from '{source_name}': {error}"))?;
        if !has_record {
            break;
        }
        let row_number = record
            .position()
            .map(|position| position.line())
            .unwrap_or((total_rows + usize::from(has_header) + 1) as u64);
        batch.push_record(&record, row_number, columns)?;
        if batch.reached_limit(batch_size) {
            total_rows = total_rows.saturating_add(batch.rows());
            total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
            write_native_csv_batch_to_sbdf(writer, &batch).map_err(|error| error.to_string())?;
            batch.clear_retain_capacity();
        }
    }

    if batch.rows() > 0 {
        total_rows = total_rows.saturating_add(batch.rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
        write_native_csv_batch_to_sbdf(writer, &batch).map_err(|error| error.to_string())?;
    }
    Ok((total_rows, total_decoded_bytes))
}

#[allow(clippy::too_many_arguments)]
fn decode_native_csv_to_direct_writer<R: Read>(
    input: R,
    source_name: &str,
    writer: &mut SequentialDirectSbdfWriter,
    config: &SbdfEncodingConfig,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(false)
        .from_reader(input);
    let mut record = csv::ByteRecord::new();
    let mut batch =
        native_csv::NativeCsvBatch::new(config.column_types.as_ref().clone(), batch_size);
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;

    loop {
        let has_record = reader
            .read_byte_record(&mut record)
            .map_err(|error| format!("failed to parse CSV record from '{source_name}': {error}"))?;
        if !has_record {
            break;
        }
        let row_number = record
            .position()
            .map(|position| position.line())
            .unwrap_or((total_rows + usize::from(has_header) + 1) as u64);
        batch.push_record(&record, row_number, &config.columns)?;
        if batch.reached_limit(batch_size) {
            total_rows = total_rows.saturating_add(batch.rows());
            total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
            writer.write_native_batch(&batch, config)?;
            batch.clear_retain_capacity();
        }
    }
    if batch.rows() > 0 {
        total_rows = total_rows.saturating_add(batch.rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
        writer.write_native_batch(&batch, config)?;
    }
    Ok((total_rows, total_decoded_bytes))
}

#[allow(clippy::too_many_arguments)]
#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn decode_native_csv_to_buffer<R: Read>(
    input: R,
    source_name: &str,
    output: &mut Vec<u8>,
    config: &SbdfEncodingConfig,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .flexible(false)
        .from_reader(input);
    let mut record = csv::ByteRecord::new();
    let mut batch =
        native_csv::NativeCsvBatch::new(config.column_types.as_ref().clone(), batch_size);
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;

    loop {
        let has_record = reader
            .read_byte_record(&mut record)
            .map_err(|error| format!("failed to parse CSV record from '{source_name}': {error}"))?;
        if !has_record {
            break;
        }
        let row_number = record
            .position()
            .map(|position| position.line())
            .unwrap_or((total_rows + usize::from(has_header) + 1) as u64);
        batch.push_record(&record, row_number, &config.columns)?;
        if batch.reached_limit(batch_size) {
            total_rows = total_rows.saturating_add(batch.rows());
            total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
            encode_native_csv_batch_into(
                output,
                &config.columns,
                &config.column_types,
                config.encoding_strategy(),
                &batch,
            )
            .map_err(|error| error.to_string())?;
            batch.clear_retain_capacity();
        }
    }
    if batch.rows() > 0 {
        total_rows = total_rows.saturating_add(batch.rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(batch.decoded_bytes());
        encode_native_csv_batch_into(
            output,
            &config.columns,
            &config.column_types,
            config.encoding_strategy(),
            &batch,
        )
        .map_err(|error| error.to_string())?;
    }
    Ok((total_rows, total_decoded_bytes))
}

#[allow(clippy::too_many_arguments)]
fn decode_arrow_csv_to_writer<R: Read>(
    input: R,
    source_name: &str,
    writer: &mut StreamingSbdfWriter,
    schema: Arc<Schema>,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let format = CsvFormat::default()
        .with_header(has_header)
        .with_delimiter(delimiter);
    let reader = CsvReaderBuilder::new(schema)
        .with_format(format)
        .with_batch_size(batch_size)
        .build(input)
        .map_err(|error| format!("failed to create CSV reader for '{source_name}': {error}"))?;
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;
    for batch in reader {
        let batch = batch.map_err(|error| {
            format!("failed to decode CSV record batch from '{source_name}': {error}")
        })?;
        total_rows = total_rows.saturating_add(batch.num_rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(
            batch
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>(),
        );
        write_record_batch_to_sbdf(writer, &batch).map_err(|error| error.to_string())?;
    }
    Ok((total_rows, total_decoded_bytes))
}

fn parse_utf8_csv_primitive<T>(column: &dyn Array, column_name: &str) -> Result<ArrayRef, String>
where
    T: ArrowPrimitiveType + Parser,
{
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("CSV column '{column_name}' is not a UTF-8 decode buffer"))?;
    let mut builder = PrimitiveBuilder::<T>::with_capacity(strings.len());
    for value in strings.iter() {
        match value {
            None => builder.append_null(),
            Some(value) => builder.append_value(T::parse(value).ok_or_else(|| {
                format!(
                    "failed to parse CSV column '{column_name}' value '{value}' as {}",
                    T::DATA_TYPE
                )
            })?),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[allow(clippy::too_many_arguments)]
fn decode_arrow_csv_to_direct_writer<R: Read + Send + 'static>(
    input: R,
    source_name: &str,
    writer: &mut SequentialDirectSbdfWriter,
    config: &SbdfEncodingConfig,
    schema: Arc<Schema>,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let format = CsvFormat::default()
        .with_header(has_header)
        .with_delimiter(delimiter);
    let decode_fields = schema
        .fields()
        .iter()
        .zip(config.column_types.iter())
        .map(|(field, value_type)| {
            if matches!(value_type, ValueType::Float | ValueType::Double) {
                Arc::new(
                    Field::new(field.name(), DataType::Utf8, field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    let decode_schema = Arc::new(Schema::new_with_metadata(
        decode_fields,
        schema.metadata().clone(),
    ));
    let reader = CsvReaderBuilder::new(decode_schema)
        .with_format(format)
        .with_batch_size(batch_size)
        .build(input)
        .map_err(|error| format!("failed to create CSV reader for '{source_name}': {error}"))?;
    let source_name = source_name.to_string();
    let producer_source_name = source_name.clone();
    let batches = reader.map(move |batch| {
        batch.map_err(|error| {
            format!("failed to decode CSV record batch from '{producer_source_name}': {error}")
        })
    });
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;
    consume_with_one_batch_prefetch(batches, |batch| {
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                if column.data_type() != &DataType::Utf8 {
                    return Ok(Arc::clone(column));
                }
                let column_name = &config.columns[index];
                match config.column_types[index] {
                    ValueType::Float => {
                        parse_utf8_csv_primitive::<Float32Type>(column.as_ref(), column_name)
                    }
                    ValueType::Double => {
                        parse_utf8_csv_primitive::<Float64Type>(column.as_ref(), column_name)
                    }
                    _ => Ok(Arc::clone(column)),
                }
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(|error| {
                format!("failed to decode CSV record batch from '{source_name}': {error}")
            })?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|error| format!("failed to restore typed CSV record batch: {error}"))?;
        total_rows = total_rows.saturating_add(batch.num_rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(
            batch
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>(),
        );
        writer.write_record_batch(&batch, config)
    })?;
    Ok((total_rows, total_decoded_bytes))
}

#[allow(clippy::too_many_arguments)]
#[cfg(any(test, feature = "planned-offset-prototype"))]
#[allow(dead_code)]
fn decode_arrow_csv_to_buffer<R: Read>(
    input: R,
    source_name: &str,
    output: &mut Vec<u8>,
    config: &SbdfEncodingConfig,
    schema: Arc<Schema>,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
) -> Result<(usize, usize), String> {
    let format = CsvFormat::default()
        .with_header(has_header)
        .with_delimiter(delimiter);
    let reader = CsvReaderBuilder::new(schema)
        .with_format(format)
        .with_batch_size(batch_size)
        .build(input)
        .map_err(|error| format!("failed to create CSV reader for '{source_name}': {error}"))?;
    let mut total_rows = 0usize;
    let mut total_decoded_bytes = 0usize;
    for batch in reader {
        let batch = batch.map_err(|error| {
            format!("failed to decode CSV record batch from '{source_name}': {error}")
        })?;
        total_rows = total_rows.saturating_add(batch.num_rows());
        total_decoded_bytes = total_decoded_bytes.saturating_add(
            batch
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>(),
        );
        encode_record_batch_into(
            output,
            &config.columns,
            &config.column_types,
            config.encoding_strategy(),
            &batch,
        )
        .map_err(|error| error.to_string())?;
    }
    Ok((total_rows, total_decoded_bytes))
}

#[allow(clippy::too_many_arguments)]
fn csv_mmap_parallel_to_sbdf(
    csv_path: &str,
    sbdf_path: &str,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
    workers: usize,
    schema: Arc<Schema>,
    use_native_buffers: bool,
    config: &SbdfEncodingConfig,
) -> Result<(), String> {
    let input = File::open(csv_path)
        .map_err(|error| format!("failed to reopen CSV file '{csv_path}': {error}"))?;
    let mmap = unsafe { MmapOptions::new().map(&input) }
        .map_err(|error| format!("failed to mmap CSV file '{csv_path}': {error}"))?;
    let mmap = SharedMmap(Arc::new(mmap));
    let target_rows = Arc::new(AtomicUsize::new(batch_size));
    let spans = CsvSpanIter::new(
        mmap.clone(),
        batch_size,
        Arc::clone(&target_rows),
        TARGET_CSV_INPUT_BYTES,
        has_header,
    )?;

    let mut assembler = SbdfAssembler::new(Path::new(sbdf_path), config)?;
    let encode_config = config.clone();
    let encode_mmap = mmap.clone();
    let adaptive_rows = Arc::clone(&target_rows);
    let source_path = csv_path.to_string();
    let decode_schema = Arc::clone(&schema);
    let tasks = spans.map(|span| {
        span.map(|span| SequencedTask {
            sequence: span.sequence,
            payload: span,
        })
    });
    parallel_encode_tasks(tasks, workers, 0, &mut assembler, move |task, partial| {
        let CsvSpan {
            sequence,
            start,
            end,
            expected_rows,
        } = task.payload;
        let result = (|| {
            let mut writer = encode_config
                .writer(&partial, false)
                .map_err(|error| error.to_string())?;
            let source_span = format!("'{source_path}' bytes {start}..{end}");
            let (row_count, decoded_bytes) = if use_native_buffers {
                decode_native_csv_to_writer(
                    &encode_mmap.as_ref()[start..end],
                    &source_span,
                    &mut writer,
                    &encode_config.columns,
                    &encode_config.column_types,
                    batch_size,
                    delimiter,
                    false,
                )
            } else {
                decode_arrow_csv_to_writer(
                    &encode_mmap.as_ref()[start..end],
                    &source_span,
                    &mut writer,
                    Arc::clone(&decode_schema),
                    batch_size,
                    delimiter,
                    false,
                )
            }
            .map_err(|error| {
                format!("failed to decode CSV span '{source_path}' bytes {start}..{end}: {error}")
            })?;
            if row_count != expected_rows {
                return Err(format!(
                    "CSV span {sequence} produced {row_count} rows; boundary count was {expected_rows}"
                ));
            }
            writer.finish(false).map_err(|error| error.to_string())?;
            if row_count > 0 && decoded_bytes > 0 {
                let next_rows = row_count
                    .saturating_mul(TARGET_CSV_DECODED_BYTES)
                    .checked_div(decoded_bytes)
                    .unwrap_or(1)
                    .clamp(1, batch_size);
                adaptive_rows.store(next_rows, Ordering::Relaxed);
            }
            complete_fragment(
                sequence,
                partial.clone(),
                row_count,
                encode_config.fingerprint(),
            )
        })();
        if result.is_err() {
            let _ = fs::remove_file(&partial);
        }
        result
    })?;
    assembler.finish()
}

#[allow(clippy::too_many_arguments)]
fn csv_to_sbdf_streaming_impl(
    csv_path: String,
    sbdf_path: String,
    batch_size: usize,
    infer_schema_rows: usize,
    column_types: Option<HashMap<String, String>>,
    delimiter: u8,
    has_header: bool,
    encoding_rle: bool,
    adaptive_encoding: bool,
    workers: usize,
) -> PyResult<()> {
    if batch_size == 0 {
        return Err(PyValueError::new_err(
            "batch_size must be greater than zero",
        ));
    }
    if infer_schema_rows == 0 {
        return Err(PyValueError::new_err(
            "infer_schema_rows must be greater than zero",
        ));
    }
    if workers == 0 {
        return Err(PyValueError::new_err("workers must be greater than zero"));
    }
    if workers > MAX_PARALLEL_WORKERS {
        return Err(PyValueError::new_err(format!(
            "workers must not exceed {MAX_PARALLEL_WORKERS}"
        )));
    }

    let format = CsvFormat::default()
        .with_header(has_header)
        .with_delimiter(delimiter);
    let inference_input = File::open(&csv_path).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to open CSV file '{csv_path}': {error}"))
    })?;
    let (inferred_schema, records_read) = format
        .infer_schema(inference_input, Some(infer_schema_rows))
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to infer CSV schema from '{csv_path}': {error}"
            ))
        })?;
    if records_read == 0 {
        return Err(PyValueError::new_err(format!(
            "CSV file '{csv_path}' contains no data rows"
        )));
    }

    let schema = csv_schema_with_rules(&inferred_schema, column_types.as_ref())?;
    let (columns, typed_columns) = infer_arrow_schema(&schema)?;
    let config = SbdfEncodingConfig::new(
        columns.clone(),
        &typed_columns,
        encoding_rle,
        adaptive_encoding,
    )?;
    let use_native_buffers = config.columns.len() >= NATIVE_CSV_MIN_COLUMNS;
    let schema = Arc::new(schema);
    if workers > 1 {
        match csv_mmap_parallel_to_sbdf(
            &csv_path,
            &sbdf_path,
            batch_size,
            delimiter,
            has_header,
            workers,
            Arc::clone(&schema),
            use_native_buffers,
            &config,
        ) {
            Ok(()) => return Ok(()),
            Err(error) if error.starts_with("failed to mmap CSV file") => {
                // Mapping is an optimization. Preserve the bounded sequential path when the
                // platform or input cannot be memory mapped.
            }
            Err(error) => return Err(PyRuntimeError::new_err(error)),
        }
    }
    let input = File::open(&csv_path).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to reopen CSV file '{csv_path}': {error}"))
    })?;
    let mut writer = SequentialDirectSbdfWriter::new(Path::new(&sbdf_path), &config)
        .map_err(PyRuntimeError::new_err)?;
    if use_native_buffers {
        decode_native_csv_to_direct_writer(
            input,
            &csv_path,
            &mut writer,
            &config,
            batch_size,
            delimiter,
            has_header,
        )
    } else {
        decode_arrow_csv_to_direct_writer(
            input,
            &csv_path,
            &mut writer,
            &config,
            schema,
            batch_size,
            delimiter,
            has_header,
        )
    }
    .map_err(PyRuntimeError::new_err)?;

    writer.finish().map_err(PyRuntimeError::new_err)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn parquet_files_to_sbdf_streaming_impl(
    parquet_files: Vec<String>,
    sbdf_path: String,
    batch_size: usize,
    column_types: Option<HashMap<String, String>>,
    encoding_rle: bool,
    adaptive_encoding: bool,
    workers: usize,
    adaptive_workers: bool,
) -> PyResult<()> {
    if batch_size == 0 {
        return Err(PyValueError::new_err(
            "batch_size must be greater than zero",
        ));
    }
    if parquet_files.is_empty() {
        return Err(PyValueError::new_err("parquet_files must not be empty"));
    }
    if workers == 0 {
        return Err(PyValueError::new_err("workers must be greater than zero"));
    }
    if workers > MAX_PARALLEL_WORKERS {
        return Err(PyValueError::new_err(format!(
            "workers must not exceed {MAX_PARALLEL_WORKERS}"
        )));
    }

    let first_path = parquet_files[0].clone();
    let mut plans = Vec::with_capacity(parquet_files.len());
    let mut expected_schema = None;
    let mut total_row_groups = 0usize;
    let mut max_row_groups_per_file = 0usize;
    let mut total_uncompressed_bytes = 0u64;
    for parquet_path in &parquet_files {
        let metadata = load_parquet_reader_metadata(parquet_path)?;
        let schema = metadata.schema().as_ref().clone();
        if let Some(expected) = expected_schema.as_ref() {
            if &schema != expected {
                return Err(PyValueError::new_err(format!(
                    "schema mismatch for Parquet file '{parquet_path}'; expected schema from '{first_path}'"
                )));
            }
        } else {
            expected_schema = Some(schema);
        }
        let row_groups = metadata.metadata().num_row_groups();
        total_row_groups = total_row_groups.saturating_add(row_groups);
        max_row_groups_per_file = max_row_groups_per_file.max(row_groups);
        total_uncompressed_bytes = total_uncompressed_bytes.saturating_add(
            metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|row_group| row_group.total_byte_size().max(0) as u64)
                .sum::<u64>(),
        );
        plans.push(ParquetInputPlan {
            source_path: PathBuf::from(parquet_path),
            batch_size: effective_parquet_batch_size(batch_size, metadata.metadata()),
            metadata,
        });
    }
    let expected_schema = expected_schema.expect("non-empty Parquet input has a schema");
    let (columns, _) = infer_arrow_schema(&expected_schema)?;
    let typed_columns = get_column_types(&columns, column_types, &expected_schema)?;
    let config = SbdfEncodingConfig::new(
        columns.clone(),
        &typed_columns,
        encoding_rle,
        adaptive_encoding,
    )?;
    let effective_workers = if adaptive_workers {
        select_parquet_workers(
            workers,
            total_row_groups,
            max_row_groups_per_file,
            total_uncompressed_bytes,
        )
    } else {
        workers
    };

    if effective_workers > 1 {
        #[cfg(feature = "planned-offset-prototype")]
        {
            let mut assembler = DirectSbdfAssembler::new(Path::new(&sbdf_path), &config)
                .map_err(PyRuntimeError::new_err)?;
            let mut next_sequence = 0usize;
            for plan in plans {
                let row_group_count = plan.metadata.metadata().num_row_groups();
                let source_path = plan.source_path;
                let task_metadata = plan.metadata;
                let effective_batch_size = plan.batch_size;
                let start_sequence = next_sequence;
                let tasks = (0..row_group_count).map(move |row_group_index| {
                    let expected_rows =
                        task_metadata.metadata().row_group(row_group_index).num_rows();
                    let expected_rows = usize::try_from(expected_rows).map_err(|_| {
                        format!(
                            "invalid negative or oversized row count for Parquet row-group {row_group_index} in '{}'",
                            source_path.display()
                        )
                    })?;
                    Ok(SequencedTask {
                        sequence: start_sequence + row_group_index,
                        payload: ParquetRowGroupTask {
                            source_path: source_path.clone(),
                            metadata: task_metadata.clone(),
                            row_group_index,
                            expected_rows,
                            batch_size: effective_batch_size,
                        },
                    })
                });
                let planning_config = config.clone();
                next_sequence = parallel_plan_record_batches(
                    tasks,
                    effective_workers,
                    next_sequence,
                    &mut assembler,
                    config.clone(),
                    move |task| {
                        plan_parquet_row_group_batches(
                            task.sequence,
                            task.payload,
                            &planning_config,
                        )
                    },
                )
                .map_err(PyRuntimeError::new_err)?;
            }
            return assembler
                .finish_with_sync(false)
                .map_err(PyRuntimeError::new_err);
        }

        #[cfg(not(feature = "planned-offset-prototype"))]
        {
            let mut assembler = SbdfAssembler::new(Path::new(&sbdf_path), &config)
                .map_err(PyRuntimeError::new_err)?;
            let mut next_sequence = 0usize;
            for plan in plans {
                let row_group_count = plan.metadata.metadata().num_row_groups();
                let source_path = plan.source_path;
                let task_metadata = plan.metadata;
                let effective_batch_size = plan.batch_size;
                let start_sequence = next_sequence;
                let tasks = (0..row_group_count).map(move |row_group_index| {
                let expected_rows = task_metadata.metadata().row_group(row_group_index).num_rows();
                let expected_rows = usize::try_from(expected_rows).map_err(|_| {
                    format!(
                        "invalid negative or oversized row count for Parquet row-group {row_group_index} in '{}'",
                        source_path.display()
                    )
                })?;
                Ok(SequencedTask {
                    sequence: start_sequence + row_group_index,
                    payload: ParquetRowGroupTask {
                        source_path: source_path.clone(),
                        metadata: task_metadata.clone(),
                        row_group_index,
                        expected_rows,
                        batch_size: effective_batch_size,
                    },
                })
            });
                let encode_config = config.clone();
                next_sequence = parallel_encode_tasks(
                    tasks,
                    effective_workers,
                    next_sequence,
                    &mut assembler,
                    move |task, partial| {
                        encode_parquet_row_group_fragment(
                            task.sequence,
                            task.payload,
                            partial,
                            &encode_config,
                        )
                    },
                )
                .map_err(PyRuntimeError::new_err)?;
            }
            return assembler.finish().map_err(PyRuntimeError::new_err);
        }
    }

    #[cfg(feature = "planned-offset-prototype")]
    {
        let mut writer = SequentialDirectSbdfWriter::new(Path::new(&sbdf_path), &config)
            .map_err(PyRuntimeError::new_err)?;
        for plan in plans {
            let parquet_path = plan.source_path.to_string_lossy().into_owned();
            let prefetch = should_prefetch_parquet_batch(plan.batch_size, plan.metadata.metadata());
            let input = File::open(&plan.source_path).map_err(|error| {
                PyRuntimeError::new_err(format!(
                    "failed to reopen Parquet file '{parquet_path}': {error}"
                ))
            })?;
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(input, plan.metadata)
                .with_batch_size(plan.batch_size)
                .build()
                .map_err(|error| {
                    PyRuntimeError::new_err(format!(
                        "failed to build planned Parquet batch reader for '{parquet_path}': {error}"
                    ))
                })?;
            let read_path = parquet_path.clone();
            consume_batches(
                reader.map(move |batch| {
                    batch.map_err(|error| {
                        format!("failed while reading Parquet batch '{read_path}': {error}")
                    })
                }),
                |batch| {
                    writer.write_record_batch(&batch, &config).map_err(|error| {
                        format!(
                            "failed while writing planned SBDF data from '{parquet_path}': {error}"
                        )
                    })
                },
                prefetch,
            )
            .map_err(PyRuntimeError::new_err)?;
        }
        writer.finish().map_err(PyRuntimeError::new_err)
    }

    #[cfg(not(feature = "planned-offset-prototype"))]
    {
        let mut writer = StreamingSbdfWriter::new(
            sbdf_path,
            Some(columns.clone()),
            Some(
                typed_columns
                    .iter()
                    .map(|(column, value_type)| {
                        (column.clone(), value_type.spotfire_name().to_string())
                    })
                    .collect(),
            ),
            encoding_rle,
            adaptive_encoding,
        )?;

        for plan in plans {
            let parquet_path = plan.source_path.to_string_lossy().into_owned();
            let prefetch = should_prefetch_parquet_batch(plan.batch_size, plan.metadata.metadata());
            let input = File::open(&plan.source_path).map_err(|error| {
                PyRuntimeError::new_err(format!(
                    "failed to reopen Parquet file '{parquet_path}': {error}"
                ))
            })?;

            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(input, plan.metadata)
                .with_batch_size(plan.batch_size)
                .build()
                .map_err(|error| {
                    PyRuntimeError::new_err(format!(
                        "failed to build Parquet batch reader for '{parquet_path}': {error}"
                    ))
                })?;

            let read_path = parquet_path.clone();
            consume_batches(
                reader.map(move |batch| {
                    batch.map_err(|error| {
                        format!("failed while reading Parquet batch '{read_path}': {error}")
                    })
                }),
                |batch| {
                    write_record_batch_to_sbdf(&mut writer, &batch).map_err(|error| {
                        format!("failed while writing SBDF data from '{parquet_path}': {error}")
                    })
                },
                prefetch,
            )
            .map_err(PyRuntimeError::new_err)?;
        }

        writer.close()?;
        Ok(())
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (parquet_path, sbdf_path, batch_size=5_000, column_types=None, encoding_rle=true, adaptive_encoding=false, workers=3, adaptive_workers=true))]
fn parquet_to_sbdf_streaming(
    parquet_path: String,
    sbdf_path: String,
    batch_size: usize,
    column_types: Option<HashMap<String, String>>,
    encoding_rle: bool,
    adaptive_encoding: bool,
    workers: usize,
    adaptive_workers: bool,
) -> PyResult<()> {
    parquet_files_to_sbdf_streaming_impl(
        vec![parquet_path],
        sbdf_path,
        batch_size,
        column_types,
        encoding_rle,
        adaptive_encoding,
        workers,
        adaptive_workers,
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (parquet_files, sbdf_path, batch_size=5_000, column_types=None, encoding_rle=true, adaptive_encoding=false, workers=3, adaptive_workers=true))]
fn parquet_files_to_sbdf_streaming(
    parquet_files: Vec<String>,
    sbdf_path: String,
    batch_size: usize,
    column_types: Option<HashMap<String, String>>,
    encoding_rle: bool,
    adaptive_encoding: bool,
    workers: usize,
    adaptive_workers: bool,
) -> PyResult<()> {
    parquet_files_to_sbdf_streaming_impl(
        parquet_files,
        sbdf_path,
        batch_size,
        column_types,
        encoding_rle,
        adaptive_encoding,
        workers,
        adaptive_workers,
    )
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (csv_path, sbdf_path, batch_size=5_000, infer_schema_rows=10_000, column_types=None, delimiter=b',', has_header=true, encoding_rle=true, adaptive_encoding=false, workers=1))]
fn csv_to_sbdf_streaming(
    csv_path: String,
    sbdf_path: String,
    batch_size: usize,
    infer_schema_rows: usize,
    column_types: Option<HashMap<String, String>>,
    delimiter: u8,
    has_header: bool,
    encoding_rle: bool,
    adaptive_encoding: bool,
    workers: usize,
) -> PyResult<()> {
    csv_to_sbdf_streaming_impl(
        csv_path,
        sbdf_path,
        batch_size,
        infer_schema_rows,
        column_types,
        delimiter,
        has_header,
        encoding_rle,
        adaptive_encoding,
        workers,
    )
}

#[pyfunction]
fn resolve_dataframe_column_types(
    columns: Vec<String>,
    dtype_names: Vec<String>,
) -> PyResult<HashMap<String, String>> {
    if columns.len() != dtype_names.len() {
        return Err(PyValueError::new_err(
            "columns and dtype_names must have the same length",
        ));
    }
    columns
        .into_iter()
        .zip(dtype_names)
        .map(|(column, dtype_name)| {
            type_rules::dataframe_dtype(&column, &dtype_name)
                .map(|value_type| (column.clone(), value_type.spotfire_name().to_string()))
                .ok_or_else(|| {
                    PyTypeError::new_err(format!(
                        "automatic SBDF type mapping is not available for DataFrame column '{column}' with dtype '{dtype_name}'; provide column_types"
                    ))
                })
        })
        .collect()
}

#[pyfunction]
fn generate_sbdf_sidecar(
    sbdf_path: String,
    sidecar_path: String,
    table_id: String,
    row_key_columns: Vec<String>,
) -> PyResult<()> {
    sbdf_index::generate_sidecar(
        Path::new(&sbdf_path),
        Path::new(&sidecar_path),
        &table_id,
        &row_key_columns,
    )
    .map_err(PyRuntimeError::new_err)
}

#[pyclass(unsendable)]
struct StreamingSbdfWriter {
    output: Option<BufWriter<File>>,
    columns: Vec<String>,
    column_types: Vec<ValueType>,
    initialized: bool,
    closed: bool,
    encoding_rle: bool,
    adaptive_encoding: bool,
    output_buffer: Vec<u8>,
}

#[pymethods]
impl StreamingSbdfWriter {
    #[new]
    #[pyo3(signature = (sbdf_file, columns=None, column_types=None, encoding_rle=true, adaptive_encoding=false))]
    fn new(
        sbdf_file: String,
        columns: Option<Vec<String>>,
        column_types: Option<HashMap<String, String>>,
        encoding_rle: bool,
        adaptive_encoding: bool,
    ) -> PyResult<Self> {
        let output_file = File::create(&sbdf_file).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to open SBDF file '{sbdf_file}': {error}"))
        })?;
        let mut writer = Self {
            output: Some(BufWriter::with_capacity(
                DIRECT_SINK_BUFFER_BYTES,
                output_file,
            )),
            columns: Vec::new(),
            column_types: Vec::new(),
            initialized: false,
            closed: false,
            encoding_rle,
            adaptive_encoding,
            output_buffer: Vec::new(),
        };
        if let Some(columns) = columns {
            let column_types = column_types
                .ok_or_else(|| PyValueError::new_err("column_types must be provided"))?;
            writer.initialize_schema(
                columns,
                column_types
                    .into_iter()
                    .map(|(key, value)| ValueType::from_name(&value).map(|typed| (key, typed)))
                    .collect::<PyResult<HashMap<_, _>>>()?,
            )?;
        }
        Ok(writer)
    }

    fn write_batch(&mut self, _py: Python<'_>, batch: Bound<'_, PyDict>) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("writer already closed"));
        }

        let columns: Vec<String> = batch
            .keys()
            .iter()
            .map(|item| item.extract::<String>())
            .collect::<PyResult<_>>()?;
        if columns.is_empty() {
            return Ok(());
        }

        let row_count = batch
            .get_item(columns[0].as_str())?
            .ok_or_else(|| PyValueError::new_err("missing first column"))?
            .len()?;
        for column in &columns {
            let len = batch
                .get_item(column.as_str())?
                .ok_or_else(|| PyValueError::new_err(format!("missing batch column: {column}")))?
                .len()?;
            if len != row_count {
                return Err(PyValueError::new_err(
                    "all batch columns must have the same row count",
                ));
            }
        }
        if row_count == 0 {
            return Ok(());
        }

        if !self.initialized {
            let mut inferred = HashMap::new();
            for column in &columns {
                let list = batch.get_item(column.as_str())?.unwrap();
                for item in list.try_iter()? {
                    let item = item?;
                    if !is_missing(&item)? {
                        inferred.insert(column.clone(), ValueType::infer(&item)?);
                        break;
                    }
                }
            }
            self.initialize_schema(columns.clone(), inferred)?;
        }

        if columns != self.columns {
            return Err(PyValueError::new_err(
                "batch columns do not match writer schema",
            ));
        }

        let mut output = self.take_output_buffer();
        let encode_result = (|| {
            rust_sbdf::begin_table_slice(&mut output, self.columns.len())
                .map_err(PyRuntimeError::new_err)?;
            for (index, column) in self.columns.iter().enumerate() {
                let list = batch.get_item(column.as_str())?.unwrap();
                let values: Vec<Bound<'_, PyAny>> = list.try_iter()?.collect::<PyResult<_>>()?;
                let invalids: Vec<u8> = values
                    .iter()
                    .map(|value| Ok(u8::from(is_missing(value)?)))
                    .collect::<PyResult<_>>()?;
                let buffer = build_column_buffer(self.column_types[index], &values, &invalids)?;
                let value_type = self.column_types[index];
                rust_sbdf::encode_column_slice(
                    &mut output,
                    value_type.sbdf_type_id(),
                    buffer.value_view(),
                    self.encoding_strategy(),
                    invalids.contains(&1).then_some(invalids.as_slice()),
                )
                .map_err(|error| {
                    PyRuntimeError::new_err(format!(
                        "failed to encode Rust SBDF column '{column}': {error}"
                    ))
                })?;
            }
            Ok(())
        })();
        self.finish_output_buffer(output, encode_result)
    }

    fn close(&mut self) -> PyResult<()> {
        self.finish(true)
    }
}

impl StreamingSbdfWriter {
    fn new_typed(
        sbdf_file: &Path,
        columns: Vec<String>,
        column_types: Vec<ValueType>,
        encoding_rle: bool,
        adaptive_encoding: bool,
        write_preamble: bool,
    ) -> PyResult<Self> {
        let output_file = File::create(sbdf_file).map_err(|error| {
            PyRuntimeError::new_err(format!(
                "failed to open SBDF file '{}': {error}",
                sbdf_file.display()
            ))
        })?;
        let mut writer = Self {
            output: Some(BufWriter::with_capacity(
                DIRECT_SINK_BUFFER_BYTES,
                output_file,
            )),
            columns: Vec::new(),
            column_types: Vec::new(),
            initialized: false,
            closed: false,
            encoding_rle,
            adaptive_encoding,
            output_buffer: Vec::new(),
        };
        writer.initialize_typed_schema(columns, column_types, write_preamble)?;
        Ok(writer)
    }

    fn initialize_schema(
        &mut self,
        columns: Vec<String>,
        types: HashMap<String, ValueType>,
    ) -> PyResult<()> {
        let column_types = columns
            .iter()
            .map(|column| {
                types.get(column).copied().ok_or_else(|| {
                    PyValueError::new_err(format!("missing type for column '{column}'"))
                })
            })
            .collect::<PyResult<_>>()?;
        self.initialize_typed_schema(columns, column_types, true)
    }

    fn initialize_typed_schema(
        &mut self,
        columns: Vec<String>,
        column_types: Vec<ValueType>,
        write_preamble: bool,
    ) -> PyResult<()> {
        if columns.len() != column_types.len() {
            return Err(PyValueError::new_err(
                "columns and column_types must have the same length",
            ));
        }
        self.columns = columns;
        self.column_types = column_types;
        if write_preamble {
            let type_ids = self
                .column_types
                .iter()
                .map(|value_type| value_type.sbdf_type_id())
                .collect::<Vec<_>>();
            rust_sbdf::write_preamble(
                self.output
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("writer already closed"))?,
                &self.columns,
                &type_ids,
            )
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to write SBDF metadata: {error}"))
            })?;
        }

        self.initialized = true;
        Ok(())
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("writer already closed"));
        }
        self.output
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("writer already closed"))?
            .write_all(bytes)
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to append SBDF fragment bytes: {error}"))
            })
    }

    fn encoding_strategy(&self) -> rust_sbdf::EncodingStrategy {
        if self.adaptive_encoding {
            rust_sbdf::EncodingStrategy::Adaptive
        } else if self.encoding_rle {
            rust_sbdf::EncodingStrategy::Rle
        } else {
            rust_sbdf::EncodingStrategy::Plain
        }
    }

    fn take_output_buffer(&mut self) -> Vec<u8> {
        let mut output = std::mem::take(&mut self.output_buffer);
        output.clear();
        output
    }

    fn finish_output_buffer(
        &mut self,
        mut output: Vec<u8>,
        encode_result: PyResult<()>,
    ) -> PyResult<()> {
        const MAX_RETAINED_CAPACITY: usize = 64 * 1024 * 1024;

        let result = encode_result.and_then(|()| self.append_bytes(&output));
        if output.capacity() <= MAX_RETAINED_CAPACITY {
            output.clear();
            self.output_buffer = output;
        }
        result
    }

    fn finish(&mut self, write_end: bool) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        if write_end && self.initialized {
            rust_sbdf::write_end_marker(
                self.output
                    .as_mut()
                    .ok_or_else(|| PyRuntimeError::new_err("writer already closed"))?,
            )
            .map_err(|error| {
                PyRuntimeError::new_err(format!("failed to write SBDF end marker: {error}"))
            })?;
        }
        if let Some(mut output) = self.output.take() {
            output.flush().map_err(|error| {
                PyRuntimeError::new_err(format!("failed to flush SBDF output: {error}"))
            })?;
        }
        self.closed = true;
        Ok(())
    }

    fn cleanup(&mut self) {
        if let Some(mut output) = self.output.take() {
            let _ = output.flush();
        }
    }
}

impl Drop for StreamingSbdfWriter {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[pymodule]
fn _native(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__doc__", "Rust-backed streaming SBDF writer")?;
    module.add("SBDFError", _py.get_type::<PyRuntimeError>())?;
    module.add_class::<StreamingSbdfWriter>()?;
    module.add_function(wrap_pyfunction!(parquet_to_sbdf_streaming, module)?)?;
    module.add_function(wrap_pyfunction!(parquet_files_to_sbdf_streaming, module)?)?;
    module.add_function(wrap_pyfunction!(csv_to_sbdf_streaming, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_dataframe_column_types, module)?)?;
    module.add_function(wrap_pyfunction!(generate_sbdf_sidecar, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_native_column_buffer, cap_parquet_batch_size, consume_batches,
        consume_with_one_batch_prefetch, csv_schema_with_rules, days_since_epoch,
        decode_native_csv_to_writer, encode_parquet_row_group_fragment, estimated_batch_bytes,
        fnv1a_update, parallel_encode_buffers, parallel_plan_record_batches,
        parquet_files_to_sbdf_streaming_impl, plan_native_csv_batch, plan_record_batch,
        sbdf_millis_from_unix_days, sbdf_millis_from_unix_millis, select_parquet_workers,
        write_record_batch_to_sbdf, DirectSbdfAssembler, EncodedBufferResult, ParquetRowGroupTask,
        PlannedBatchResult, PositionalFileSink, SbdfAssembler, SbdfEncodingConfig, SequencedTask,
        SequentialDirectSbdfWriter, ValueType, FNV_OFFSET_BASIS, MAX_PREFETCH_BATCH_BYTES,
        MILLIS_PER_DAY, UNIX_EPOCH_DAYS_FROM_YEAR_ONE, UNIX_EPOCH_MILLIS_FROM_YEAR_ONE,
    };
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_csv::reader::{Format as CsvFormat, ReaderBuilder as CsvReaderBuilder};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::arrow_reader::ArrowReaderMetadata;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::sync::mpsc::{channel, TryRecvError};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn create(name: &str) -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "streaming-sbdf-{name}-{}-{unique}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn year_one_epoch_is_zero_days() {
        assert_eq!(days_since_epoch(1, 1, 1), 0);
    }

    #[test]
    fn unix_epoch_offset_matches_year_one_calendar_days() {
        assert_eq!(days_since_epoch(1970, 1, 1), UNIX_EPOCH_DAYS_FROM_YEAR_ONE);
        assert_eq!(
            UNIX_EPOCH_MILLIS_FROM_YEAR_ONE,
            UNIX_EPOCH_DAYS_FROM_YEAR_ONE * MILLIS_PER_DAY
        );
    }

    #[test]
    fn date32_days_are_shifted_from_unix_epoch_to_sbdf_epoch() {
        assert_eq!(
            sbdf_millis_from_unix_days(0),
            UNIX_EPOCH_MILLIS_FROM_YEAR_ONE
        );
        let days_2025 = days_since_epoch(2025, 1, 1) - days_since_epoch(1970, 1, 1);
        assert_eq!(
            sbdf_millis_from_unix_days(days_2025),
            days_since_epoch(2025, 1, 1) * MILLIS_PER_DAY
        );
    }

    #[test]
    fn timestamp_millis_are_shifted_from_unix_epoch_to_sbdf_epoch() {
        assert_eq!(
            sbdf_millis_from_unix_millis(0),
            UNIX_EPOCH_MILLIS_FROM_YEAR_ONE
        );
        let noon_ms = 12 * 60 * 60 * 1000;
        assert_eq!(
            sbdf_millis_from_unix_millis(noon_ms),
            UNIX_EPOCH_MILLIS_FROM_YEAR_ONE + noon_ms
        );
    }

    #[test]
    fn prefetch_decodes_at_most_one_batch_ahead_and_preserves_order() {
        let (decoded_sender, decoded_receiver) = channel();
        let producer = (0..3).map(move |value| {
            decoded_sender.send(value).unwrap();
            Ok(value)
        });
        let mut consumed = Vec::new();

        consume_with_one_batch_prefetch(producer, |value| {
            consumed.push(value);
            if value == 0 {
                assert_eq!(decoded_receiver.recv().unwrap(), 0);
                assert_eq!(
                    decoded_receiver
                        .recv_timeout(Duration::from_secs(1))
                        .unwrap(),
                    1
                );
                assert_eq!(decoded_receiver.try_recv(), Err(TryRecvError::Empty));
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(consumed, vec![0, 1, 2]);
    }

    #[test]
    fn prefetch_stops_after_decode_error() {
        let producer = vec![Ok(1), Err("decode failed".to_string()), Ok(3)].into_iter();
        let mut consumed = Vec::new();

        let error = consume_with_one_batch_prefetch(producer, |value| {
            consumed.push(value);
            Ok(())
        })
        .unwrap_err();

        assert_eq!(error, "decode failed");
        assert_eq!(consumed, vec![1]);
    }

    #[test]
    fn prefetch_does_not_deadlock_after_consumer_error() {
        let producer = (0..100).map(Ok::<_, String>);

        let error = consume_with_one_batch_prefetch(producer, |_| Err("write failed".to_string()))
            .unwrap_err();

        assert_eq!(error, "write failed");
    }

    #[test]
    fn csv_schema_applies_reusable_name_rules_before_inferred_types() {
        let inferred = Schema::new(vec![
            Field::new("wafer_id", DataType::Utf8, true),
            Field::new("event_time_text", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]);

        let resolved = csv_schema_with_rules(&inferred, None).unwrap();

        assert_eq!(resolved.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            resolved.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(resolved.field(2).data_type(), &DataType::Float64);
    }

    #[test]
    fn explicit_csv_column_type_has_highest_precedence() {
        let inferred = Schema::new(vec![Field::new("wafer_id", DataType::Utf8, true)]);
        let provided = HashMap::from([("wafer_id".to_string(), "String".to_string())]);

        let resolved = csv_schema_with_rules(&inferred, Some(&provided)).unwrap();

        assert_eq!(resolved.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn native_and_arrow_csv_decoders_produce_identical_rust_sbdf_bytes() {
        let directory = TestDirectory::create("native-csv-parity");
        let input = directory.0.join("input.csv");
        fs::write(
            &input,
            "wafer_id,event_time,flag,value,text,date\n\
             1,2026-08-20T01:02:03.004,true,1.5,alpha,2026-08-20\n\
             2,2026-08-20T01:02:04.005,FALSE,,\"first\nsecond\",2026-08-21\n\
             3,2026-08-20T01:02:05.006,true,3.5,\"a \"\"quote\"\"\",\n",
        )
        .unwrap();

        let format = CsvFormat::default().with_header(true).with_delimiter(b',');
        let (inferred, _) = format
            .infer_schema(File::open(&input).unwrap(), Some(100))
            .unwrap();
        let schema = csv_schema_with_rules(&inferred, None).unwrap();
        let (columns, typed_columns) = super::infer_arrow_schema(&schema).unwrap();
        for encoding_rle in [false, true] {
            let config =
                SbdfEncodingConfig::new(columns.clone(), &typed_columns, encoding_rle, false)
                    .unwrap();
            let direct = directory.0.join(format!("direct-{encoding_rle}.sbdf"));
            let mut direct_writer = config.writer(&direct, true).unwrap();
            decode_native_csv_to_writer(
                File::open(&input).unwrap(),
                input.to_str().unwrap(),
                &mut direct_writer,
                &config.columns,
                &config.column_types,
                2,
                b',',
                true,
            )
            .unwrap();
            direct_writer.close().unwrap();

            let reference = directory
                .0
                .join(format!("arrow-reference-{encoding_rle}.sbdf"));
            let mut writer = config.writer(&reference, true).unwrap();
            let reader = CsvReaderBuilder::new(Arc::new(schema.clone()))
                .with_format(CsvFormat::default().with_header(true).with_delimiter(b','))
                .with_batch_size(2)
                .build(File::open(&input).unwrap())
                .unwrap();
            for batch in reader {
                write_record_batch_to_sbdf(&mut writer, &batch.unwrap()).unwrap();
            }
            writer.close().unwrap();

            let direct_bytes = fs::read(direct).unwrap();
            let reference_bytes = fs::read(reference).unwrap();
            let direct_hash = fnv1a_update(FNV_OFFSET_BASIS, &direct_bytes);
            let reference_hash = fnv1a_update(FNV_OFFSET_BASIS, &reference_bytes);
            assert_eq!(
                direct_hash, reference_hash,
                "native/Arrow Rust SBDF checksum mismatch for encoding_rle={encoding_rle}: native={direct_hash:016x}, Arrow={reference_hash:016x}"
            );
            assert_eq!(
                direct_bytes, reference_bytes,
                "native/Arrow Rust SBDF bytes differ for encoding_rle={encoding_rle}"
            );
        }
    }

    #[test]
    fn native_primitive_buffer_borrows_sliced_arrow_values() {
        let base = Int64Array::from(vec![10, 20, 30, 40]);
        let sliced = base.slice(1, 2);
        let (buffer, invalids) = build_native_column_buffer("value", &sliced).unwrap();

        assert!(buffer.borrows_arrow_payload());
        assert!(invalids.is_none());
        let super::rust_sbdf::ValueView::Primitive {
            bytes,
            count,
            width,
        } = buffer.value_view(ValueType::Long).unwrap()
        else {
            panic!("expected primitive value view");
        };
        assert_eq!(count, 2);
        assert_eq!(width, std::mem::size_of::<i64>());
        let values = bytes
            .chunks_exact(width)
            .map(|bytes| i64::from_ne_bytes(bytes.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(values, [20, 30]);
    }

    #[test]
    fn native_string_buffer_borrows_payload_and_only_allocates_invalids_when_needed() {
        let array = StringArray::from(vec![Some("alpha"), None, Some("gamma")]);

        let (buffer, invalids) = build_native_column_buffer("label", &array).unwrap();

        assert!(buffer.borrows_arrow_payload());
        assert_eq!(invalids, Some(vec![0, 1, 0]));
    }

    #[test]
    fn nullable_native_primitive_normalizes_null_payload_in_owned_buffer() {
        let array = Int64Array::from(vec![Some(10), None, Some(30)]);

        let (buffer, invalids) = build_native_column_buffer("value", &array).unwrap();

        assert!(!buffer.borrows_arrow_payload());
        assert_eq!(invalids, Some(vec![0, 1, 0]));
        let super::rust_sbdf::ValueView::Primitive { bytes, width, .. } =
            buffer.value_view(ValueType::Long).unwrap()
        else {
            panic!("expected primitive value view");
        };
        let values = bytes
            .chunks_exact(width)
            .map(|bytes| i64::from_ne_bytes(bytes.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(values, [10, 0, 30]);
    }

    #[test]
    fn parquet_batch_size_is_capped_by_uncompressed_row_width() {
        let total_rows = 300_000;
        let total_uncompressed_bytes = 1_070_623_282;

        assert_eq!(
            cap_parquet_batch_size(5_000, total_rows, total_uncompressed_bytes),
            5_000
        );
        assert!(cap_parquet_batch_size(50_000, total_rows, total_uncompressed_bytes) < 19_000);
        assert_eq!(cap_parquet_batch_size(50_000, 0, 0), 50_000);
    }

    #[test]
    fn adaptive_parquet_workers_follow_profiled_shape_boundaries() {
        let mebibyte = 1024 * 1024;

        assert_eq!(select_parquet_workers(3, 1, 1, 135 * mebibyte), 1);
        assert_eq!(select_parquet_workers(3, 21, 21, 75 * mebibyte), 2);
        assert_eq!(select_parquet_workers(3, 74, 74, 535 * mebibyte), 1);
        assert_eq!(select_parquet_workers(3, 147, 147, 1_069 * mebibyte), 3);
        assert_eq!(select_parquet_workers(3, 300, 300, 1_070 * mebibyte), 1);
        assert_eq!(select_parquet_workers(1, 147, 147, 1_069 * mebibyte), 1);
    }

    #[test]
    fn parquet_row_groups_are_independently_decoded_and_assembled_in_order() {
        let directory = TestDirectory::create("row-groups");
        let parquet_path = directory.0.join("input.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        let mut parquet_writer = ArrowWriter::try_new(
            File::create(&parquet_path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        parquet_writer.write(&batch).unwrap();
        parquet_writer.close().unwrap();

        let input = File::open(&parquet_path).unwrap();
        let metadata = ArrowReaderMetadata::load(&input, Default::default()).unwrap();
        assert_eq!(metadata.metadata().num_row_groups(), 3);

        let config = SbdfEncodingConfig::new(
            vec!["id".to_string(), "label".to_string()],
            &HashMap::from([
                ("id".to_string(), ValueType::Long),
                ("label".to_string(), ValueType::String),
            ]),
            true,
            false,
        )
        .unwrap();
        let output = directory.0.join("output.sbdf");
        let mut assembler = SbdfAssembler::new(&output, &config).unwrap();
        for row_group_index in 0..metadata.metadata().num_row_groups() {
            let partial = assembler
                .workspace
                .path
                .join(format!("slice-{row_group_index:020}.partial"));
            let fragment = encode_parquet_row_group_fragment(
                row_group_index,
                ParquetRowGroupTask {
                    source_path: parquet_path.clone(),
                    metadata: metadata.clone(),
                    row_group_index,
                    expected_rows: 2,
                    batch_size: 1,
                },
                partial,
                &config,
            )
            .unwrap();
            assert_eq!(fragment.row_count, 2);
            assembler.append_fragment(&fragment).unwrap();
        }
        assembler.finish().unwrap();

        assert!(output.metadata().unwrap().len() > 0);
        let parallel_output = directory.0.join("parallel-output.sbdf");
        parquet_files_to_sbdf_streaming_impl(
            vec![parquet_path.to_string_lossy().into_owned()],
            parallel_output.to_string_lossy().into_owned(),
            1,
            None,
            true,
            false,
            2,
            false,
        )
        .unwrap();
        assert_eq!(
            fs::read(&output).unwrap(),
            fs::read(&parallel_output).unwrap()
        );
        assert!(directory.0.join("input.parquet").exists());
        assert_eq!(
            fs::read_dir(&directory.0)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".work-"))
                .count(),
            0
        );
    }

    #[test]
    fn planned_offsets_preserve_sequence_when_buffers_finish_out_of_order() {
        let directory = TestDirectory::create("planned-offset-order");
        let config = SbdfEncodingConfig::new(
            vec!["value".to_string()],
            &HashMap::from([("value".to_string(), ValueType::Long)]),
            true,
            false,
        )
        .unwrap();
        let batches = vec![
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![10, 11])) as _,
            )])
            .unwrap(),
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![20, 21])) as _,
            )])
            .unwrap(),
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(vec![30, 31])) as _,
            )])
            .unwrap(),
        ];

        let expected_path = directory.0.join("expected.sbdf");
        let mut expected_writer = config.writer(&expected_path, true).unwrap();
        for batch in &batches {
            super::write_record_batch_to_sbdf(&mut expected_writer, batch).unwrap();
        }
        expected_writer.close().unwrap();

        let output_path = directory.0.join("direct.sbdf");
        let mut assembler = DirectSbdfAssembler::new(&output_path, &config).unwrap();
        let tasks = batches.into_iter().enumerate().map(|(sequence, batch)| {
            Ok(SequencedTask {
                sequence,
                payload: batch,
            })
        });
        let encode_config = config.clone();
        parallel_encode_buffers(tasks, 3, 0, &mut assembler, move |task| {
            std::thread::sleep(Duration::from_millis((2 - task.sequence) as u64 * 10));
            let mut bytes = Vec::new();
            super::encode_record_batch_into(
                &mut bytes,
                &encode_config.columns,
                &encode_config.column_types,
                encode_config.encoding_strategy(),
                &task.payload,
            )
            .map_err(|error| error.to_string())?;
            Ok(EncodedBufferResult {
                sequence: task.sequence,
                row_count: task.payload.num_rows(),
                bytes,
                schema_checksum: encode_config.fingerprint(),
            })
        })
        .unwrap();
        assembler.finish().unwrap();

        assert_eq!(
            fs::read(expected_path).unwrap(),
            fs::read(output_path).unwrap()
        );
    }

    #[test]
    fn planned_batch_handoff_writes_without_encoded_buffers() {
        let directory = TestDirectory::create("planned-batch-handoff");
        let config = SbdfEncodingConfig::new(
            vec!["value".to_string(), "label".to_string()],
            &HashMap::from([
                ("value".to_string(), ValueType::Long),
                ("label".to_string(), ValueType::String),
            ]),
            true,
            true,
        )
        .unwrap();
        let batches = vec![
            RecordBatch::try_from_iter(vec![
                ("value", Arc::new(Int64Array::from(vec![10, 11])) as _),
                ("label", Arc::new(StringArray::from(vec!["a", "a"])) as _),
            ])
            .unwrap(),
            RecordBatch::try_from_iter(vec![
                ("value", Arc::new(Int64Array::from(vec![20, 21])) as _),
                ("label", Arc::new(StringArray::from(vec!["bb", "cc"])) as _),
            ])
            .unwrap(),
            RecordBatch::try_from_iter(vec![
                ("value", Arc::new(Int64Array::from(vec![30, 31])) as _),
                (
                    "label",
                    Arc::new(StringArray::from(vec!["ddd", "ddd"])) as _,
                ),
            ])
            .unwrap(),
        ];
        let expected_path = directory.0.join("expected.sbdf");
        let mut expected_writer = config.writer(&expected_path, true).unwrap();
        for batch in &batches {
            super::write_record_batch_to_sbdf(&mut expected_writer, batch).unwrap();
        }
        expected_writer.close().unwrap();

        let output_path = directory.0.join("planned-direct.sbdf");
        let mut assembler = DirectSbdfAssembler::new(&output_path, &config).unwrap();
        let tasks = batches.into_iter().enumerate().map(|(sequence, batch)| {
            Ok(SequencedTask {
                sequence,
                payload: batch,
            })
        });
        let planning_config = config.clone();
        parallel_plan_record_batches(tasks, 3, 0, &mut assembler, config.clone(), move |task| {
            std::thread::sleep(Duration::from_millis((2 - task.sequence) as u64 * 10));
            let plan = plan_record_batch(&task.payload, &planning_config)?;
            let byte_len = plan.byte_len();
            let resident_bytes = task
                .payload
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .sum::<usize>()
                .saturating_add(plan.resident_bytes());
            Ok(PlannedBatchResult {
                sequence: task.sequence,
                row_count: task.payload.num_rows(),
                batches: vec![task.payload],
                plans: vec![plan],
                byte_len,
                resident_bytes,
                schema_checksum: planning_config.fingerprint(),
            })
        })
        .unwrap();
        assembler.finish().unwrap();

        assert_eq!(
            fs::read(expected_path).unwrap(),
            fs::read(output_path).unwrap()
        );
    }

    #[test]
    fn sequential_planned_writer_matches_existing_writer() {
        let directory = TestDirectory::create("sequential-planned-writer");
        let config = SbdfEncodingConfig::new(
            vec!["value".to_string(), "label".to_string()],
            &HashMap::from([
                ("value".to_string(), ValueType::Long),
                ("label".to_string(), ValueType::String),
            ]),
            true,
            true,
        )
        .unwrap();
        let batches = [
            RecordBatch::try_from_iter(vec![
                (
                    "value",
                    Arc::new(Int64Array::from(vec![Some(10), None, Some(12)])) as _,
                ),
                (
                    "label",
                    Arc::new(StringArray::from(vec!["same", "same", "other"])) as _,
                ),
            ])
            .unwrap(),
            RecordBatch::try_from_iter(vec![
                (
                    "value",
                    Arc::new(Int64Array::from(vec![Some(20), Some(21)])) as _,
                ),
                (
                    "label",
                    Arc::new(StringArray::from(vec!["variable-width", "tail"])) as _,
                ),
            ])
            .unwrap(),
        ];
        let expected_path = directory.0.join("expected.sbdf");
        let mut expected_writer = config.writer(&expected_path, true).unwrap();
        for batch in &batches {
            super::write_record_batch_to_sbdf(&mut expected_writer, batch).unwrap();
        }
        expected_writer.close().unwrap();

        let output_path = directory.0.join("sequential-direct.sbdf");
        let mut writer = SequentialDirectSbdfWriter::new(&output_path, &config).unwrap();
        for batch in &batches {
            writer.write_record_batch(batch, &config).unwrap();
        }
        writer.finish().unwrap();

        assert_eq!(
            fs::read(expected_path).unwrap(),
            fs::read(output_path).unwrap()
        );
    }

    #[test]
    fn planned_record_batch_encodes_directly_into_its_assigned_range() {
        let directory = TestDirectory::create("planned-record-batch");
        let path = directory.0.join("assigned-ranges.bin");
        let file = File::create(&path).unwrap();
        file.set_len(4096).unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "value",
                Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as _,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec!["same", "same", "other"])) as _,
            ),
        ])
        .unwrap();
        let config = SbdfEncodingConfig::new(
            vec!["value".to_string(), "label".to_string()],
            &HashMap::from([
                ("value".to_string(), ValueType::Long),
                ("label".to_string(), ValueType::String),
            ]),
            true,
            true,
        )
        .unwrap();
        let mut staged = Vec::new();
        super::encode_record_batch_into(
            &mut staged,
            &config.columns,
            &config.column_types,
            config.encoding_strategy(),
            &batch,
        )
        .unwrap();
        let plan = plan_record_batch(&batch, &config).unwrap();
        assert_eq!(plan.byte_len(), staged.len());

        let offset = 37u64;
        let mut sink = PositionalFileSink::new(&file, offset, plan.byte_len());
        super::encode_planned_record_batch(&mut sink, &batch, &config, &plan).unwrap();
        let (written, checksum) = sink.finish().unwrap();

        assert_eq!(written, staged.len());
        assert_eq!(checksum, fnv1a_update(FNV_OFFSET_BASIS, &staged));
        let output = fs::read(path).unwrap();
        assert!(output[..offset as usize].iter().all(|byte| *byte == 0));
        assert_eq!(
            &output[offset as usize..offset as usize + staged.len()],
            staged
        );
        assert_eq!(output[offset as usize + staged.len()], 0);
    }

    #[test]
    fn planned_native_csv_batch_uses_actual_spans_and_nulls() {
        let mut batch =
            super::native_csv::NativeCsvBatch::new(vec![ValueType::Long, ValueType::String], 3);
        let columns = vec!["value".to_string(), "label".to_string()];
        for (row_number, fields) in [
            vec!["7", "same"],
            vec!["", "same"],
            vec!["9", "variable-width-value"],
        ]
        .into_iter()
        .enumerate()
        {
            batch
                .push_record(
                    &csv::ByteRecord::from(fields),
                    row_number as u64 + 1,
                    &columns,
                )
                .unwrap();
        }
        let config = SbdfEncodingConfig::new(
            columns.clone(),
            &HashMap::from([
                ("value".to_string(), ValueType::Long),
                ("label".to_string(), ValueType::String),
            ]),
            true,
            true,
        )
        .unwrap();
        let mut staged = Vec::new();
        super::encode_native_csv_batch_into(
            &mut staged,
            &columns,
            &config.column_types,
            config.encoding_strategy(),
            &batch,
        )
        .unwrap();
        let plan = plan_native_csv_batch(&batch, &config).unwrap();
        assert_eq!(plan.byte_len(), staged.len());

        let mut direct = Vec::with_capacity(plan.byte_len());
        super::encode_planned_native_csv_batch(&mut direct, &batch, &config, &plan).unwrap();
        assert_eq!(direct, staged);
    }

    #[test]
    fn single_row_group_adaptive_path_matches_direct_output() {
        let directory = TestDirectory::create("single-row-group");
        let parquet_path = directory.0.join("input.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        let mut parquet_writer =
            ArrowWriter::try_new(File::create(&parquet_path).unwrap(), schema, None).unwrap();
        parquet_writer.write(&batch).unwrap();
        parquet_writer.close().unwrap();

        let direct = directory.0.join("direct.sbdf");
        parquet_files_to_sbdf_streaming_impl(
            vec![parquet_path.to_string_lossy().into_owned()],
            direct.to_string_lossy().into_owned(),
            5_000,
            None,
            true,
            false,
            1,
            false,
        )
        .unwrap();
        let adaptive = directory.0.join("adaptive.sbdf");
        parquet_files_to_sbdf_streaming_impl(
            vec![parquet_path.to_string_lossy().into_owned()],
            adaptive.to_string_lossy().into_owned(),
            5_000,
            None,
            true,
            false,
            3,
            true,
        )
        .unwrap();

        assert_eq!(fs::read(direct).unwrap(), fs::read(adaptive).unwrap());
        assert_eq!(
            fs::read_dir(&directory.0)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".work-"))
                .count(),
            0
        );
    }

    #[test]
    fn large_estimated_batch_disables_prefetch_budget() {
        let total_rows = 300_000;
        let total_uncompressed_bytes = 1_070_623_282;
        let default_bytes =
            estimated_batch_bytes(5_000, total_rows, total_uncompressed_bytes).unwrap();
        let capped_rows = cap_parquet_batch_size(50_000, total_rows, total_uncompressed_bytes);
        let capped_bytes =
            estimated_batch_bytes(capped_rows, total_rows, total_uncompressed_bytes).unwrap();

        assert!(default_bytes <= MAX_PREFETCH_BATCH_BYTES);
        assert!(capped_bytes > MAX_PREFETCH_BATCH_BYTES);
    }

    #[test]
    fn synchronous_batch_consumption_preserves_order() {
        let producer = (0..4).map(Ok::<_, String>);
        let mut consumed = Vec::new();

        consume_batches(
            producer,
            |value| {
                consumed.push(value);
                Ok(())
            },
            false,
        )
        .unwrap();

        assert_eq!(consumed, vec![0, 1, 2, 3]);
    }
}
