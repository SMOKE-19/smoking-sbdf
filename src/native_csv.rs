use crate::{
    checked_value_length, sbdf_millis_from_unix_days, sbdf_millis_from_unix_millis, ColumnBuffer,
    ValueType, TARGET_CSV_DECODED_BYTES,
};
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMillisecondType,
};
use arrow_cast::parse::Parser;
use csv::ByteRecord;
use std::ffi::c_int;

pub(crate) struct NativeCsvBatch {
    pub(crate) buffers: Vec<ColumnBuffer>,
    pub(crate) invalids: Vec<Vec<u8>>,
    has_invalid: Vec<bool>,
    column_types: Vec<ValueType>,
    rows: usize,
    decoded_bytes: usize,
}

impl NativeCsvBatch {
    pub(crate) fn new(column_types: Vec<ValueType>, row_capacity: usize) -> Self {
        let buffers = column_types
            .iter()
            .map(|value_type| match value_type {
                ValueType::Bool => ColumnBuffer::Bool(Vec::with_capacity(row_capacity)),
                ValueType::Int => ColumnBuffer::Int(Vec::with_capacity(row_capacity)),
                ValueType::Long => ColumnBuffer::Long(Vec::with_capacity(row_capacity)),
                ValueType::Float => ColumnBuffer::Float(Vec::with_capacity(row_capacity)),
                ValueType::Double => ColumnBuffer::Double(Vec::with_capacity(row_capacity)),
                ValueType::DateTime | ValueType::Date | ValueType::Time | ValueType::TimeSpan => {
                    ColumnBuffer::TimeLike(Vec::with_capacity(row_capacity))
                }
                ValueType::String => ColumnBuffer::StringArena {
                    values: Vec::new(),
                    offsets: Vec::with_capacity(row_capacity),
                    lengths: Vec::with_capacity(row_capacity),
                },
                ValueType::Binary => ColumnBuffer::Binary {
                    _values: Vec::with_capacity(row_capacity),
                    ptrs: Vec::with_capacity(row_capacity),
                    lengths: Vec::with_capacity(row_capacity),
                },
            })
            .collect::<Vec<_>>();
        let column_count = column_types.len();
        Self {
            buffers,
            invalids: (0..column_count)
                .map(|_| Vec::with_capacity(row_capacity))
                .collect(),
            has_invalid: vec![false; column_count],
            column_types,
            rows: 0,
            decoded_bytes: 0,
        }
    }

    pub(crate) fn push_record(
        &mut self,
        record: &ByteRecord,
        row_number: u64,
        column_names: &[String],
    ) -> Result<(), String> {
        if record.len() != self.buffers.len() {
            return Err(format!(
                "CSV row {row_number} has {} fields; expected {}",
                record.len(),
                self.buffers.len()
            ));
        }

        for (index, column_name) in column_names.iter().enumerate().take(self.buffers.len()) {
            let field = record.get(index).unwrap_or_default();
            let invalid = field.is_empty();
            self.invalids[index].push(u8::from(invalid));
            self.has_invalid[index] |= invalid;
            self.decoded_bytes = self.decoded_bytes.saturating_add(1);
            append_field(
                &mut self.buffers[index],
                self.column_types[index],
                field,
                invalid,
                row_number,
                column_name,
                &mut self.decoded_bytes,
            )?;
        }
        self.rows += 1;
        Ok(())
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    pub(crate) fn decoded_bytes(&self) -> usize {
        self.decoded_bytes
    }

    pub(crate) fn reached_limit(&self, requested_rows: usize) -> bool {
        self.rows >= requested_rows || self.decoded_bytes >= TARGET_CSV_DECODED_BYTES
    }

    pub(crate) fn invalids(&self, index: usize) -> Option<&[u8]> {
        self.has_invalid[index].then_some(self.invalids[index].as_slice())
    }

    pub(crate) fn clear_retain_capacity(&mut self) {
        for buffer in &mut self.buffers {
            buffer.clear_retain_capacity();
        }
        for invalids in &mut self.invalids {
            invalids.clear();
        }
        self.has_invalid.fill(false);
        self.rows = 0;
        self.decoded_bytes = 0;
    }
}

fn parse_text<'a>(field: &'a [u8], row_number: u64, column_name: &str) -> Result<&'a str, String> {
    std::str::from_utf8(field).map_err(|error| {
        format!("invalid UTF-8 in CSV row {row_number}, column '{column_name}': {error}")
    })
}

fn parse_value<T: Parser>(
    field: &[u8],
    row_number: u64,
    column_name: &str,
    type_name: &str,
) -> Result<T::Native, String> {
    let text = parse_text(field, row_number, column_name)?;
    T::parse(text).ok_or_else(|| {
        format!(
            "failed to parse CSV row {row_number}, column '{column_name}' value '{text}' as {type_name}"
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn append_field(
    buffer: &mut ColumnBuffer,
    value_type: ValueType,
    field: &[u8],
    invalid: bool,
    row_number: u64,
    column_name: &str,
    decoded_bytes: &mut usize,
) -> Result<(), String> {
    match (value_type, buffer) {
        (ValueType::Bool, ColumnBuffer::Bool(values)) => {
            let value = if invalid {
                false
            } else {
                let text = parse_text(field, row_number, column_name)?;
                if text.eq_ignore_ascii_case("true") {
                    true
                } else if text.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(format!(
                        "failed to parse CSV row {row_number}, column '{column_name}' value '{text}' as Boolean"
                    ));
                }
            };
            values.push(u8::from(value));
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<u8>());
        }
        (ValueType::Int, ColumnBuffer::Int(values)) => {
            values.push(if invalid {
                0
            } else {
                parse_value::<Int32Type>(field, row_number, column_name, "Integer")?
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<i32>());
        }
        (ValueType::Long, ColumnBuffer::Long(values)) => {
            values.push(if invalid {
                0
            } else {
                parse_value::<Int64Type>(field, row_number, column_name, "LongInteger")?
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<i64>());
        }
        (ValueType::Float, ColumnBuffer::Float(values)) => {
            values.push(if invalid {
                0.0
            } else {
                parse_value::<Float32Type>(field, row_number, column_name, "SingleReal")?
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<f32>());
        }
        (ValueType::Double, ColumnBuffer::Double(values)) => {
            values.push(if invalid {
                0.0
            } else {
                parse_value::<Float64Type>(field, row_number, column_name, "Real")?
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<f64>());
        }
        (ValueType::DateTime, ColumnBuffer::TimeLike(values)) => {
            values.push(if invalid {
                0
            } else {
                sbdf_millis_from_unix_millis(parse_value::<TimestampMillisecondType>(
                    field,
                    row_number,
                    column_name,
                    "DateTime",
                )?)
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<i64>());
        }
        (ValueType::Date, ColumnBuffer::TimeLike(values)) => {
            values.push(if invalid {
                0
            } else {
                sbdf_millis_from_unix_days(parse_value::<Date32Type>(
                    field,
                    row_number,
                    column_name,
                    "Date",
                )? as i64)
            });
            *decoded_bytes = decoded_bytes.saturating_add(std::mem::size_of::<i64>());
        }
        (
            ValueType::String,
            ColumnBuffer::StringArena {
                values,
                offsets,
                lengths,
            },
        ) => {
            parse_text(field, row_number, column_name)?;
            let length = checked_value_length(column_name, field.len())
                .map_err(|error| error.to_string())?;
            offsets.push(values.len());
            lengths.push(length);
            values.extend_from_slice(field);
            *decoded_bytes = decoded_bytes
                .saturating_add(field.len())
                .saturating_add(std::mem::size_of::<usize>() + std::mem::size_of::<c_int>());
        }
        (ValueType::Time | ValueType::TimeSpan | ValueType::Binary, _) => {
            return Err(format!(
                "CSV direct parsing does not support SBDF type '{}' for column '{column_name}'",
                value_type.spotfire_name()
            ));
        }
        _ => {
            return Err(format!(
                "internal CSV buffer type mismatch for column '{column_name}'"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::NativeCsvBatch;
    use crate::{ColumnBuffer, ValueType, UNIX_EPOCH_MILLIS_FROM_YEAR_ONE};
    use csv::ByteRecord;

    #[test]
    fn typed_batch_parses_rules_nulls_and_strings() {
        let mut batch = NativeCsvBatch::new(
            vec![ValueType::Long, ValueType::DateTime, ValueType::String],
            2,
        );
        let columns = vec!["wafer_id".into(), "event_time".into(), "text".into()];
        batch
            .push_record(
                &ByteRecord::from(vec!["7", "1970-01-01T00:00:01", "hello"]),
                2,
                &columns,
            )
            .unwrap();
        batch
            .push_record(&ByteRecord::from(vec!["", "", ""]), 3, &columns)
            .unwrap();

        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.invalids(0), Some([0, 1].as_slice()));
        match &batch.buffers[0] {
            ColumnBuffer::Long(values) => assert_eq!(values, &[7, 0]),
            _ => panic!("unexpected wafer_id buffer"),
        }
        match &batch.buffers[1] {
            ColumnBuffer::TimeLike(values) => {
                assert_eq!(values, &[UNIX_EPOCH_MILLIS_FROM_YEAR_ONE + 1_000, 0]);
            }
            _ => panic!("unexpected event_time buffer"),
        }
        match &batch.buffers[2] {
            ColumnBuffer::StringArena {
                values,
                offsets,
                lengths,
            } => {
                assert_eq!(values, b"hello");
                assert_eq!(offsets, &[0, 5]);
                assert_eq!(lengths, &[5, 0]);
            }
            _ => panic!("unexpected text buffer"),
        }
    }

    #[test]
    fn clear_retains_allocations_for_next_batch() {
        let mut batch = NativeCsvBatch::new(vec![ValueType::String], 2);
        batch
            .push_record(&ByteRecord::from(vec!["reusable"]), 1, &["text".into()])
            .unwrap();
        let capacity = match &batch.buffers[0] {
            ColumnBuffer::StringArena { values, .. } => values.capacity(),
            _ => 0,
        };

        batch.clear_retain_capacity();

        assert_eq!(batch.rows(), 0);
        match &batch.buffers[0] {
            ColumnBuffer::StringArena { values, .. } => {
                assert!(values.is_empty());
                assert_eq!(values.capacity(), capacity);
            }
            _ => panic!("unexpected text buffer"),
        }
    }
}
