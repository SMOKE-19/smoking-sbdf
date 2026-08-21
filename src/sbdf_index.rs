use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

const MAGIC: [u8; 2] = [0xdf, 0x5b];
const FILE_HEADER: u8 = 0x01;
const TABLE_METADATA: u8 = 0x02;
const TABLE_SLICE: u8 = 0x03;
const COLUMN_SLICE: u8 = 0x04;
const TABLE_END: u8 = 0x05;
const PLAIN: u8 = 0x01;
const RLE: u8 = 0x02;
const BITS: u8 = 0x03;
const END_MARKER_BYTES: u64 = 3;

const TABLE_ID_COLUMN: &str = "__sbdf_table_id";
const FILE_COLUMN: &str = "__sbdf_file";
const SLICE_ID_COLUMN: &str = "__sbdf_slice_id";
const ROW_INDEX_COLUMN: &str = "__sbdf_row_index";
const ROW_IN_SLICE_COLUMN: &str = "__sbdf_row_in_slice";
const SLICE_ROW_START_COLUMN: &str = "__sbdf_slice_row_start";
const SLICE_ROW_COUNT_COLUMN: &str = "__sbdf_slice_row_count";
const BYTE_OFFSET_COLUMN: &str = "__sbdf_byte_offset";
const BYTE_LENGTH_COLUMN: &str = "__sbdf_byte_length";

const COORDINATE_COLUMNS: [&str; 9] = [
    TABLE_ID_COLUMN,
    FILE_COLUMN,
    SLICE_ID_COLUMN,
    ROW_INDEX_COLUMN,
    ROW_IN_SLICE_COLUMN,
    SLICE_ROW_START_COLUMN,
    SLICE_ROW_COUNT_COLUMN,
    BYTE_OFFSET_COLUMN,
    BYTE_LENGTH_COLUMN,
];

#[derive(Debug)]
struct TableSchema {
    columns: Vec<String>,
    type_ids: Vec<u8>,
    preamble_bytes: u64,
}

#[derive(Clone, Debug)]
enum ScalarValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
}

struct DecodedArray {
    count: usize,
    values: Option<Vec<Option<ScalarValue>>>,
}

pub(crate) fn generate_sidecar(
    sbdf_path: &Path,
    sidecar_path: &Path,
    table_id: &str,
    row_key_columns: &[String],
) -> Result<(), String> {
    validate_request(sbdf_path, sidecar_path, table_id, row_key_columns)?;
    if let Some(parent) = sidecar_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create sidecar directory '{}': {error}",
                parent.display()
            )
        })?;
    }
    let input = File::open(sbdf_path).map_err(|error| {
        format!(
            "failed to open SBDF file '{}': {error}",
            sbdf_path.display()
        )
    })?;
    let file_size = input
        .metadata()
        .map_err(|error| {
            format!(
                "failed to stat SBDF file '{}': {error}",
                sbdf_path.display()
            )
        })?
        .len();
    let mut input = BufReader::new(input);
    let table_schema = read_preamble(&mut input)?;
    let key_indexes = resolve_key_indexes(&table_schema, row_key_columns)?;
    let sidecar_schema = sidecar_schema(
        sbdf_path,
        file_size,
        table_id,
        row_key_columns,
        &key_indexes,
        &table_schema,
    )?;

    let partial_path = partial_path(sidecar_path);
    let output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&partial_path)
        .map_err(|error| {
            format!(
                "failed to create sidecar output '{}': {error}",
                partial_path.display()
            )
        })?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(usize::MAX))
        .build();
    let mut writer = ArrowWriter::try_new(output, Arc::clone(&sidecar_schema), Some(properties))
        .map_err(|error| format!("failed to initialize Parquet sidecar writer: {error}"))?;
    let result = write_sidecar_rows(
        &mut input,
        &mut writer,
        file_size,
        table_id,
        row_key_columns,
        &key_indexes,
        &table_schema,
        sidecar_schema,
    )
    .and_then(|()| {
        writer
            .close()
            .map(|_| ())
            .map_err(|error| format!("failed to close Parquet sidecar: {error}"))
    });
    if let Err(error) = result {
        let _ = fs::remove_file(&partial_path);
        return Err(error);
    }
    publish_partial(&partial_path, sidecar_path)
}

fn validate_request(
    sbdf_path: &Path,
    sidecar_path: &Path,
    table_id: &str,
    row_key_columns: &[String],
) -> Result<(), String> {
    if table_id.trim().is_empty() {
        return Err("table_id must not be empty".to_string());
    }
    if row_key_columns.is_empty() {
        return Err("row_key_columns must not be empty".to_string());
    }
    if sbdf_path == sidecar_path {
        return Err("sidecar_path must differ from sbdf_path".to_string());
    }
    if row_key_columns.iter().collect::<HashSet<_>>().len() != row_key_columns.len() {
        return Err("row_key_columns must not contain duplicates".to_string());
    }
    if let Some(column) = row_key_columns
        .iter()
        .find(|column| COORDINATE_COLUMNS.contains(&column.as_str()))
    {
        return Err(format!(
            "row key column '{column}' collides with a reserved sidecar column"
        ));
    }
    Ok(())
}

fn resolve_key_indexes(
    schema: &TableSchema,
    row_key_columns: &[String],
) -> Result<Vec<usize>, String> {
    let lookup = schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect::<HashMap<_, _>>();
    row_key_columns
        .iter()
        .map(|name| {
            lookup
                .get(name.as_str())
                .copied()
                .ok_or_else(|| format!("row key column '{name}' is not present in the SBDF schema"))
        })
        .collect()
}

fn sidecar_schema(
    sbdf_path: &Path,
    file_size: u64,
    table_id: &str,
    row_key_columns: &[String],
    key_indexes: &[usize],
    table_schema: &TableSchema,
) -> Result<SchemaRef, String> {
    let mut fields = Vec::with_capacity(row_key_columns.len() + COORDINATE_COLUMNS.len());
    for (name, index) in row_key_columns.iter().zip(key_indexes) {
        let type_id = table_schema.type_ids[*index];
        let mut metadata = HashMap::new();
        metadata.insert("smoking_sbdf.type_id".to_string(), type_id.to_string());
        metadata.insert(
            "smoking_sbdf.type_name".to_string(),
            sbdf_type_name(type_id)?.to_string(),
        );
        fields.push(Field::new(name, arrow_type(type_id)?, true).with_metadata(metadata));
    }
    fields.extend([
        Field::new(TABLE_ID_COLUMN, DataType::Utf8, false),
        Field::new(FILE_COLUMN, DataType::Utf8, false),
        Field::new(SLICE_ID_COLUMN, DataType::Int64, false),
        Field::new(ROW_INDEX_COLUMN, DataType::Int64, false),
        Field::new(ROW_IN_SLICE_COLUMN, DataType::Int32, false),
        Field::new(SLICE_ROW_START_COLUMN, DataType::Int64, false),
        Field::new(SLICE_ROW_COUNT_COLUMN, DataType::Int32, false),
        Field::new(BYTE_OFFSET_COLUMN, DataType::Int64, false),
        Field::new(BYTE_LENGTH_COLUMN, DataType::Int64, false),
    ]);
    let mut metadata = HashMap::new();
    metadata.insert("smoking_sbdf.sidecar.version".to_string(), "1".to_string());
    metadata.insert("smoking_sbdf.table_id".to_string(), table_id.to_string());
    metadata.insert(
        "smoking_sbdf.file".to_string(),
        sbdf_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );
    metadata.insert("smoking_sbdf.file_size".to_string(), file_size.to_string());
    metadata.insert(
        "smoking_sbdf.preamble_bytes".to_string(),
        table_schema.preamble_bytes.to_string(),
    );
    metadata.insert(
        "smoking_sbdf.end_marker_offset".to_string(),
        file_size
            .checked_sub(END_MARKER_BYTES)
            .ok_or_else(|| "SBDF file is shorter than its end marker".to_string())?
            .to_string(),
    );
    metadata.insert(
        "smoking_sbdf.row_key_count".to_string(),
        row_key_columns.len().to_string(),
    );
    for (index, name) in row_key_columns.iter().enumerate() {
        metadata.insert(format!("smoking_sbdf.row_key.{index}"), name.clone());
    }
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

#[allow(clippy::too_many_arguments)]
fn write_sidecar_rows<R: Read + Seek, W: std::io::Write + Send>(
    input: &mut R,
    writer: &mut ArrowWriter<W>,
    file_size: u64,
    table_id: &str,
    row_key_columns: &[String],
    key_indexes: &[usize],
    table_schema: &TableSchema,
    sidecar_schema: SchemaRef,
) -> Result<(), String> {
    let key_set = key_indexes.iter().copied().collect::<HashSet<_>>();
    let sbdf_file = table_schema_file_name(sidecar_schema.as_ref());
    let mut slice_id = 0i64;
    let mut row_start = 0i64;
    loop {
        let slice_offset = input
            .stream_position()
            .map_err(|error| format!("failed to determine SBDF slice position: {error}"))?;
        let section = read_section(input)?;
        if section == TABLE_END {
            if slice_offset + END_MARKER_BYTES != file_size {
                return Err("SBDF end marker is not at the end of the file".to_string());
            }
            return Ok(());
        }
        if section != TABLE_SLICE {
            return Err(format!(
                "expected SBDF table slice at byte {slice_offset}, found section {section:#04x}"
            ));
        }
        let column_count = read_count(input, "table slice column count")?;
        if column_count != table_schema.columns.len() {
            return Err(format!(
                "table slice {slice_id} has {column_count} columns; expected {}",
                table_schema.columns.len()
            ));
        }
        let mut row_count = None;
        let mut key_values = HashMap::<usize, Vec<Option<ScalarValue>>>::new();
        for column_index in 0..column_count {
            let position = input
                .stream_position()
                .map_err(|error| format!("failed to determine SBDF column position: {error}"))?;
            if read_section(input)? != COLUMN_SLICE {
                return Err(format!(
                    "expected SBDF column slice at byte {position} in table slice {slice_id}"
                ));
            }
            let selected = key_set.contains(&column_index);
            let mut values = read_array(input, selected)?;
            let value_count = values.count;
            if row_count
                .replace(value_count)
                .is_some_and(|count| count != value_count)
            {
                return Err(format!(
                    "column row count mismatch in table slice {slice_id}"
                ));
            }
            let property_count = read_count(input, "column property count")?;
            for _ in 0..property_count {
                let name = read_length_prefixed(input, "column property name")?;
                let invalid = read_array(input, selected && name == b"IsInvalid")?;
                if selected && name == b"IsInvalid" {
                    apply_invalids(
                        values
                            .values
                            .as_mut()
                            .ok_or_else(|| "missing decoded row key values".to_string())?,
                        invalid
                            .values
                            .ok_or_else(|| "missing decoded IsInvalid values".to_string())?,
                        slice_id,
                    )?;
                }
            }
            if selected {
                key_values.insert(
                    column_index,
                    values
                        .values
                        .ok_or_else(|| "missing decoded row key values".to_string())?,
                );
            }
        }
        let row_count = row_count.unwrap_or(0);
        let slice_end = input
            .stream_position()
            .map_err(|error| format!("failed to determine SBDF slice end: {error}"))?;
        let batch = sidecar_batch(
            Arc::clone(&sidecar_schema),
            table_id,
            &sbdf_file,
            row_key_columns,
            key_indexes,
            &table_schema.type_ids,
            key_values,
            slice_id,
            row_start,
            row_count,
            slice_offset,
            slice_end - slice_offset,
        )?;
        writer.write(&batch).map_err(|error| {
            format!("failed to write Parquet sidecar slice {slice_id}: {error}")
        })?;
        writer.flush().map_err(|error| {
            format!("failed to flush Parquet sidecar row group {slice_id}: {error}")
        })?;
        row_start = row_start
            .checked_add(
                i64::try_from(row_count)
                    .map_err(|_| "SBDF sidecar row count exceeds i64".to_string())?,
            )
            .ok_or_else(|| "SBDF sidecar row count overflow".to_string())?;
        slice_id = slice_id
            .checked_add(1)
            .ok_or_else(|| "SBDF sidecar slice count overflow".to_string())?;
    }
}

#[allow(clippy::too_many_arguments)]
fn sidecar_batch(
    schema: SchemaRef,
    table_id: &str,
    sbdf_file: &str,
    row_key_columns: &[String],
    key_indexes: &[usize],
    type_ids: &[u8],
    mut key_values: HashMap<usize, Vec<Option<ScalarValue>>>,
    slice_id: i64,
    row_start: i64,
    row_count: usize,
    byte_offset: u64,
    byte_length: u64,
) -> Result<RecordBatch, String> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for (name, index) in row_key_columns.iter().zip(key_indexes) {
        arrays.push(values_to_array(
            name,
            type_ids[*index],
            key_values.remove(index).ok_or_else(|| {
                format!("missing decoded row key column '{name}' in slice {slice_id}")
            })?,
        )?);
    }
    let row_count_i32 = i32::try_from(row_count)
        .map_err(|_| "SBDF sidecar slice row count exceeds i32".to_string())?;
    let offset_i64 = i64::try_from(byte_offset)
        .map_err(|_| "SBDF sidecar byte offset exceeds i64".to_string())?;
    let length_i64 = i64::try_from(byte_length)
        .map_err(|_| "SBDF sidecar byte length exceeds i64".to_string())?;
    let row_indexes = (0..row_count)
        .map(|index| {
            row_start
                .checked_add(index as i64)
                .ok_or_else(|| "SBDF sidecar row index overflow".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_in_slice = (0..row_count)
        .map(|index| {
            i32::try_from(index).map_err(|_| "SBDF sidecar row-in-slice exceeds i32".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    arrays.extend([
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            table_id, row_count,
        ))) as ArrayRef,
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            sbdf_file, row_count,
        ))) as ArrayRef,
        Arc::new(Int64Array::from_value(slice_id, row_count)) as ArrayRef,
        Arc::new(Int64Array::from(row_indexes)) as ArrayRef,
        Arc::new(Int32Array::from(row_in_slice)) as ArrayRef,
        Arc::new(Int64Array::from_value(row_start, row_count)) as ArrayRef,
        Arc::new(Int32Array::from_value(row_count_i32, row_count)) as ArrayRef,
        Arc::new(Int64Array::from_value(offset_i64, row_count)) as ArrayRef,
        Arc::new(Int64Array::from_value(length_i64, row_count)) as ArrayRef,
    ]);
    RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("failed to build Parquet sidecar batch: {error}"))
}

fn values_to_array(
    column_name: &str,
    type_id: u8,
    values: Vec<Option<ScalarValue>>,
) -> Result<ArrayRef, String> {
    macro_rules! primitive_array {
        ($variant:ident, $array:ty) => {{
            let typed = values
                .into_iter()
                .map(|value| match value {
                    Some(ScalarValue::$variant(value)) => Ok(Some(value)),
                    None => Ok(None),
                    _ => Err(format!(
                        "decoded row key type mismatch for column '{column_name}'"
                    )),
                })
                .collect::<Result<Vec<_>, String>>()?;
            Arc::new(<$array>::from(typed)) as ArrayRef
        }};
    }
    let array = match type_id {
        0x01 => primitive_array!(Bool, BooleanArray),
        0x02 => primitive_array!(Int32, Int32Array),
        0x03 | 0x06..=0x09 => primitive_array!(Int64, Int64Array),
        0x04 => primitive_array!(Float32, Float32Array),
        0x05 => primitive_array!(Float64, Float64Array),
        0x0a => {
            let typed = values
                .into_iter()
                .map(|value| match value {
                    Some(ScalarValue::String(value)) => Ok(Some(value)),
                    None => Ok(None),
                    _ => Err(format!(
                        "decoded row key type mismatch for column '{column_name}'"
                    )),
                })
                .collect::<Result<Vec<_>, String>>()?;
            Arc::new(StringArray::from(typed))
        }
        0x0c => {
            let typed = values
                .iter()
                .map(|value| match value {
                    Some(ScalarValue::Binary(value)) => Ok(Some(value.as_slice())),
                    None => Ok(None),
                    _ => Err(format!(
                        "decoded row key type mismatch for column '{column_name}'"
                    )),
                })
                .collect::<Result<Vec<_>, String>>()?;
            Arc::new(BinaryArray::from(typed))
        }
        other => return Err(format!("unsupported SBDF row key type {other:#04x}")),
    };
    Ok(array)
}

fn apply_invalids(
    values: &mut [Option<ScalarValue>],
    markers: Vec<Option<ScalarValue>>,
    slice_id: i64,
) -> Result<(), String> {
    if markers.len() != values.len() {
        return Err(format!(
            "IsInvalid length mismatch in table slice {slice_id}"
        ));
    }
    for (value, marker) in values.iter_mut().zip(markers) {
        match marker {
            Some(ScalarValue::Bool(true)) => *value = None,
            Some(ScalarValue::Bool(false)) => {}
            _ => return Err(format!("invalid IsInvalid value in table slice {slice_id}")),
        }
    }
    Ok(())
}

fn read_array<R: Read>(input: &mut R, decode: bool) -> Result<DecodedArray, String> {
    let encoding = read_u8(input, "array encoding")?;
    let type_id = read_u8(input, "array type")?;
    let count = read_count(input, "array value count")?;
    match encoding {
        PLAIN => Ok(DecodedArray {
            count,
            values: read_values(input, type_id, count, decode)?,
        }),
        RLE => {
            let run_count = read_count(input, "RLE run count")?;
            let lengths = if decode {
                let mut lengths = vec![0u8; run_count];
                input
                    .read_exact(&mut lengths)
                    .map_err(|error| format!("failed to read RLE run lengths: {error}"))?;
                Some(lengths)
            } else {
                discard_exact(input, run_count, "RLE run lengths")?;
                None
            };
            let encoded_count = read_count(input, "RLE encoded value count")?;
            if encoded_count != run_count {
                return Err("SBDF RLE run and value counts differ".to_string());
            }
            let encoded = read_values(input, type_id, encoded_count, decode)?;
            let values = if let (Some(encoded), Some(lengths)) = (encoded, lengths) {
                let mut values = Vec::with_capacity(count);
                for (value, length) in encoded.into_iter().zip(lengths) {
                    values.extend(std::iter::repeat_n(value, usize::from(length) + 1));
                }
                if values.len() != count {
                    return Err("SBDF RLE decoded row count mismatch".to_string());
                }
                Some(values)
            } else {
                None
            };
            Ok(DecodedArray { count, values })
        }
        BITS => {
            let byte_count = count.div_ceil(8);
            if !decode {
                discard_exact(input, byte_count, "bit array")?;
                return Ok(DecodedArray {
                    count,
                    values: None,
                });
            }
            let mut bytes = vec![0u8; byte_count];
            input
                .read_exact(&mut bytes)
                .map_err(|error| format!("failed to read bit array: {error}"))?;
            let values = (0..count)
                .map(|index| {
                    let mask = 1u8 << (7 - index % 8);
                    Some(ScalarValue::Bool(bytes[index / 8] & mask != 0))
                })
                .collect();
            Ok(DecodedArray {
                count,
                values: Some(values),
            })
        }
        other => Err(format!("unsupported SBDF array encoding {other:#04x}")),
    }
}

fn read_values<R: Read>(
    input: &mut R,
    type_id: u8,
    count: usize,
    decode: bool,
) -> Result<Option<Vec<Option<ScalarValue>>>, String> {
    if matches!(type_id, 0x0a | 0x0c) {
        let packed_len = read_count(input, "packed variable array byte length")?;
        if !decode {
            discard_exact(input, packed_len, "packed variable array")?;
            return Ok(None);
        }
        let mut packed = vec![0u8; packed_len];
        input
            .read_exact(&mut packed)
            .map_err(|error| format!("failed to read packed variable array: {error}"))?;
        let mut cursor = std::io::Cursor::new(packed.as_slice());
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let length = read_7bit_count(&mut cursor)?;
            let mut bytes = vec![0u8; length];
            cursor
                .read_exact(&mut bytes)
                .map_err(|error| format!("failed to read variable SBDF value: {error}"))?;
            values.push(Some(if type_id == 0x0a {
                ScalarValue::String(String::from_utf8(bytes).map_err(|error| {
                    format!("row key contains invalid UTF-8 string data: {error}")
                })?)
            } else {
                ScalarValue::Binary(bytes)
            }));
        }
        if cursor.position() != packed_len as u64 {
            return Err("packed variable SBDF array contains trailing bytes".to_string());
        }
        return Ok(Some(values));
    }
    let width = primitive_width(type_id)?;
    let byte_len = count
        .checked_mul(width)
        .ok_or_else(|| "SBDF primitive array size overflow".to_string())?;
    if !decode {
        discard_exact(input, byte_len, "primitive SBDF values")?;
        return Ok(None);
    }
    let mut bytes = vec![0u8; byte_len];
    input
        .read_exact(&mut bytes)
        .map_err(|error| format!("failed to read primitive SBDF values: {error}"))?;
    bytes
        .chunks_exact(width)
        .map(|bytes| primitive_scalar(type_id, bytes).map(Some))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn primitive_scalar(type_id: u8, bytes: &[u8]) -> Result<ScalarValue, String> {
    match type_id {
        0x01 => Ok(ScalarValue::Bool(bytes[0] != 0)),
        0x02 => Ok(ScalarValue::Int32(i32::from_le_bytes(
            bytes.try_into().unwrap(),
        ))),
        0x03 | 0x06..=0x09 => Ok(ScalarValue::Int64(i64::from_le_bytes(
            bytes.try_into().unwrap(),
        ))),
        0x04 => Ok(ScalarValue::Float32(f32::from_le_bytes(
            bytes.try_into().unwrap(),
        ))),
        0x05 => Ok(ScalarValue::Float64(f64::from_le_bytes(
            bytes.try_into().unwrap(),
        ))),
        other => Err(format!("unsupported SBDF primitive type {other:#04x}")),
    }
}

fn read_preamble<R: Read + Seek>(input: &mut R) -> Result<TableSchema, String> {
    if read_section(input)? != FILE_HEADER {
        return Err("SBDF file does not start with a file header".to_string());
    }
    let mut version = [0u8; 2];
    input
        .read_exact(&mut version)
        .map_err(|error| format!("failed to read SBDF version: {error}"))?;
    if version != [1, 0] {
        return Err(format!(
            "unsupported SBDF version {}.{}",
            version[0], version[1]
        ));
    }
    if read_section(input)? != TABLE_METADATA {
        return Err("SBDF file header is not followed by table metadata".to_string());
    }
    if read_count(input, "table property count")? != 0 {
        return Err("SBDF table properties are not supported by the sidecar indexer".to_string());
    }
    let column_count = read_count(input, "metadata column count")?;
    let definition_count = read_count(input, "metadata definition count")?;
    let mut definitions = Vec::with_capacity(definition_count);
    for _ in 0..definition_count {
        let name = read_length_prefixed(input, "metadata definition name")?;
        let type_id = read_u8(input, "metadata definition type")?;
        if read_u8(input, "metadata definition default marker")? != 0 {
            return Err("metadata definition defaults are not supported".to_string());
        }
        definitions.push((name, type_id));
    }
    let mut columns = Vec::with_capacity(column_count);
    let mut type_ids = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let mut column_name = None;
        let mut column_type = None;
        for (name, type_id) in &definitions {
            if read_u8(input, "metadata value marker")? == 0 {
                continue;
            }
            let value = read_length_prefixed(input, "metadata value")?;
            if name == b"Name" && *type_id == 0x0a {
                column_name = Some(
                    String::from_utf8(value)
                        .map_err(|error| format!("SBDF column name is not UTF-8: {error}"))?,
                );
            } else if name == b"DataType" && *type_id == 0x0c {
                if value.len() != 1 {
                    return Err("SBDF DataType metadata must contain one byte".to_string());
                }
                column_type = Some(value[0]);
            }
        }
        columns.push(column_name.ok_or_else(|| "SBDF column has no Name metadata".to_string())?);
        type_ids
            .push(column_type.ok_or_else(|| "SBDF column has no DataType metadata".to_string())?);
    }
    let preamble_bytes = input
        .stream_position()
        .map_err(|error| format!("failed to determine SBDF preamble length: {error}"))?;
    Ok(TableSchema {
        columns,
        type_ids,
        preamble_bytes,
    })
}

fn read_section<R: Read>(input: &mut R) -> Result<u8, String> {
    let mut magic = [0u8; 2];
    input
        .read_exact(&mut magic)
        .map_err(|error| format!("failed to read SBDF section magic: {error}"))?;
    if magic != MAGIC {
        return Err("invalid SBDF section magic".to_string());
    }
    read_u8(input, "section id")
}

fn read_u8<R: Read>(input: &mut R, label: &str) -> Result<u8, String> {
    let mut byte = [0u8; 1];
    input
        .read_exact(&mut byte)
        .map_err(|error| format!("failed to read {label}: {error}"))?;
    Ok(byte[0])
}

fn read_count<R: Read>(input: &mut R, label: &str) -> Result<usize, String> {
    let mut bytes = [0u8; 4];
    input
        .read_exact(&mut bytes)
        .map_err(|error| format!("failed to read {label}: {error}"))?;
    usize::try_from(i32::from_le_bytes(bytes)).map_err(|_| format!("{label} is negative"))
}

fn read_length_prefixed<R: Read>(input: &mut R, label: &str) -> Result<Vec<u8>, String> {
    let length = read_count(input, label)?;
    let mut bytes = vec![0u8; length];
    input
        .read_exact(&mut bytes)
        .map_err(|error| format!("failed to read {label}: {error}"))?;
    Ok(bytes)
}

fn read_7bit_count<R: Read>(input: &mut R) -> Result<usize, String> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = read_u8(input, "packed integer")?;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(value).map_err(|_| "packed integer exceeds usize".to_string());
        }
    }
    Err("invalid packed SBDF integer".to_string())
}

fn discard_exact<R: Read>(input: &mut R, mut length: usize, label: &str) -> Result<(), String> {
    let mut buffer = [0u8; 16 * 1024];
    while length != 0 {
        let requested = length.min(buffer.len());
        input
            .read_exact(&mut buffer[..requested])
            .map_err(|error| format!("failed to skip {label}: {error}"))?;
        length -= requested;
    }
    Ok(())
}

fn primitive_width(type_id: u8) -> Result<usize, String> {
    match type_id {
        0x01 => Ok(1),
        0x02 | 0x04 => Ok(4),
        0x03 | 0x05..=0x09 => Ok(8),
        other => Err(format!("unsupported SBDF type {other:#04x}")),
    }
}

fn arrow_type(type_id: u8) -> Result<DataType, String> {
    match type_id {
        0x01 => Ok(DataType::Boolean),
        0x02 => Ok(DataType::Int32),
        0x03 | 0x06..=0x09 => Ok(DataType::Int64),
        0x04 => Ok(DataType::Float32),
        0x05 => Ok(DataType::Float64),
        0x0a => Ok(DataType::Utf8),
        0x0c => Ok(DataType::Binary),
        other => Err(format!("unsupported SBDF type {other:#04x}")),
    }
}

fn sbdf_type_name(type_id: u8) -> Result<&'static str, String> {
    match type_id {
        0x01 => Ok("Boolean"),
        0x02 => Ok("Integer"),
        0x03 => Ok("LongInteger"),
        0x04 => Ok("SingleReal"),
        0x05 => Ok("Real"),
        0x06 => Ok("DateTime"),
        0x07 => Ok("Date"),
        0x08 => Ok("Time"),
        0x09 => Ok("TimeSpan"),
        0x0a => Ok("String"),
        0x0c => Ok("Binary"),
        other => Err(format!("unsupported SBDF type {other:#04x}")),
    }
}

fn table_schema_file_name(schema: &Schema) -> String {
    schema
        .metadata()
        .get("smoking_sbdf.file")
        .cloned()
        .unwrap_or_default()
}

fn partial_path(target: &Path) -> PathBuf {
    let counter = crate::WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "sidecar.parquet".to_string());
    target.with_file_name(format!(
        ".{file_name}.{}.{}.partial",
        std::process::id(),
        counter
    ))
}

fn publish_partial(partial: &Path, target: &Path) -> Result<(), String> {
    match fs::rename(partial, target) {
        Ok(()) => Ok(()),
        Err(first_error) if target.exists() => {
            fs::remove_file(target).map_err(|error| {
                format!(
                    "failed to replace sidecar output '{}': {error}",
                    target.display()
                )
            })?;
            fs::rename(partial, target).map_err(|error| {
                format!(
                    "failed to publish sidecar output '{}' after replace fallback \
                     (initial error: {first_error}): {error}",
                    target.display()
                )
            })
        }
        Err(error) => Err(format!(
            "failed to publish sidecar output '{}' from '{}': {error}",
            target.display(),
            partial.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{generate_sidecar, BYTE_LENGTH_COLUMN, BYTE_OFFSET_COLUMN, SLICE_ID_COLUMN};
    use crate::rust_sbdf::{self, EncodingStrategy, ValueView};
    use arrow_array::{Array, Int64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::ffi::c_int;
    use std::fs::{self, File};
    use std::path::Path;
    use std::sync::atomic::Ordering;

    fn primitive_i64(values: &[i64]) -> ValueView<'_> {
        let bytes = unsafe {
            std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
        };
        ValueView::Primitive {
            bytes,
            count: values.len(),
            width: std::mem::size_of::<i64>(),
        }
    }

    fn append_slice(output: &mut Vec<u8>, devices: &[&str], ids: &[i64], invalids: Option<&[u8]>) {
        rust_sbdf::begin_table_slice(output, 2).unwrap();
        let arena = devices.concat().into_bytes();
        let mut offset = 0usize;
        let mut offsets = Vec::with_capacity(devices.len());
        let mut lengths = Vec::<c_int>::with_capacity(devices.len());
        for value in devices {
            offsets.push(offset);
            lengths.push(value.len() as c_int);
            offset += value.len();
        }
        rust_sbdf::encode_column_slice(
            output,
            0x0a,
            ValueView::Arena {
                bytes: &arena,
                offsets: &offsets,
                lengths: &lengths,
            },
            EncodingStrategy::Rle,
            invalids,
        )
        .unwrap();
        rust_sbdf::encode_column_slice(
            output,
            0x03,
            primitive_i64(ids),
            EncodingStrategy::Rle,
            None,
        )
        .unwrap();
    }

    #[test]
    fn parquet_sidecar_preserves_keys_coordinates_and_slice_row_groups() {
        let sequence = crate::WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "smoking-sbdf-sidecar-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let sbdf_path = root.join("events.sbdf");
        let sidecar_path = root.join("events.sbdf.sidecar.parquet");
        let mut sbdf = Vec::new();
        rust_sbdf::write_preamble(
            &mut sbdf,
            &["device_key".to_string(), "event_id".to_string()],
            &[0x0a, 0x03],
        )
        .unwrap();
        append_slice(&mut sbdf, &["A", "B"], &[1, 2], None);
        append_slice(&mut sbdf, &[""], &[3], Some(&[1]));
        rust_sbdf::write_end_marker(&mut sbdf).unwrap();
        fs::write(&sbdf_path, &sbdf).unwrap();

        generate_sidecar(
            &sbdf_path,
            &sidecar_path,
            "fab.events",
            &["device_key".to_string(), "event_id".to_string()],
        )
        .unwrap();

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&sidecar_path).unwrap()).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 2);
        let schema = builder.schema().clone();
        assert_eq!(
            schema.metadata().get("smoking_sbdf.table_id"),
            Some(&"fab.events".to_string())
        );
        assert_eq!(
            schema.metadata().get("smoking_sbdf.row_key.0"),
            Some(&"device_key".to_string())
        );
        let batches = builder
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let devices = batches[0]
            .column_by_name("device_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(devices.value(0), "A");
        assert_eq!(devices.value(1), "B");
        assert!(devices.is_null(2));
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("event_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, [1, 2, 3]);
        let batch = &batches[0];
        let slice_ids = batch
            .column_by_name(SLICE_ID_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(slice_ids.values(), &[0, 0, 1]);
        let offsets = batch
            .column_by_name(BYTE_OFFSET_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lengths = batch
            .column_by_name(BYTE_LENGTH_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in [0, 2] {
            let offset = offsets.value(row) as usize;
            assert_eq!(&sbdf[offset..offset + 3], &[0xdf, 0x5b, 0x03]);
            assert!(lengths.value(row) > 0);
        }
        assert_eq!(&fs::read(&sidecar_path).unwrap()[..4], b"PAR1");
        let _ = fs::remove_dir_all(Path::new(&root));
    }
}
