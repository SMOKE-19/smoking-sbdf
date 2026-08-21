#![cfg_attr(not(any(test, feature = "planned-offset-prototype")), allow(dead_code))]

// This SBDF wire-format implementation was developed with reference to
// spotfiresoftware/spotfire-sbdf-c. The upstream copyright and BSD 3-Clause
// license are preserved in THIRD_PARTY_NOTICES.md.

use std::ffi::{c_char, c_int};
use std::io::Write;
use std::mem::size_of;

const TABLE_SLICE_SECTION: u8 = 0x03;
const COLUMN_SLICE_SECTION: u8 = 0x04;
const FILE_HEADER_SECTION: u8 = 0x01;
const TABLE_METADATA_SECTION: u8 = 0x02;
const TABLE_END_SECTION: u8 = 0x05;
const PLAIN_ENCODING: u8 = 0x01;
const RLE_ENCODING: u8 = 0x02;
const BIT_ENCODING: u8 = 0x03;
const BOOL_TYPE: u8 = 0x01;
const SECTION_MAGIC: [u8; 2] = [0xdf, 0x5b];
const IS_INVALID: &[u8] = b"IsInvalid";
const STRING_TYPE: u8 = 0x0a;
const BINARY_TYPE: u8 = 0x0c;
const COLUMN_NAME_METADATA: &[u8] = b"Name";
const COLUMN_TYPE_METADATA: &[u8] = b"DataType";

pub(crate) fn write_preamble<W: Write>(
    output: &mut W,
    columns: &[String],
    type_ids: &[u8],
) -> Result<(), String> {
    if columns.len() != type_ids.len() {
        return Err("SBDF columns and type ids must have the same length".to_string());
    }
    let mut written = 0usize;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[FILE_HEADER_SECTION, 1, 0], &mut written)?;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[TABLE_METADATA_SECTION], &mut written)?;
    write_sink(output, &0i32.to_le_bytes(), &mut written)?;
    write_sink(
        output,
        &checked_i32(columns.len(), "SBDF metadata column count")?.to_le_bytes(),
        &mut written,
    )?;
    write_sink(output, &2i32.to_le_bytes(), &mut written)?;
    write_metadata_definition(output, COLUMN_NAME_METADATA, STRING_TYPE, &mut written)?;
    write_metadata_definition(output, COLUMN_TYPE_METADATA, BINARY_TYPE, &mut written)?;
    for (column, type_id) in columns.iter().zip(type_ids) {
        write_sink(output, &[1], &mut written)?;
        write_length_prefixed(output, column.as_bytes(), &mut written)?;
        write_sink(output, &[1], &mut written)?;
        write_length_prefixed(output, &[*type_id], &mut written)?;
    }
    Ok(())
}

pub(crate) fn write_end_marker<W: Write>(output: &mut W) -> Result<(), String> {
    let mut written = 0usize;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[TABLE_END_SECTION], &mut written)?;
    Ok(())
}

fn write_metadata_definition<W: Write>(
    output: &mut W,
    name: &[u8],
    type_id: u8,
    written: &mut usize,
) -> Result<(), String> {
    write_length_prefixed(output, name, written)?;
    write_sink(output, &[type_id, 0], written)
}

fn write_length_prefixed<W: Write>(
    output: &mut W,
    value: &[u8],
    written: &mut usize,
) -> Result<(), String> {
    write_sink(
        output,
        &checked_i32(value.len(), "SBDF string length")?.to_le_bytes(),
        written,
    )?;
    write_sink(output, value, written)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EncodingStrategy {
    Plain,
    Rle,
    Adaptive,
}

#[derive(Clone, Copy)]
pub(crate) enum ValueView<'a> {
    Primitive {
        bytes: &'a [u8],
        count: usize,
        width: usize,
    },
    Pointers {
        pointers: &'a [*const c_char],
        lengths: &'a [c_int],
    },
    Arena {
        bytes: &'a [u8],
        offsets: &'a [usize],
        lengths: &'a [c_int],
    },
}

#[derive(Clone, Copy)]
pub(crate) struct ColumnInput<'a> {
    pub(crate) type_id: u8,
    pub(crate) values: ValueView<'a>,
    pub(crate) strategy: EncodingStrategy,
    pub(crate) invalids: Option<&'a [u8]>,
}

#[derive(Debug)]
enum PlannedEncoding {
    Plain,
    Rle {
        run_lengths: Vec<u8>,
        run_value_indexes: Vec<usize>,
    },
    Bits,
}

#[derive(Debug)]
struct ColumnLayout {
    value_count: usize,
    encoding: PlannedEncoding,
    has_invalids: bool,
}

#[derive(Debug)]
pub(crate) struct EncodedLayoutPlan {
    byte_len: usize,
    columns: Vec<ColumnLayout>,
}

impl EncodedLayoutPlan {
    pub(crate) fn byte_len(&self) -> usize {
        self.byte_len
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        let encoding_bytes = self.columns.iter().fold(0usize, |total, column| {
            let bytes = match &column.encoding {
                PlannedEncoding::Rle {
                    run_lengths,
                    run_value_indexes,
                } => run_lengths.capacity().saturating_add(
                    run_value_indexes
                        .capacity()
                        .saturating_mul(size_of::<usize>()),
                ),
                PlannedEncoding::Plain | PlannedEncoding::Bits => 0,
            };
            total.saturating_add(bytes)
        });
        size_of::<Self>()
            .saturating_add(
                self.columns
                    .capacity()
                    .saturating_mul(size_of::<ColumnLayout>()),
            )
            .saturating_add(encoding_bytes)
    }
}

impl<'a> ValueView<'a> {
    fn count(self) -> usize {
        match self {
            Self::Primitive { count, .. } => count,
            Self::Pointers { pointers, .. } => pointers.len(),
            Self::Arena { offsets, .. } => offsets.len(),
        }
    }

    fn validate(self) -> Result<(), String> {
        match self {
            Self::Primitive {
                bytes,
                count,
                width,
            } => {
                let expected = count
                    .checked_mul(width)
                    .ok_or_else(|| "SBDF primitive buffer size overflow".to_string())?;
                if width == 0 || bytes.len() != expected {
                    return Err("SBDF primitive buffer length mismatch".to_string());
                }
            }
            Self::Pointers { pointers, lengths } => {
                if pointers.len() != lengths.len() {
                    return Err("SBDF pointer/length array mismatch".to_string());
                }
                if lengths.iter().any(|length| *length < 0) {
                    return Err("SBDF variable value has a negative length".to_string());
                }
            }
            Self::Arena {
                bytes,
                offsets,
                lengths,
            } => {
                if offsets.len() != lengths.len() {
                    return Err("SBDF arena offset/length array mismatch".to_string());
                }
                for (&offset, &length) in offsets.iter().zip(lengths) {
                    let length = usize::try_from(length)
                        .map_err(|_| "SBDF arena value has a negative length".to_string())?;
                    if offset
                        .checked_add(length)
                        .is_none_or(|end| end > bytes.len())
                    {
                        return Err("SBDF arena value is outside its byte buffer".to_string());
                    }
                }
            }
        }
        checked_i32(self.count(), "SBDF value count")?;
        Ok(())
    }

    fn value(self, index: usize) -> Result<&'a [u8], String> {
        match self {
            Self::Primitive { bytes, width, .. } => {
                let start = index * width;
                Ok(&bytes[start..start + width])
            }
            Self::Pointers { pointers, lengths } => {
                let length = usize::try_from(lengths[index])
                    .map_err(|_| "SBDF variable value has a negative length".to_string())?;
                let pointer = pointers[index].cast::<u8>();
                if pointer.is_null() && length != 0 {
                    return Err("SBDF variable value has a null pointer".to_string());
                }
                if length == 0 {
                    return Ok(&[]);
                }
                // SAFETY: callers keep the source Arrow/native buffer alive for the entire
                // encode call and provide the validated byte length for every pointer.
                Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
            }
            Self::Arena {
                bytes,
                offsets,
                lengths,
            } => {
                let length = usize::try_from(lengths[index])
                    .map_err(|_| "SBDF arena value has a negative length".to_string())?;
                Ok(&bytes[offsets[index]..offsets[index] + length])
            }
        }
    }

    fn is_primitive(self) -> bool {
        matches!(self, Self::Primitive { .. })
    }
}

pub(crate) fn begin_table_slice(output: &mut Vec<u8>, column_count: usize) -> Result<(), String> {
    write_section(output, TABLE_SLICE_SECTION);
    write_i32(
        output,
        checked_i32(column_count, "SBDF table column count")?,
    );
    Ok(())
}

pub(crate) fn encode_column_slice(
    output: &mut Vec<u8>,
    type_id: u8,
    values: ValueView<'_>,
    strategy: EncodingStrategy,
    invalids: Option<&[u8]>,
) -> Result<(), String> {
    values.validate()?;
    if invalids.is_some_and(|markers| markers.len() != values.count()) {
        return Err("SBDF invalid marker count does not match values".to_string());
    }

    write_section(output, COLUMN_SLICE_SECTION);
    if type_id == BOOL_TYPE {
        encode_bits(output, type_id, values)?;
    } else {
        match strategy {
            EncodingStrategy::Rle if type_id != 0x0c => encode_rle(output, type_id, values)?,
            EncodingStrategy::Adaptive if type_id != 0x0c && rle_is_smaller(values)? => {
                encode_rle(output, type_id, values)?
            }
            _ => encode_plain(output, type_id, values)?,
        }
    }

    match invalids {
        Some(markers) => {
            write_i32(output, 1);
            write_i32(
                output,
                checked_i32(IS_INVALID.len(), "SBDF property name length")?,
            );
            output.extend_from_slice(IS_INVALID);
            let marker_view = ValueView::Primitive {
                bytes: markers,
                count: markers.len(),
                width: 1,
            };
            encode_bits(output, BOOL_TYPE, marker_view)?;
        }
        None => write_i32(output, 0),
    }
    Ok(())
}

pub(crate) fn begin_table_slice_to_sink<W: Write>(
    output: &mut W,
    column_count: usize,
) -> Result<usize, String> {
    let mut written = 0usize;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[TABLE_SLICE_SECTION], &mut written)?;
    write_sink(
        output,
        &checked_i32(column_count, "SBDF table column count")?.to_le_bytes(),
        &mut written,
    )?;
    Ok(written)
}

pub(crate) fn encode_column_slice_to_sink<W: Write>(
    output: &mut W,
    type_id: u8,
    values: ValueView<'_>,
    strategy: EncodingStrategy,
    invalids: Option<&[u8]>,
) -> Result<usize, String> {
    values.validate()?;
    if invalids.is_some_and(|markers| markers.len() != values.count()) {
        return Err("SBDF invalid marker count does not match values".to_string());
    }
    let mut written = 0usize;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[COLUMN_SLICE_SECTION], &mut written)?;
    let encoding = planned_encoding(type_id, values, strategy)?;
    encode_planned_values(output, type_id, values, &encoding, &mut written)?;
    match invalids {
        Some(markers) => {
            write_sink(output, &1i32.to_le_bytes(), &mut written)?;
            write_sink(
                output,
                &checked_i32(IS_INVALID.len(), "SBDF property name length")?.to_le_bytes(),
                &mut written,
            )?;
            write_sink(output, IS_INVALID, &mut written)?;
            encode_planned_values(
                output,
                BOOL_TYPE,
                ValueView::Primitive {
                    bytes: markers,
                    count: markers.len(),
                    width: 1,
                },
                &PlannedEncoding::Bits,
                &mut written,
            )?;
        }
        None => write_sink(output, &0i32.to_le_bytes(), &mut written)?,
    }
    Ok(written)
}

pub(crate) fn plan_table_slice(columns: &[ColumnInput<'_>]) -> Result<EncodedLayoutPlan, String> {
    checked_i32(columns.len(), "SBDF table column count")?;
    let mut byte_len = SECTION_MAGIC.len() + 1 + size_of::<i32>();
    let mut layouts = Vec::with_capacity(columns.len());
    for column in columns {
        column.values.validate()?;
        if column
            .invalids
            .is_some_and(|markers| markers.len() != column.values.count())
        {
            return Err("SBDF invalid marker count does not match values".to_string());
        }
        let encoding = planned_encoding(column.type_id, column.values, column.strategy)?;
        let encoded_values_len = planned_encoding_len(column.values, &encoding)?;
        let property_len = match column.invalids {
            Some(markers) => {
                size_of::<i32>() * 2 + IS_INVALID.len() + bit_encoding_len(markers.len())
            }
            None => size_of::<i32>(),
        };
        byte_len = byte_len
            .checked_add(SECTION_MAGIC.len() + 1)
            .and_then(|size| size.checked_add(encoded_values_len))
            .and_then(|size| size.checked_add(property_len))
            .ok_or_else(|| "SBDF table slice layout size overflow".to_string())?;
        layouts.push(ColumnLayout {
            value_count: column.values.count(),
            encoding,
            has_invalids: column.invalids.is_some(),
        });
    }
    Ok(EncodedLayoutPlan {
        byte_len,
        columns: layouts,
    })
}

pub(crate) fn encode_planned_table_slice<W: Write>(
    output: &mut W,
    columns: &[ColumnInput<'_>],
    plan: &EncodedLayoutPlan,
) -> Result<(), String> {
    if columns.len() != plan.columns.len() {
        return Err("SBDF layout column count changed before encode".to_string());
    }
    let mut written = 0usize;
    write_sink(output, &SECTION_MAGIC, &mut written)?;
    write_sink(output, &[TABLE_SLICE_SECTION], &mut written)?;
    write_sink(
        output,
        &checked_i32(columns.len(), "SBDF table column count")?.to_le_bytes(),
        &mut written,
    )?;
    for (column, layout) in columns.iter().zip(&plan.columns) {
        column.values.validate()?;
        if column.values.count() != layout.value_count
            || column.invalids.is_some() != layout.has_invalids
        {
            return Err("SBDF values changed after layout planning".to_string());
        }
        write_sink(output, &SECTION_MAGIC, &mut written)?;
        write_sink(output, &[COLUMN_SLICE_SECTION], &mut written)?;
        encode_planned_values(
            output,
            column.type_id,
            column.values,
            &layout.encoding,
            &mut written,
        )?;
        match column.invalids {
            Some(markers) => {
                if markers.len() != layout.value_count {
                    return Err("SBDF invalid markers changed after layout planning".to_string());
                }
                write_sink(output, &1i32.to_le_bytes(), &mut written)?;
                write_sink(
                    output,
                    &checked_i32(IS_INVALID.len(), "SBDF property name length")?.to_le_bytes(),
                    &mut written,
                )?;
                write_sink(output, IS_INVALID, &mut written)?;
                let marker_view = ValueView::Primitive {
                    bytes: markers,
                    count: markers.len(),
                    width: 1,
                };
                encode_planned_values(
                    output,
                    BOOL_TYPE,
                    marker_view,
                    &PlannedEncoding::Bits,
                    &mut written,
                )?;
            }
            None => write_sink(output, &0i32.to_le_bytes(), &mut written)?,
        }
    }
    if written != plan.byte_len {
        return Err(format!(
            "SBDF planned length mismatch: planned {}, wrote {written}",
            plan.byte_len
        ));
    }
    Ok(())
}

fn planned_encoding(
    type_id: u8,
    values: ValueView<'_>,
    strategy: EncodingStrategy,
) -> Result<PlannedEncoding, String> {
    if type_id == BOOL_TYPE {
        return Ok(PlannedEncoding::Bits);
    }
    let use_rle = type_id != 0x0c
        && match strategy {
            EncodingStrategy::Plain => false,
            EncodingStrategy::Rle => true,
            EncodingStrategy::Adaptive => rle_is_smaller(values)?,
        };
    if !use_rle {
        return Ok(PlannedEncoding::Plain);
    }
    let mut run_lengths = Vec::new();
    let mut run_value_indexes = Vec::new();
    if values.count() > 0 {
        let mut start = 0usize;
        let mut length = 1usize;
        for index in 1..values.count() {
            if length == 256 || values.value(index)? != values.value(start)? {
                run_lengths.push((length - 1) as u8);
                run_value_indexes.push(start);
                start = index;
                length = 1;
            } else {
                length += 1;
            }
        }
        run_lengths.push((length - 1) as u8);
        run_value_indexes.push(start);
    }
    checked_i32(run_lengths.len(), "SBDF planned RLE run count")?;
    Ok(PlannedEncoding::Rle {
        run_lengths,
        run_value_indexes,
    })
}

fn planned_encoding_len(
    values: ValueView<'_>,
    encoding: &PlannedEncoding,
) -> Result<usize, String> {
    match encoding {
        PlannedEncoding::Plain => {
            let values_len = encoded_values_size(values, 0..values.count())?;
            2usize
                .checked_add(size_of::<i32>())
                .and_then(|size| size.checked_add(values_len))
                .ok_or_else(|| "SBDF planned plain size overflow".to_string())
        }
        PlannedEncoding::Rle {
            run_lengths,
            run_value_indexes,
        } => {
            let values_len = encoded_values_size(values, run_value_indexes.iter().copied())?;
            14usize
                .checked_add(run_lengths.len())
                .and_then(|size| size.checked_add(values_len))
                .ok_or_else(|| "SBDF planned RLE size overflow".to_string())
        }
        PlannedEncoding::Bits => Ok(bit_encoding_len(values.count())),
    }
}

fn bit_encoding_len(count: usize) -> usize {
    2 + size_of::<i32>() + count.div_ceil(8)
}

fn encode_planned_values<W: Write>(
    output: &mut W,
    type_id: u8,
    values: ValueView<'_>,
    encoding: &PlannedEncoding,
    written: &mut usize,
) -> Result<(), String> {
    match encoding {
        PlannedEncoding::Plain => {
            write_sink(output, &[PLAIN_ENCODING, type_id], written)?;
            write_sink(
                output,
                &checked_i32(values.count(), "SBDF plain value count")?.to_le_bytes(),
                written,
            )?;
            if cfg!(target_endian = "little") {
                if let ValueView::Primitive { bytes, .. } = values {
                    return write_sink(output, bytes, written);
                }
            }
            encode_values_to_sink(output, values, 0..values.count(), written)
        }
        PlannedEncoding::Rle {
            run_lengths,
            run_value_indexes,
        } => {
            write_sink(output, &[RLE_ENCODING, type_id], written)?;
            write_sink(
                output,
                &checked_i32(values.count(), "SBDF RLE row count")?.to_le_bytes(),
                written,
            )?;
            write_sink(
                output,
                &checked_i32(run_lengths.len(), "SBDF RLE run count")?.to_le_bytes(),
                written,
            )?;
            write_sink(output, run_lengths, written)?;
            write_sink(
                output,
                &checked_i32(run_value_indexes.len(), "SBDF RLE value count")?.to_le_bytes(),
                written,
            )?;
            if cfg!(target_endian = "little")
                && run_lengths.len() == values.count()
                && run_lengths.iter().all(|length| *length == 0)
            {
                if let ValueView::Primitive { bytes, .. } = values {
                    return write_sink(output, bytes, written);
                }
            }
            encode_values_to_sink(output, values, run_value_indexes.iter().copied(), written)
        }
        PlannedEncoding::Bits => {
            write_sink(output, &[BIT_ENCODING, type_id], written)?;
            write_sink(
                output,
                &checked_i32(values.count(), "SBDF bit value count")?.to_le_bytes(),
                written,
            )?;
            let mut byte = 0u8;
            for index in 0..values.count() {
                byte <<= 1;
                if values.value(index)?.iter().any(|value| *value != 0) {
                    byte |= 1;
                }
                if index % 8 == 7 {
                    write_sink(output, &[byte], written)?;
                    byte = 0;
                }
            }
            let remaining = values.count() % 8;
            if remaining != 0 {
                write_sink(output, &[byte << (8 - remaining)], written)?;
            }
            Ok(())
        }
    }
}

fn encode_values_to_sink<W, I>(
    output: &mut W,
    values: ValueView<'_>,
    indexes: I,
    written: &mut usize,
) -> Result<(), String>
where
    W: Write,
    I: IntoIterator<Item = usize>,
    I::IntoIter: Clone,
{
    let indexes = indexes.into_iter();
    if values.is_primitive() {
        const PRIMITIVE_GATHER_BYTES: usize = 64 * 1024;
        let mut gathered = Vec::with_capacity(PRIMITIVE_GATHER_BYTES);
        for index in indexes {
            let value = values.value(index)?;
            if gathered.len().saturating_add(value.len()) > PRIMITIVE_GATHER_BYTES {
                write_sink(output, &gathered, written)?;
                gathered.clear();
            }
            if cfg!(target_endian = "little") || value.len() == 1 {
                gathered.extend_from_slice(value);
            } else {
                gathered.extend(value.iter().rev().copied());
            }
        }
        return write_sink(output, &gathered, written);
    }
    let mut packed_size = 0usize;
    for index in indexes.clone() {
        let length = values.value(index)?.len();
        packed_size = packed_size
            .checked_add(packed_i32_len(length)?)
            .and_then(|size| size.checked_add(length))
            .ok_or_else(|| "SBDF packed variable array size overflow".to_string())?;
    }
    write_sink(
        output,
        &checked_i32(packed_size, "SBDF packed variable array size")?.to_le_bytes(),
        written,
    )?;
    for index in indexes {
        let value = values.value(index)?;
        let mut packed = [0u8; 5];
        let packed_len = pack_7bit_i32(
            checked_i32(value.len(), "SBDF variable value length")?,
            &mut packed,
        );
        write_sink(output, &packed[..packed_len], written)?;
        write_sink(output, value, written)?;
    }
    Ok(())
}

fn write_sink<W: Write>(output: &mut W, bytes: &[u8], written: &mut usize) -> Result<(), String> {
    output
        .write_all(bytes)
        .map_err(|error| format!("failed to write planned SBDF bytes: {error}"))?;
    *written = written
        .checked_add(bytes.len())
        .ok_or_else(|| "SBDF written byte count overflow".to_string())?;
    Ok(())
}

fn pack_7bit_i32(value: i32, output: &mut [u8; 5]) -> usize {
    let mut value = value as u32;
    let mut index = 0usize;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output[index] = byte;
        index += 1;
        if value == 0 {
            return index;
        }
    }
}

fn rle_is_smaller(values: ValueView<'_>) -> Result<bool, String> {
    let plain_values_size = encoded_values_size(values, 0..values.count())?;
    let plain_size = 6usize
        .checked_add(plain_values_size)
        .ok_or_else(|| "SBDF plain encoded size overflow".to_string())?;

    let mut run_count = 0usize;
    let mut rle_values_size = usize::from(!values.is_primitive()) * size_of::<i32>();
    if values.count() > 0 {
        let mut start = 0usize;
        let mut length = 1usize;
        for index in 1..values.count() {
            if length == 256 || values.value(index)? != values.value(start)? {
                run_count += 1;
                rle_values_size = add_encoded_value_size(rle_values_size, values, start)?;
                start = index;
                length = 1;
            } else {
                length += 1;
            }
        }
        run_count += 1;
        rle_values_size = add_encoded_value_size(rle_values_size, values, start)?;
    }
    checked_i32(run_count, "SBDF adaptive RLE run count")?;
    if !values.is_primitive() {
        checked_i32(
            rle_values_size - size_of::<i32>(),
            "SBDF adaptive RLE packed values",
        )?;
    }
    let rle_size = 14usize
        .checked_add(run_count)
        .and_then(|size| size.checked_add(rle_values_size))
        .ok_or_else(|| "SBDF RLE encoded size overflow".to_string())?;
    Ok(rle_size < plain_size)
}

fn encoded_values_size<I>(values: ValueView<'_>, indexes: I) -> Result<usize, String>
where
    I: IntoIterator<Item = usize>,
{
    let mut size = usize::from(!values.is_primitive()) * size_of::<i32>();
    for index in indexes {
        size = add_encoded_value_size(size, values, index)?;
    }
    if !values.is_primitive() {
        checked_i32(size - size_of::<i32>(), "SBDF adaptive plain packed values")?;
    }
    Ok(size)
}

fn add_encoded_value_size(
    size: usize,
    values: ValueView<'_>,
    index: usize,
) -> Result<usize, String> {
    let value = values.value(index)?;
    let prefix = if values.is_primitive() {
        0
    } else {
        packed_i32_len(value.len())?
    };
    size.checked_add(prefix)
        .and_then(|size| size.checked_add(value.len()))
        .ok_or_else(|| "SBDF adaptive encoded value size overflow".to_string())
}

fn encode_plain(output: &mut Vec<u8>, type_id: u8, values: ValueView<'_>) -> Result<(), String> {
    output.extend_from_slice(&[PLAIN_ENCODING, type_id]);
    write_i32(
        output,
        checked_i32(values.count(), "SBDF plain value count")?,
    );
    if cfg!(target_endian = "little") {
        if let ValueView::Primitive { bytes, .. } = values {
            output.extend_from_slice(bytes);
            return Ok(());
        }
    }
    encode_values(output, values, 0..values.count())
}

fn encode_rle(output: &mut Vec<u8>, type_id: u8, values: ValueView<'_>) -> Result<(), String> {
    output.extend_from_slice(&[RLE_ENCODING, type_id]);
    write_i32(output, checked_i32(values.count(), "SBDF RLE row count")?);

    let run_count_offset = output.len();
    write_i32(output, 0);
    let mut run_count = 0usize;
    let mut run_value_indexes = Vec::<usize>::new();
    if values.count() > 0 {
        let mut start = 0usize;
        let mut length = 1usize;
        for index in 1..values.count() {
            if length == 256 || values.value(index)? != values.value(start)? {
                output.push((length - 1) as u8);
                run_count += 1;
                run_value_indexes.push(start);
                start = index;
                length = 1;
            } else {
                length += 1;
            }
        }
        output.push((length - 1) as u8);
        run_count += 1;
        run_value_indexes.push(start);
    }

    output[run_count_offset..run_count_offset + size_of::<i32>()]
        .copy_from_slice(&checked_i32(run_count, "SBDF RLE run count")?.to_le_bytes());
    write_i32(
        output,
        checked_i32(run_value_indexes.len(), "SBDF RLE value count")?,
    );
    if cfg!(target_endian = "little") && run_count == values.count() {
        if let ValueView::Primitive { bytes, .. } = values {
            output.extend_from_slice(bytes);
            return Ok(());
        }
    }
    encode_values(output, values, run_value_indexes)
}

fn encode_bits(output: &mut Vec<u8>, type_id: u8, values: ValueView<'_>) -> Result<(), String> {
    output.extend_from_slice(&[BIT_ENCODING, type_id]);
    write_i32(output, checked_i32(values.count(), "SBDF bit value count")?);
    let mut byte = 0u8;
    for index in 0..values.count() {
        byte <<= 1;
        if values.value(index)?.iter().any(|value| *value != 0) {
            byte |= 1;
        }
        if index % 8 == 7 {
            output.push(byte);
            byte = 0;
        }
    }
    let remaining = values.count() % 8;
    if remaining != 0 {
        output.push(byte << (8 - remaining));
    }
    Ok(())
}

fn encode_values<I>(output: &mut Vec<u8>, values: ValueView<'_>, indexes: I) -> Result<(), String>
where
    I: IntoIterator<Item = usize>,
    I::IntoIter: Clone,
{
    let indexes = indexes.into_iter();
    if values.is_primitive() {
        for index in indexes {
            let value = values.value(index)?;
            if cfg!(target_endian = "little") || value.len() == 1 {
                output.extend_from_slice(value);
            } else {
                output.extend(value.iter().rev());
            }
        }
        return Ok(());
    }

    let mut packed_size = 0usize;
    for index in indexes.clone() {
        let length = values.value(index)?.len();
        packed_size = packed_size
            .checked_add(packed_i32_len(length)?)
            .and_then(|size| size.checked_add(length))
            .ok_or_else(|| "SBDF packed variable array size overflow".to_string())?;
    }
    write_i32(
        output,
        checked_i32(packed_size, "SBDF packed variable array size")?,
    );
    for index in indexes {
        let value = values.value(index)?;
        write_7bit_i32(
            output,
            checked_i32(value.len(), "SBDF variable value length")?,
        );
        output.extend_from_slice(value);
    }
    Ok(())
}

fn write_section(output: &mut Vec<u8>, id: u8) {
    output.extend_from_slice(&SECTION_MAGIC);
    output.push(id);
}

fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_7bit_i32(output: &mut Vec<u8>, value: i32) {
    let mut value = value as u32;
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn packed_i32_len(value: usize) -> Result<usize, String> {
    let value = checked_i32(value, "SBDF packed integer")? as u32;
    Ok(match value {
        0..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x1f_ffff => 3,
        0x20_0000..=0x0fff_ffff => 4,
        _ => 5,
    })
}

fn checked_i32(value: usize, label: &str) -> Result<i32, String> {
    i32::try_from(value).map_err(|_| format!("{label} exceeds the SBDF i32 limit"))
}

#[cfg(test)]
mod tests {
    use super::{
        begin_table_slice, begin_table_slice_to_sink, encode_column_slice,
        encode_column_slice_to_sink, encode_planned_table_slice, plan_table_slice,
        write_end_marker, write_preamble, ColumnInput, EncodingStrategy, ValueView, PLAIN_ENCODING,
        RLE_ENCODING,
    };

    #[test]
    fn preamble_and_end_marker_match_sbdf_1_metadata_contract() {
        let mut output = Vec::new();
        write_preamble(&mut output, &["name".to_string()], &[0x0a]).unwrap();
        write_end_marker(&mut output).unwrap();

        let expected = "df5b010100df5b02000000000100000002000000040000004e616d650a00\
                        0800000044617461547970650c0001040000006e616d6501010000000adf5b05";
        let expected = expected
            .split_whitespace()
            .collect::<String>()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(output, expected);
    }

    #[test]
    fn encodes_plain_primitive_and_invalid_bits() {
        let values = [1i32, 2];
        let bytes = unsafe {
            std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), size_of_val(&values))
        };
        let mut output = Vec::new();
        begin_table_slice(&mut output, 1).unwrap();
        encode_column_slice(
            &mut output,
            0x02,
            ValueView::Primitive {
                bytes,
                count: 2,
                width: 4,
            },
            EncodingStrategy::Plain,
            Some(&[0, 1]),
        )
        .unwrap();

        assert!(output.starts_with(&[0xdf, 0x5b, 0x03, 1, 0, 0, 0]));
        assert!(output.ends_with(&[0x03, 0x01, 2, 0, 0, 0, 0x40]));
    }

    #[test]
    fn rle_splits_runs_at_256_values() {
        let values = vec![7u8; 257];
        let mut output = Vec::new();
        begin_table_slice(&mut output, 1).unwrap();
        encode_column_slice(
            &mut output,
            0x02,
            ValueView::Primitive {
                bytes: &values,
                count: values.len(),
                width: 1,
            },
            EncodingStrategy::Rle,
            None,
        )
        .unwrap();
        assert!(output
            .windows(6)
            .any(|window| window == [2, 0, 0, 0, 255, 0]));
    }

    #[test]
    fn direct_rle_matches_staged_for_unique_primitives() {
        let values = (0..4_096i64).collect::<Vec<_>>();
        let bytes = unsafe {
            std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), size_of_val(values.as_slice()))
        };
        let view = ValueView::Primitive {
            bytes,
            count: values.len(),
            width: size_of::<i64>(),
        };
        let mut staged = Vec::new();
        begin_table_slice(&mut staged, 1).unwrap();
        encode_column_slice(&mut staged, 0x03, view, EncodingStrategy::Rle, None).unwrap();

        let mut direct = Vec::new();
        begin_table_slice_to_sink(&mut direct, 1).unwrap();
        encode_column_slice_to_sink(&mut direct, 0x03, view, EncodingStrategy::Rle, None).unwrap();

        assert_eq!(direct, staged);
    }

    #[test]
    fn adaptive_uses_plain_for_unique_values_and_rle_for_repeated_values() {
        let unique = [1i32, 2, 3, 4];
        let repeated = [7i32; 32];
        let encode = |values: &[i32]| {
            let bytes = unsafe {
                std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), size_of_val(values))
            };
            let mut output = Vec::new();
            begin_table_slice(&mut output, 1).unwrap();
            encode_column_slice(
                &mut output,
                0x02,
                ValueView::Primitive {
                    bytes,
                    count: values.len(),
                    width: size_of::<i32>(),
                },
                EncodingStrategy::Adaptive,
                None,
            )
            .unwrap();
            output
        };

        assert_eq!(encode(&unique)[10], PLAIN_ENCODING);
        assert_eq!(encode(&repeated)[10], RLE_ENCODING);
    }

    #[test]
    fn adaptive_handles_variable_values_and_keeps_binary_plain() {
        let repeated_bytes = b"same";
        let repeated_offsets = vec![0; 32];
        let repeated_lengths = vec![4; 32];
        let repeated = ValueView::Arena {
            bytes: repeated_bytes,
            offsets: &repeated_offsets,
            lengths: &repeated_lengths,
        };
        let mut string_output = Vec::new();
        begin_table_slice(&mut string_output, 1).unwrap();
        encode_column_slice(
            &mut string_output,
            0x05,
            repeated,
            EncodingStrategy::Adaptive,
            Some(&[0; 32]),
        )
        .unwrap();

        let mut binary_output = Vec::new();
        begin_table_slice(&mut binary_output, 1).unwrap();
        encode_column_slice(
            &mut binary_output,
            0x0c,
            repeated,
            EncodingStrategy::Adaptive,
            None,
        )
        .unwrap();

        assert_eq!(string_output[10], RLE_ENCODING);
        assert_eq!(binary_output[10], PLAIN_ENCODING);
    }

    #[test]
    fn planned_sink_matches_staged_serializer_and_exact_length() {
        let numbers = [7i32, 7, 9, 9, 9];
        let number_bytes = unsafe {
            std::slice::from_raw_parts(numbers.as_ptr().cast::<u8>(), size_of_val(&numbers))
        };
        let text_bytes = b"alphabetagamma";
        let text_offsets = [0, 5, 9, 9, 9];
        let text_lengths = [5, 4, 5, 5, 5];
        let invalids = [0, 0, 0, 1, 0];
        let columns = [
            ColumnInput {
                type_id: 0x02,
                values: ValueView::Primitive {
                    bytes: number_bytes,
                    count: numbers.len(),
                    width: size_of::<i32>(),
                },
                strategy: EncodingStrategy::Adaptive,
                invalids: None,
            },
            ColumnInput {
                type_id: 0x0a,
                values: ValueView::Arena {
                    bytes: text_bytes,
                    offsets: &text_offsets,
                    lengths: &text_lengths,
                },
                strategy: EncodingStrategy::Rle,
                invalids: Some(&invalids),
            },
        ];

        let mut staged = Vec::new();
        begin_table_slice(&mut staged, columns.len()).unwrap();
        for column in &columns {
            encode_column_slice(
                &mut staged,
                column.type_id,
                column.values,
                column.strategy,
                column.invalids,
            )
            .unwrap();
        }

        let plan = plan_table_slice(&columns).unwrap();
        let mut planned = Vec::new();
        encode_planned_table_slice(&mut planned, &columns, &plan).unwrap();

        assert_eq!(plan.byte_len(), staged.len());
        assert_eq!(planned, staged);
    }
}
