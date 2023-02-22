use parquet2::{
    encoding::Encoding,
    page::DataPage,
    schema::types::PrimitiveType,
    statistics::{serialize_statistics, BinaryStatistics, ParquetStatistics, Statistics},
};

use super::super::binary::{encode_delta, ord_binary};
use super::super::utils;
use super::super::WriteOptions;
use crate::{
    array::{Array, Utf8Array},
    error::{Error, Result},
    io::parquet::read::schema::is_nullable,
    offset::Offset,
};

pub(crate) fn encode_plain<O: Offset>(
    array: &Utf8Array<O>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    if is_optional {
        array.iter().for_each(|x| {
            if let Some(x) = x {
                // BYTE_ARRAY: first 4 bytes denote length in littleendian.
                let len = (x.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(x.as_bytes());
            }
        })
    } else {
        array.values_iter().for_each(|x| {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x.as_bytes());
        })
    }
}

pub fn array_to_page<O: Offset>(
    array: &Utf8Array<O>,
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> Result<DataPage> {
    let validity = array.validity();
    let is_optional = is_nullable(&type_.field_info);

    let mut buffer = vec![];
    utils::write_def_levels(
        &mut buffer,
        is_optional,
        validity,
        array.len(),
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => encode_plain(array, is_optional, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(
            array.values(),
            array.offsets().buffer(),
            array.validity(),
            is_optional,
            &mut buffer,
        ),
        _ => {
            return Err(Error::InvalidArgumentError(format!(
                "Datatype {:?} cannot be encoded by {:?} encoding",
                array.data_type(),
                encoding
            )))
        }
    }

    let statistics = if options.write_statistics {
        Some(build_statistics(array, type_.clone()))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        array.len(),
        array.len(),
        array.null_count(),
        0,
        definition_levels_byte_length,
        statistics,
        type_,
        options,
        encoding,
    )
}

pub(crate) fn build_statistics<O: Offset>(
    array: &Utf8Array<O>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let statistics = &BinaryStatistics {
        primitive_type,
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .map(|x| truncate_up(x))
            .max_by(|x, y| ord_binary(x.as_bytes(), y.as_bytes()))
            .map(|x| x.into_bytes()),
        min_value: array
            .iter()
            .flatten()
            .map(|x| truncate_down(x))
            .min_by(|x, y| ord_binary(x.as_bytes(), y.as_bytes()))
            .map(|x| x.into_bytes()),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}

const MAX_STAT_LENGTH: usize = 256;

/// Truncate a string down to the given `MAX_STAT_LENGTH`; breaks
/// at a proper character boundary before the limit.
pub fn truncate_down(s: &str) -> String {
    if s.len() <= MAX_STAT_LENGTH {
        return s.to_string();
    }
    for index in (0..=MAX_STAT_LENGTH).rev() {
        if s.is_char_boundary(index) {
            return s[..index].to_string();
        }
    }
    unreachable!();
}

/// Truncate a string "up", such that it has length at most `MAX_STAT_LENGTH`
/// but is lexicographically greater than the given string. Mainly useful
/// for computing useful min/max statistics.
pub fn truncate_up(s: &str) -> String {
    if s.len() <= MAX_STAT_LENGTH {
        return s.to_string();
    }
    for index in (0..=MAX_STAT_LENGTH).rev() {
        if s.is_char_boundary(index) {
            let mut trunc = s[..index].to_string();
            let ch = trunc.pop().unwrap();
            trunc.push(match ch {
                '\0'..='\u{D7FE}' | '\u{E000}'..='\u{10FFFE}' => {
                    char::from_u32(ch as u32 + 1).unwrap()
                }
                '\u{D7FF}' => '\u{E000}',
                // technically wrong (not a proper rounding up) but good enough
                _ => '\u{10FFFF}',
            });
            return trunc;
        }
    }
    unreachable!();
}
