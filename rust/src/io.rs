  // You'll need crc32fast = "1.3" in Cargo.toml
use std::io::{BufWriter, Write};
use std::fs;
use flate2::Compression;
use std::io::Read;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::path::PathBuf;
use std::fs::File;
use anyhow::{Result, Error};
use rayon::prelude::*;
use encoding_rs::*;

use arrow::{
    array::{Array, BooleanArray, Int64Array, ListArray, StringArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use serde_json::{json, Value as JsonValue};
use oem_cp::decode_string_complete_table;
use oem_cp::code_table::{DECODING_TABLE_CP855, DECODING_TABLE_CP852, DECODING_TABLE_CP866};


/*====================================================================
=                      READ PARQUET INTO LIST OF JSONS               =
====================================================================*/

fn convert_column_to_json(
    column: &arrow::array::ArrayRef,
    row_idx: usize,
) -> Result<JsonValue, Error> {
    match column.data_type() {
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Invalid string array");
            Ok(if array.is_null(row_idx) {
                JsonValue::Null
            } else {
                JsonValue::String(array.value(row_idx).to_string())
            })
        }
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Invalid int64 array");
            Ok(if array.is_null(row_idx) {
                JsonValue::Null
            } else {
                json!(array.value(row_idx))
            })
        }
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Invalid boolean array");
            Ok(if array.is_null(row_idx) {
                JsonValue::Null
            } else {
                json!(array.value(row_idx))
            })
        }
        DataType::List(field) if field.data_type() == &DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("Invalid list array");
            
            if array.is_null(row_idx) {
                return Ok(JsonValue::Null);
            }
            
            let list_value = array.value(row_idx);
            let string_array = list_value
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Invalid string array in list");
            
            let values: Vec<String> = (0..string_array.len())
                .filter_map(|i| {
                    if string_array.is_null(i) {
                        None
                    } else {
                        Some(string_array.value(i).to_string())
                    }
                })
                .collect();
            
            Ok(json!(values))
        }
        DataType::Timestamp(_, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Invalid timestamp array");
            Ok(if array.is_null(row_idx) {
                JsonValue::Null
            } else {
                json!(array.value(row_idx))
            })
        }
        _ => Err(Error::msg(format!("Failed on {:?} {:?} {:?}", column, row_idx, column.data_type()))),
    }
}

pub(crate) fn load_parquet_as_json_parallel(path: PathBuf) -> Result<Vec<JsonValue>, Error> {
	let open_file = File::open(path)?;
	let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(open_file)?
		.with_batch_size(1024)
		.build()?;

    let batches: Result<Vec<_>, _> = arrow_reader.collect();
    let batches = batches?;

	
    // Process batches in parallel
    let json_records: Vec<_> = batches.par_iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row_idx| {
                    let mut row_obj = json!({});                    
                    if let JsonValue::Object(ref mut map) = row_obj {
                        for (col_idx, column) in batch.columns().iter().enumerate() {
                            let col_name = batch.schema().field(col_idx).name().clone();
                            if let Ok(value) = convert_column_to_json(column, row_idx) {
                                map.insert(col_name.to_string(), value);
                            }
                        }
                    }                    
                    row_obj
                })
                .collect::<Vec<_>>()
        })
        .collect();
    
    Ok(json_records)
}

/*==============================================================
=                        ENCODING/DECODING HELPERS             =
==============================================================*/

#[derive(Debug)]
pub enum DecodingError {
    UnsupportedEncoding(String),
    DecodingFailed(String),
}

/// Decodes bytes from the specified encoding into a UTF-8 String
pub(crate) fn decode_to_string(bytes: &[u8], encoding_name: &str) -> Result<String, Error> {
    // Get the encoding by name

    if encoding_name == "IBM852" {
        return Ok(decode_string_complete_table(bytes, &DECODING_TABLE_CP852));
    }
    if encoding_name == "IBM855" {
        return Ok(decode_string_complete_table(bytes, &DECODING_TABLE_CP855));
    }
    if encoding_name == "IBM866" {
        return Ok(decode_string_complete_table(bytes, &DECODING_TABLE_CP866));
    }    

    let encoding = match encoding_name.to_uppercase().as_str() {
        "BIG5" => BIG5,
        "EUC-JP" => EUC_JP,
        "GB18030" => GB18030,  
        "ISO-8859-1" => WINDOWS_1252, 
        "ISO-8859-2" | "ISO8859-2" => ISO_8859_2,
        "ISO-8859-3" | "ISO8859-3" => ISO_8859_3,        
        "ISO-8859-4" | "ISO8859-4" => ISO_8859_4,
        "ISO-8859-5" | "ISO8859-5" => ISO_8859_5,
        "ISO-8859-6" | "ISO8859-6" => ISO_8859_6,
        "ISO-8859-7" | "ISO8859-7" => ISO_8859_7,
        "ISO-8859-8" | "ISO8859-8" => ISO_8859_8,
        "ISO-8859-9" | "ISO8859-9" => Encoding::for_label(b"ISO-8859-9").unwrap(),        
        "ISO-8859-10" | "ISO8859-10" => ISO_8859_10,
        "ISO-8859-11" | "ISO8859-11" => Encoding::for_label(b"ISO-8859-11").unwrap(),

        "ISO-8859-13" | "ISO8859-13" => ISO_8859_13,
        "ISO-8859-14" | "ISO8859-14" => ISO_8859_14,
        "ISO-8859-15" | "ISO8859-15" => ISO_8859_15,
        "ISO-8859-16" | "ISO8859-16" => ISO_8859_16,
        "KOI8-R" | "KOI8R" => KOI8_R,
        "KOI8-U" | "KOI8U" => KOI8_U,
        "MACINTOSH" | "MAC" => MACINTOSH,
        "MACCENTRALEUROPE" => WINDOWS_1250,
        "MACCYRILLIC" => X_MAC_CYRILLIC,
        "SHIFT_JIS" => SHIFT_JIS,
        "TIS-620" => WINDOWS_874,
        "UHC" => EUC_KR,
        "UTF-16" => UTF_16BE,
        "WINDOWS-874" | "CP874" => WINDOWS_874,
        "WINDOWS-1250" | "CP1250" => WINDOWS_1250,
        "WINDOWS-1251" | "CP1251" => WINDOWS_1251,
        "WINDOWS-1252" | "CP1252" => WINDOWS_1252,
        "WINDOWS-1253" | "CP1253" => WINDOWS_1253,
        "WINDOWS-1254" | "CP1254" => WINDOWS_1254,
        "WINDOWS-1255" | "CP1255" => WINDOWS_1255,
        "WINDOWS-1256" | "CP1256" => WINDOWS_1256,
        "WINDOWS-1257" | "CP1257" => WINDOWS_1257,
        "WINDOWS-1258" | "CP1258" => WINDOWS_1258,
        "UTF-8" | "UTF8" => UTF_8,
        _ => return Err(Error::msg(format!("BAD ENCODING {:?}", encoding_name))),
    };
    
    // Decode the bytes
    let (cow, _, had_errors) = encoding.decode(bytes);
    
    if had_errors {
        Err(Error::msg(format!(
            "Failed to decode bytes using {} encoding",
            encoding_name
        )))
    } else {
        Ok(cow.into_owned())
    }
}


/*=============================================================
=                        GZIP TO/FROM BYTES                   =
=============================================================*/

pub(crate) fn read_gzip_file(path: &PathBuf) -> Result<Vec<u8>> {
    // Open the file
    let file = File::open(path)?;
    
    // Create a GzDecoder wrapping the file
    let mut gz = GzDecoder::new(file);
    
    // Create a buffer to store the decompressed data
    let mut buffer = Vec::new();
    
    // Read the decompressed data into the buffer
    gz.read_to_end(&mut buffer)?;
    
    Ok(buffer)
}


pub(crate) fn write_string_gzip(content: String, path: PathBuf) -> Result<(), Error> {
    // Use a large buffer (8MB) for better performance
    const BUFFER_SIZE: usize = 16 * 1024 * 1024;
    
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let file = File::create(path).unwrap();
    let gz = GzEncoder::new(file, Compression::fast());
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, gz);
    
    // Write the entire string at once
    writer.write_all(content.as_bytes()).unwrap();
    writer.flush().unwrap();
    
    Ok(())
}

pub(crate) fn write_bytes(content: Vec<u8>, path: PathBuf) -> Result<(), Error> {    
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut file = File::create(path).unwrap();

    file.write_all(&content).unwrap();


    Ok(())
}



