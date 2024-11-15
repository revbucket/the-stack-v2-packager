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


use arrow::{
    array::{Array, BooleanArray, Int64Array, ListArray, StringArray, TimestampNanosecondArray},
    datatypes::DataType,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use serde_json::{json, Value as JsonValue};


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


pub(crate) fn save_jsonlgz(jsons: Vec<JsonValue>, output_path: &PathBuf) -> Result<(), Error> {
	if let Some(parent) = output_path.parent() {
		fs::create_dir_all(parent).unwrap();
	}
	let file = File::create(output_path).unwrap();
	let gz = GzEncoder::new(file, Compression::default());
	let mut writer = BufWriter::new(gz);
	for value in jsons {
		writeln!(writer, "{}", value.to_string()).unwrap();
	}
	writer.flush().unwrap();

	Ok(())
}


