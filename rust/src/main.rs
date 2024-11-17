use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::{Result, Error};
use crate::io::{load_parquet_as_json_parallel, read_gzip_file, write_string_gzip, decode_to_string, write_bytes_gzip};
use serde_json::{Value as JsonValue};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::time::Instant;
use encoding_rs::*;
use crc32fast::Hasher;

use std::io::{Write};
use flate2::write::GzEncoder;
use flate2::Compression;

pub mod io;
const GZIP_HEADER: [u8; 10] = [
    0x1f, 0x8b,       // Magic numbers
    0x08,             // Compression method (deflate)
    0x00,             // Flags
    0x00, 0x00, 0x00, 0x00,  // Modification time
    0x00,             // Extra flags
    0x00,             // Operating system
];


/*==============================================
=                    ARGS                      =
==============================================*/

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[clap(subcommand)]
    command: Commands,

    #[arg(long, default_value_t=0)]
    threads: usize,
}


#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]
    ProcessParquet { 
        /// Which parquet file we're working with
        /// Should be formatted like <DIR>/the-stack-v2/raw-hf-parquets/<PROGRAMMING_LANGUAGE>/train-0000-of-1234.parquet
        #[arg(required=true, long)]
        parquet_file: PathBuf,

        /// Where the completed jsonls go
        #[arg(required=true, long, default_value="/mnt/raid0/jsonls/")]
        local_jsonl_dir: PathBuf,

        /// Max number of lines per jsonl
        #[arg(long, default_value_t=16384)] // 2^14 ~ 16k. Should have nice file sizes
        max_lines: usize,
    }, 
}


/*=================================================
=                HELPER METHODS                   =
=================================================*/


fn build_pbar(num_items: usize, units: &str) -> ProgressBar {
    let mut template = String::from(units);
    template.push_str(" {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]");
    let pbar = ProgressBar::new(num_items as u64)
        .with_style(
            ProgressStyle::with_template(&template).unwrap()
        );
    pbar.inc(0);
    pbar
}



fn extract_pqt_locations(pqt: PathBuf) -> Result<(PathBuf, String, String), Error> {
    /* Given a parquet file of the form 
    BASE_DIR/the-stack-v2/raw-hf-parquets/<PROGRAMMING_LANGUAGE>/train-XXXX-of-YYYY.parquet

    outputs the (dir where contents live, language, number XXXX above)
    */
    let pl_dir = pqt.parent().unwrap();
    let raw_hf_dir = pl_dir.parent().unwrap();
    let stack_dir = raw_hf_dir.parent().unwrap();

    let language = pl_dir.file_name().unwrap().to_str().unwrap();
    let number = pqt.file_name().unwrap().to_str().unwrap().split('-').nth(1).unwrap();
    let blob_dir = stack_dir.join("data").join(language).join(number);

    Ok((blob_dir, language.to_string(), number.to_string())) 
}


fn get_output_file_loc(local_jsonl_dir: &PathBuf, language: &String, parquet_num: &String, jsonl_num: usize) -> PathBuf {
    let filename = format!("{}-{}-{:04}.jsonl.gz", language.as_str(), parquet_num.as_str(), jsonl_num);
    local_jsonl_dir.join(filename)
}



fn convert_to_utf8(input: &[u8], encoding_name: &str) -> Result<Vec<u8>, String> {
    // Get the encoding by name
    let encoding = Encoding::for_label(encoding_name.as_bytes())
        .ok_or_else(|| format!("Unsupported encoding: {}", encoding_name))?;
    
    // Decode from source encoding to UTF-8
    let (cow, _, had_errors) = encoding.decode(input);
    if had_errors {
        return Err(format!("Decoding error for {}", encoding_name));
    }
    
    // Return the UTF-8 bytes
    Ok(cow.into_owned().into_bytes())
}



fn process_row(mut row: JsonValue, blob_loc: &PathBuf) -> Result<JsonValue, Error> {
    let blob_id = row.get("blob_id").unwrap().as_str().unwrap();
    let blob_file = blob_loc.join(format!("{}{}", blob_id, ".gz"));
    let blob_contents: Vec<u8> = read_gzip_file(&blob_file).unwrap();
    let utf_str = decode_to_string(&blob_contents, row["src_encoding"].as_str().unwrap()).unwrap();
    row["contents"] = JsonValue::String(utf_str);  
    Ok(row)
}



/*=============================================
=                 COLLECT METHOD              =
=============================================*/

fn process_parquet_file(pqt: &PathBuf, local_jsonl_dir: &PathBuf, max_lines: usize) -> Result<(), Error> {
    // Step 1: load parquet file into vec of rows 
    let start_main = Instant::now();    
    let (blob_loc, language, pqt_number) = extract_pqt_locations(pqt.clone()).unwrap();
    let rows: Vec<JsonValue> = load_parquet_as_json_parallel(pqt.clone()).unwrap();
    println!("Read pqt in {:?} msecs", start_main.elapsed().as_millis());
    // Step 2: loop over chunks of rows 
    let mut chunk_num = 0; 


    let num_chunks = rows.len().div_ceil(max_lines);
    let pbar = build_pbar(num_chunks, "Chunks");
    for chunk in rows.chunks(max_lines) {
        // and process each row of the chunk (in parallel!)
        let start_chunk = Instant::now();
        let processed_chunk : String = chunk.into_par_iter()
            .map(|v| {
                format!("{}\n", process_row(v.clone(), &blob_loc).unwrap().to_string())
            })
            .collect::<String>();

        println!("Processed cuhnk in {:?} msecs", start_chunk.elapsed().as_millis());
        let output_file_loc = get_output_file_loc(local_jsonl_dir, &language, &pqt_number, chunk_num);
        let start_save = Instant::now();
        write_string_gzip(processed_chunk, output_file_loc).unwrap();
        println!("Saved chunk in {:?} msecs", start_save.elapsed().as_millis());
        chunk_num += 1;
        pbar.inc(1);
    }

    println!("Made {:?} jsonl.gz's in {:?} seconds", num_chunks, start_main.elapsed().as_secs());
    Ok(())
}



struct ChunkResult {
    compressed: Vec<u8>,
    crc: u32,
    size: u32,
}


fn process_parquet_file2(pqt: &PathBuf, local_jsonl_dir: &PathBuf, max_lines: usize) -> Result<(), Error> {
    // Step 1: load parquet file into vec of rows 
    let start_main = Instant::now();    
    let (blob_loc, language, pqt_number) = extract_pqt_locations(pqt.clone()).unwrap();
    let rows: Vec<JsonValue> = load_parquet_as_json_parallel(pqt.clone()).unwrap();
    println!("Read pqt in {:?} msecs", start_main.elapsed().as_millis());
    // Step 2: loop over chunks of rows 
    let mut chunk_num = 0; 


    let num_chunks = rows.len().div_ceil(max_lines);
    let pbar = build_pbar(num_chunks, "Chunks");
    for chunk in rows.chunks(max_lines) {
        // and process each row of the chunk (in parallel!)
        let start_chunk = Instant::now();
        let processed_chunks: Vec<ChunkResult> = chunk.into_par_iter()
            .map(|v| {
                let mut output_str = process_row(v.clone(), &blob_loc).unwrap().to_string();
                output_str.push('\n');
                let bytes = output_str.as_bytes();
                
                // Calculate CRC32 of uncompressed data
                let mut hasher = Hasher::new();
                hasher.update(bytes);
                let crc = hasher.finalize();
                
                // Compress the data
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(bytes).unwrap();
                let compressed = encoder.finish().unwrap();
                
                ChunkResult {
                    compressed: compressed[10..compressed.len()-8].to_vec(),
                    crc,
                    size: bytes.len() as u32,
                }
            })
            .collect();

        // Combine all chunks
        let mut final_output = Vec::new();
        final_output.extend_from_slice(&GZIP_HEADER);

        // Write all compressed data
        let mut total_crc = Hasher::new();
        let mut total_size = 0u32;

        for chunk in processed_chunks {
            final_output.extend(&chunk.compressed);
            // Combine CRCs properly
            let mut hasher = Hasher::new();
            hasher.update(&chunk.crc.to_le_bytes());
            total_crc = hasher;
            total_size += chunk.size;
        }

        // Write final trailer
        final_output.extend(&total_crc.finalize().to_le_bytes());
        final_output.extend(&total_size.to_le_bytes());            

        println!("Processed cuhnk in {:?} msecs", start_chunk.elapsed().as_millis());
        let output_file_loc = get_output_file_loc(local_jsonl_dir, &language, &pqt_number, chunk_num);
        let start_save = Instant::now();

        write_bytes_gzip(final_output, output_file_loc).unwrap();
        println!("Saved chunk in {:?} msecs", start_save.elapsed().as_millis());
        chunk_num += 1;
        pbar.inc(1);
    }

    println!("Made {:?} jsonl.gz's in {:?} seconds", num_chunks, start_main.elapsed().as_secs());
    Ok(())
}




/*=========================================
=                 MAIN                    =
=========================================*/

fn main() {
    let args = ArgParser::parse();
    let threads = args.threads;
    if threads != 0 {
        std::env::set_var("RAYON_NUM_THREADS", threads.to_string());
    }
    let result = match &args.command {
        Commands::ProcessParquet {parquet_file, local_jsonl_dir, max_lines} => {
            process_parquet_file2(parquet_file, local_jsonl_dir, *max_lines)
        },
    };
    result.unwrap();
}



