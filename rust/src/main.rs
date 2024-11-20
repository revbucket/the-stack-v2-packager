use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::{Result, Error};
use crate::io::{load_parquet_as_json_parallel, read_gzip_file, decode_to_string, write_bytes, FileProcessError};
use serde_json::{Value as JsonValue};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use zstd::stream::encode_all;
use zstd::DEFAULT_COMPRESSION_LEVEL;

pub mod io;

const MISSING_THRESHOLD: f64 = 0.01; 
// We're okay if 1% of the rows 404, but if it's > than that, it probably means 
// that we didn't actually download the data


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


fn get_output_file_loc(local_jsonl_dir: &PathBuf, language: &String, parquet_num: &String, jsonl_num: usize, total_num: usize) -> PathBuf {
    let filename = format!("{}-{}-{:06}-of-{:06}.jsonl.zstd", language.as_str(), parquet_num.as_str(), jsonl_num, total_num);
    local_jsonl_dir.join(filename)
}






fn process_row(mut row: JsonValue, blob_loc: &PathBuf) -> Result<JsonValue, Error> {
    let blob_id = row.get("blob_id").unwrap().as_str().unwrap();
    let blob_file = blob_loc.join(format!("{}{}", blob_id, ".gz"));

    let blob_contents = read_gzip_file(&blob_file);
    match blob_contents {
        Ok(contents) => {
            let utf_str = decode_to_string(&contents, row["src_encoding"].as_str().unwrap()).unwrap();
            row["contents"] = JsonValue::String(utf_str);     
            Ok(row)
        },
        Err(e) => {
            Err(e)
        }
    }


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
        let chunk_size = chunk.len();
        let failed_rows = AtomicUsize::new(0);
        let processed_chunks: Vec<u8> = chunk.into_par_iter()
            .map(|v| {
                let proc_output = process_row(v.clone(), &blob_loc);
                match proc_output {
                    Ok(value) => {
                        // File was read correctly
                        let mut output_str = value.to_string();
                        output_str.push('\n');
                        let bytes = output_str.as_bytes();
                        let out = encode_all(bytes, DEFAULT_COMPRESSION_LEVEL).unwrap();
                        out
                    }
                    Err(e) if matches!(e.downcast_ref(), Some(FileProcessError::FileNotFound { ..})) => {
                        // File missing, increment counter and proceed
                        failed_rows.fetch_add(1, Ordering::SeqCst);
                        let out: Vec<u8> = Vec::new();
                        out
                    },
                    Err(e) => {
                        panic!("Unexpected error {:?}", e);
                    }, 
                }
            }).flatten()
            .collect();

        let failed_rows = failed_rows.into_inner();
        if failed_rows > 0 {
            println!("Had {:?}/{:?} missing rows", failed_rows, chunk_size);
        }
        if (failed_rows as f64 / chunk_size as f64) > MISSING_THRESHOLD {
            return Err(FileProcessError::TooManyMissing { missing_count: failed_rows, total_files: chunk_size }.into());

        }

        println!("Processed cuhnk in {:?} msecs", start_chunk.elapsed().as_millis());
        let output_file_loc = get_output_file_loc(local_jsonl_dir, &language, &pqt_number, chunk_num, num_chunks);
        let start_save = Instant::now();

        write_bytes(processed_chunks, output_file_loc).unwrap();
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
            process_parquet_file(parquet_file, local_jsonl_dir, *max_lines, )
        },
    };
    result.unwrap();
}



