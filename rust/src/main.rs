use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::{Result, Error};
use crate::io::{load_parquet_as_json_parallel, read_gzip_file, save_jsonlgz};
use serde_json::{Value as JsonValue};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::time::Instant;

pub mod io;


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


fn process_row(mut row: JsonValue, blob_loc: &PathBuf) -> Result<JsonValue, Error> {
    let blob_id = row.get("blob_id").unwrap().as_str().unwrap();
    let blob_file = blob_loc.join(format!("{}{}", blob_id, ".gz"));
    let blob_contents = read_gzip_file(&blob_file).unwrap();
    let blob_str = JsonValue::String(String::from_utf8(blob_contents).unwrap());
    row["contents"] = blob_str;

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

    // Step 2: loop over chunks of rows 
    let mut chunk_num = 0; 
    let num_chunks = rows.len().div_ceil(max_lines);
    let pbar = build_pbar(num_chunks, "Chunks");
    for chunk in rows.chunks(max_lines) {
        // and process each row of the chunk (in parallel!)
        let processed_chunk : Vec<JsonValue> = chunk.into_par_iter()
            .map(|v| process_row(v.clone(), &blob_loc).unwrap())
            .collect();

        let output_file_loc = get_output_file_loc(local_jsonl_dir, &language, &pqt_number, chunk_num);
        save_jsonlgz(processed_chunk, &output_file_loc).unwrap();
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
            process_parquet_file(parquet_file, local_jsonl_dir, *max_lines)
        },
    };
    result.unwrap();
}



