use std::path::PathBuf;
use clap::{Parser, Subcommand};
use anyhow::{Result, Error};



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

    Collect { 
        /// Which programming language we're working on
        #[arg(required=true, long)]
        language: PathBuf,

        /// Where the dir in s3://ai2-llm/pretraining-data/sources/the-stack-v2/raw-hf-parquets/ is downloaded to
        #[arg(required=true, long, default_value="/mnt/raid0/the-stack-v2/")]
        base_dir: PathBuf,

        /// Directory that local jsonl.gz files should get saved in 
        #[arg(required=true, long, default_value="/mnt/raid0/jsonls/")]
        local_jsonl_dir: PathBuf,

        /// Max number of lines per jsonl
        #[arg(long, default_value_t=16384)] 
        max_lines: usize,
    }, 
}




/*=================================================
=                HELPER METHODS                   =
=================================================*/

fn collect_parquet_files(language: PathBuf, base_dir: PathBuf) -> Vec<PathBuf> {
    let pqt_files : Vec<PathBuf> = Vec::new();
    pqt_files 
}


fn extract_blob_loc(pqt: PathBuf) -> (String, String, PathBuf) {
    // Given a parquet file, points to the location where the blob.gz's live 
    // /mnt/raid0/the-stack-v2/raw-hf-parquets/1C_Enterprise/train-00000-of-00001.cmd.txt
    // -> (1C_Enterprise, 0000, /mnt/raid0/the-stack-v2/raw-hf-parquets/1C_Enterprise/0000/
    let parent = pqt.parent().unwrap();
    let language = parent.file_name().unwrap().to_str().unwrap();
    let basename = pqt.file_name().unwrap().to_str().unwrap();
    let filename = pqt.file_name().unwrap().to_str().unwrap();
    let number = filename.split('-').nth(1).unwrap();
    (language.to_string(), number.to_string(), parent.join(number))
}


fn process_row(row: usize, blob_loc: &PathBuf) -> usize {
    row
}

fn save_processed_chunk(processed_chunk: Vec<usize>, output_loc: PathBuf) -> Result<(), Error> {
    Ok(())
}


fn process_parquet_file(pqt: PathBuf, max_lines: usize, local_jsonl_dir: PathBuf) -> Result<(), Error> {
    // Step 1: load parquet file into vec of rows 
    let (language, pqt_number, blob_loc) = extract_blob_loc(pqt.clone());
    let rows: Vec<usize> =  Vec::new();

    // Step 2: loop over chunks of rows 
    //  // Step 2a (parallel): load data into jsonls and count
    let mut chunk_num = 0; 
    for chunk in rows.chunks(max_lines) {
        let mut processed_chunk : Vec<usize> = Vec::new();
        for row in chunk { // DO IN PARALLEL HERE
            processed_chunk.push(process_row(*row, &blob_loc));
        }
        let output_path = local_jsonl_dir.clone().join(&language).join(format!("{pqt_number}_{chunk_num:04}.jsonl.gz"));
        save_processed_chunk(processed_chunk, output_path).unwrap();
        chunk_num += 1;
    }
    Ok(())
}



/*=============================================
=                 COLLECT METHOD              =
=============================================*/

fn collect(language: &PathBuf, base_dir: &PathBuf, local_jsonl_dir: &PathBuf, max_lines: usize) -> Result<(), Error> {
    // Step 1: Collect parquet files 
    let parquet_files = collect_parquet_files(language.clone(), base_dir.clone());

    // Step 2: Loop over parquet files 
    parquet_files.into_iter().for_each(|pqt| {
        // Process one-by-one (parallelism happens inside here)
        process_parquet_file(pqt, max_lines, local_jsonl_dir.clone()).unwrap();
    });

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
        Commands::Collect {language, base_dir, local_jsonl_dir, max_lines} => {
            collect(language, base_dir, local_jsonl_dir, *max_lines)
        },
    };
    result.unwrap();
}
