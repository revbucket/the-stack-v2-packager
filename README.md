# the-stack-v2-packager


Some tooling for efficiently converting [the-stack-v2](https://huggingface.co/datasets/bigcode/the-stack-v2) into a usable .jsonl format.
**Why:** The Stack v2 is a huge, open-source code dataset, but the current Huggingface repository only has SWHIDs to download the contents of each code file. The contents for each line can be acquired, but it's laborious. This repo aims to speed that process up.
**What:** There's several steps of tooling involved here, but the end-goal, if everything goes right for you is that you'll have many many .jsonl.zstd files on your organization's S3 bucket, where each line in each jsonl.zstd has all the properties of each of the 2.73B rows from bigcode's HF repository, plus the contents of each software heritage file. This is amounts to about 15.1TB of compressed data.
**How:** There are several steps attached here (which will be described below, but here's the high-level...)

1. (python) Download all parquets [from Huggingface](https://huggingface.co/datasets/bigcode/the-stack-v2).
2. (python) Create `s5cmd` command files to download the requisite code files from Software Heritage. 
3. (bash) Put these parquets and command files onto your S3 bucket.
4. (bash, calling rust + python) Loop over all parquets and make the requisite jsonl's for each one. This is by far the rate limiter, but can be scaled pretty horizontally.

The rest of this README will include detailed instructions. 

## System Requirements + Recommendations
Throughout we'll assume you have access to AWS and the ability to spin up as many i4i instances as you like. We like i4i instances because the AWS nitro drives are huge and very very fast. In steps 1,2, and 3, this is basically a single-node job. Just run the scripts I provide :D. For step 4 (the slow part), you can scale that out pretty wide. 



# Detailed Steps
## 1. Download all parquets from HF
First step is to download all the parquets from HuggingFace locally. Just do this in python with a script like `download_parquets.py`. Make sure you adjust the constants in that to fit your system in particular (but if you use an i4i.32xl setup according to the setup script, then you shouldn't have to change anything).

## 2. Create the `s5cmd` commands
Ultimately we're going to paralellize over parquet files. It's much more efficient to use s5cmd to download all the code files for a particular parquet file a priori than it is to do so on the fly. We'll generally use s5cmd for all interactions with S3 (it's so fast!). To do this we first need to make the command files to call `s5cmd run` on. This is a one-off, so it can be done on the same machine that you downloaded the parquets to. Use a script like `make_s5cmd_commands.py` as an example.


## 3. Put the parquets and command files onto S3.
If you followed these first two steps, you should have a directory with structure like 
```
the-stack-v2
├── raw-hf-parquets/
│   ├── 1C_Enterprise/
│   │   ├── train-00000-of-00001.cmd.txt
│   │   └── train-00000-of-00001.parquet
│   ├── 2-Dimensional_Array/
...
```
And we want to put this whole directory somewhere on S3. 
To do this just call
`s5cmd cp -sp <path-to-the-stack-v2>/ s3://bucket/target/loc/the-stack-v2/`
... and now we're all set to start the parallel step. 

## 4. (Parallel) Loop over parquets and run the main loop.
Spin up however many EC2 i4i instances you want and run a setup script so they have a AWS nitro drive at `/mnt/raid0`. Then the flow looks like:
1. Download that set of parquets and cmd.txt files we generated in previous steps to a local directory (probably `/mnt/raid0`).
2. Run the main_loop.sh function. The arguments are as follows:...
TBD

