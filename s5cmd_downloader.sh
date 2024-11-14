#!/bin/bash

# Read the input file line by line
while IFS= read -r line; do
    # Check if the line is not empty

    if [ -n "$line" ]; then
    	echo $line
    	#
        ## List all .txt files in the directory
        for txt_file in "/mnt/raid0/the-stack-v2/data/$line"/*.txt; do
            # Check if files exist to avoid processing literal *.txt
            ./s5cmd run $txt_file
        done
        ./s5cmd cp -sp /mnt/raid0/the-stack-v2/data/$line/* s3://ai2-llm/pretraining-data/sources/the-stack-v2/raw-hf-parquets/$line/*
        
        # Remove the directory after processing
        break
        rm -rf "/mnt/raid0/the-stack-v2/data/$line"
    fi
done