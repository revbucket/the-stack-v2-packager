"""
Some code to check if all of the jsonl.zstd's have been written for this parquet file 
already or not. If already completed, skip.

(It would be cool to do this in rust, but I _REFUSE_ to interface with s3 in rust. 
It's always a nightmare.)

"""

import boto3
from typing import List, Tuple
import os
import argparse

PREFIX = 'pretraining-data/sources/the-stack-v2/'
PQT_LOC = 'raw-hf-parquets'
JSONL_LOC = 'jsonl_data'


def list_s3_files_with_sizes(bucket_name: str, prefix: str = "") -> List[Tuple[str, int]]:
    """
    Lists all files and their sizes in bytes from an S3 bucket matching the given prefix.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix to filter objects (default: empty string for all files)
        
    Returns:
        List[Tuple[str, int]]: List of tuples containing (file_key, size_in_bytes)
        
    Example:
        files = list_s3_files_with_sizes('my-bucket', 'data/2024/')
        for file_key, size in files:
            print(f"{file_key}: {size:,} bytes")
    """
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        files_with_sizes = []
        
        # Paginate through results to handle buckets with many files
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    files_with_sizes.append((obj['Key'], obj['Size']))
        
        return sorted(files_with_sizes, key=lambda x: x[0])
        
    except Exception as e:
        print(f"Error accessing S3: {str(e)}")
        return []


def get_zstd_loc(bucket_name, pqt):
	pl = os.path.basename(os.path.dirname(pqt))
	num = os.path.basename(pqt).split('-')[1]
	return os.path.join(PREFIX, JSONL_LOC, pl, '%s-%s' % (pl, num))






def main(bucket_name, pqt):
	zstd_loc = get_zstd_loc(bucket_name, pqt)
	file_sizes = [_ for _ in list_s3_files_with_sizes(bucket_name, zstd_loc) if _[0].endswith('.jsonl.zstd')]
	if len(file_sizes) == 0: return False

	num_files = len(file_sizes)
	file = os.path.basename(file_sizes[0][0]) # Just look at one file 
	parts = file.split('-')
	assert parts[-2] == 'of'
	total_expected_files = int(parts[-1].split('.')[0])

	return (num_files == total_expected_files)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--bucket', type=str, default='ai2-llm')
	parser.add_argument('--parquet', type=str, required=True, 
						help="Should look like AGS_Script/train-00000-of-00001.parquet")

	args = parser.parse_args()
	print(main(args.bucket, args.parquet))




