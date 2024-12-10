# Code to make the s5cmd files 

import glob
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial
import time
import random
import os 


# Modify these!
# This is currently set up for an i4i.32xl
PARQUET_DIR = '/mnt/raid0/the-stack-v2/raw-hf-parquets/'
TARGET_DIR = '/mnt/raid0/'
NUM_CPUS = 120
# ^^^^^


def run_imap_multiprocessing(func, argument_list, num_processes):
    pool = Pool(processes=num_processes)
    result_list_tqdm = []
    for result in tqdm(pool.imap(func=func, iterable=argument_list), total=len(argument_list)):
        result_list_tqdm.append(result)
    return result_list_tqdm



def make_s5cmd_command(parquet_file):
	assert parquet_file.endswith('.parquet')
	cmd_file = re.sub(r'\.parquet$', '.cmd.txt', parquet_file)
	pl = os.path.basename(os.path.dirname(parquet_file))
	blobline = lambda blob: 'cp s3://softwareheritage/content/%s ' % blob + os.path.join(TARGET_DIR, 'the-stack-v2/data', pl, cmd_file)

	df = pd.read_parquet(parquet_file, columns=['blob_id'])
	with open(cmd_file, 'w') as f:
		for blob in df['blob_id']:
			f.write(blobline(blob) + '\n')


def main():
	parquets = glob.glob(os.path.join(PARQUET_DIR, '**/*.parquet'), recursive=True)
	run_imap_multiprocessing(make_s5cmd_command, parquets, NUM_CPUS)



if __name__ == '__main__':
	main()

