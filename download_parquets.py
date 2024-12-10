# Basic 'download a dataset from huggingface' script. 

import traceback
from huggingface_hub import snapshot_download
import os 

# Modify these!
# This is currently set up for an i4i.32xl
BASE_STORAGE_DIR = '/mnt/raid0'
LOCAL_DIR = os.path.join(BASE_STORAGE_DIR, 'the-stack-v2/raw-hf-parquets/')
CACHE_DIR = os.path.join(BASE_STORAGE_DIR, 'cache/')
NUM_CPUS = 120
# ^^^^^

def main():
	while True:
		try:
			snapshot_download(
				'bigcode/the-stack-v2',
				local_dir=LOCAL_DIR,
				cache_dir=CACHE_DIR,
				repo_type='dataset',
				local_dir_use_symlinks=True,
				num_workers=NUM_CPUS)
			break
		except KeyboardInterrupt:
			break
		except:
			traceback.print_exc()
			continue


if __name__ == '__main__':
	main()
