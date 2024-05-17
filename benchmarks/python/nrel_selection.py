
from datetime import datetime
import sys
import time
import random
import logging
import s3fs
import h5py
import h5pyd
import numpy as np
import config
import argparse


def read_col(dset, filepath):
    # choose a random column index
    extent = dset.shape[1]
    index = random.randint(0, extent-1)
    logging.info(f"using column index: {index}")
    if n > 1:
        # divide row into n selections
        region_size = dset.shape[0] // n
        selections = [np.s_[region_size * i:min(region_size * (i+1), dset.shape[0]), index] for i in range(n-1)]
        # add the last selection
        selections.append(np.s_[region_size * (n-1):dset.shape[0], index])
        print(f"reading {n} selections in parallel")
        mm = get_multimanager([dset] * n, filepath)
        arrs = mm[selections]
        arr = np.concatenate(arrs)
    else:
        arr = dset[:, index]

    print(f"{dset.name}[:, {index}] - min: {arr.min()} max: {arr.max()} mean: {arr.mean():.2f}")


def read_row(dset, filepath):
    # choose a random row index
    extent = dset.shape[0]
    index = random.randint(0, extent-1)
    logging.info(f"using row index: {index}")
    if n > 1:
        # divide col into n selections
        region_size = dset.shape[1] // n
        selections = [np.s_[index, region_size * i:min(region_size * (i+1), dset.shape[1])] for i in range(n-1)]
        # add the last selection
        selections.append(np.s_[index, region_size * (n-1):dset.shape[1]])
        print(f"reading {n} selections in parallel")
        mm = get_multimanager([dset] * n, filepath)
        arrs = mm[selections]
        arr = np.concatenate(arrs, axis=0)
    else:
        arr = dset[index, :]

    print(f"{dset.name}[{index}, :] - min: {arr.min()} max: {arr.max()} mean: {arr.mean():.2f}")


def get_loglevel():
    val = config.get("loglevel")
    val = val.upper()
    if val == "DEBUG":
        loglevel = logging.DEBUG
    elif val == "INFO":
        loglevel = logging.INFO
    elif val in ("WARN", "WARNING"):
        loglevel = logging.WARNING
    elif val == "ERROR":
        loglevel = logging.ERROR
    else:
        choices = ("DEBUG", "INFO", "WARNING", "ERROR")

        raise ValueError(f"loglevel must be one of {choices}")
    return loglevel

# generic file open -> return h5py(filename) or h5pyd(filename)
# based on a "hdf5://" prefix or not


def h5File(filepath, mode='r', page_buf_size=None):
    kwargs = {'mode': mode}
    if page_buf_size is not None:
        logging.info(f"using page_buf_size of {page_buf_size}")
        kwargs['page_buf_size'] = page_buf_size
        kwargs['fs_strategy'] = "page"
    if filepath.startswith("hdf5://"):
        f = h5pyd.File(filepath, **kwargs)
    elif filepath.startswith("s3://"):
        if mode != 'r':
            raise ValueError("s3fs can only be used with read access mode")
        s3 = s3fs.S3FileSystem()
        f = h5py.File(s3.open(filepath, 'rb'), **kwargs)
    elif filepath.startswith("http"):
        # use ros3 driver
        kwargs['driver'] = "ros3"
        kwargs['aws_region'] = config.get("aws_region").encode("utf-8")
        kwargs['secret_id'] = config.get("aws_access_key_id").encode("utf-8")
        kwargs['secret_key'] = config.get("aws_secret_access_key").encode("utf-8")
        f = h5py.File(filepath, **kwargs)
    else:
        f = h5py.File(filepath, **kwargs)
    return f


# Get MultiManager based on file type
def get_multimanager(dsets, filepath):
    if filepath.startswith("hdf5://"):
        return h5pyd.MultiManager(dsets)
    elif filepath.startswith("s3://"):
        return h5py.MultiManager(dsets)
    else:
        raise ValueError("MultiManager requires h5py or h5pyd file")


#
# main
#
parser = argparse.ArgumentParser()
parser.add_argument("--run_number", help="Run number for subsequent runs", type=int, default=1)
parser.add_argument("--n", help="Number of parallel selections to read simultaneously", type=int, default=1)

args = parser.parse_args()
run_number = args.run_number
n = args.n

# setup logging
logfname = config.get("log_file")
loglevel = get_loglevel()
logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
logging.debug(f"set log_level to {loglevel}")

nrel_foldername = config.get("nrel_foldername")
if not nrel_foldername or nrel_foldername[-1] != '/':
    sys.exit("expected nrel_foldername to end with '/'")
nrel_filename = config.get("nrel_filename")
nrel_filepath = f"{nrel_foldername}{nrel_filename}"
logging.info(f"filepath: {nrel_filepath}")
print(f"filepath: {nrel_filepath}")
nrel_h5path = config.get("nrel_h5path")
page_buf_size_exp = 0
if nrel_filename.startswith("PAGE10MiB"):
    page_buf_size_exp = int(config.get("page_buf_size_exp"))
    logging.info(f"page_buf_size_exp: {page_buf_size_exp}")
    if page_buf_size_exp > 0:
        page_buf_size = 2 ** page_buf_size_exp
    else:
        page_buf_size = None  
else:
    page_buf_size = None

start_time = time.time()  # start the clock!

if not nrel_filepath.startswith("hdf5://") and n > 1:
    raise ValueError("Only h5pyd MultiManager supports reading multiple views on the same dataset")

with h5File(nrel_filepath) as f:
    print(nrel_h5path)
    dset = f[nrel_h5path]
    read_col(dset, nrel_filepath)
    read_row(dset, nrel_filepath)

stop_time = time.time()
dt = datetime.fromtimestamp(start_time)
start_str = dt.strftime("%Y-%m-%d %H:%M:%S")
dt = datetime.fromtimestamp(stop_time)
stop_str = dt.strftime("%Y-%m-%d %H:%M:%S")
elapsed = stop_time - start_time
machine = config.get("machine")
# print result for inclusion in benchmark csv
csv_str = f"{run_number}, {start_str}, {stop_str}, {elapsed:5.1f}, python, {machine}, "
csv_str += f"{nrel_foldername}, , {nrel_filename}, {n}   , , , , , , "
if page_buf_size_exp > 0:
    csv_str += f"page_buf_size_exp: {page_buf_size_exp}"
print(csv_str)
