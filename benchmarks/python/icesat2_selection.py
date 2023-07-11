from collections import namedtuple

from datetime import datetime
import sys
import time
import logging
import s3fs
import h5py
import h5pyd
import numpy as np
import config

ground_tracks = ("gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r")
scalar_datasets = ("/orbit_info/sc_orient", 
                   "/ancillary_data/start_rgt",
                   "/ancillary_data/start_cycle")
reference_datasets = ("geolocation/reference_photon_lat",
                    "geolocation/reference_photon_lon",
                    "geolocation/segment_ph_cnt")

ph_count_datasets = ("heights/dist_ph_along",
                    "heights/h_ph",
                    "heights/signal_conf_ph",
                    "heights/quality_ph",
                    "heights/lat_ph",
                    "heights/lon_ph",
                    "heights/delta_time",
                    )

geolocation_lat = "/geolocation/reference_photon_lat"
geolocation_lon = "/geolocation/reference_photon_lon"

BBox = namedtuple("BBox", "min_lon max_lon min_lat max_lat")
Range = namedtuple("Range", "min max")

# copy any attributes from input root group to output root
def copy_root_attrs(fin, fout):
    for k in fin.attrs:
        v = fin.attrs[k]
        fout.attrs[k] = v


# copy scalar datasets
def copy_scalar_datasets(fin, fout):
    for h5path in scalar_datasets:
        dset = fin[h5path]
        data = dset[...]
        parts = h5path.split("/")
        grp = fout
        for i in range(len(parts) - 1):
            if not parts[i]:
                continue
            grp_name = parts[i]
            if grp_name not in grp:
                grp.create_group(grp_name)
            grp = grp[grp_name]
        grp.create_dataset(parts[-1], data=data)

# get minmax values for array and given range
def get_minmax(arr, range):
    data = arr[range[0]:range[1]]
    return Range(data.min(), data.max())


# return min, max indexes of the given array where array values fall 
# withing min/max val
def get_range(lat_arr, lon_arr, bbox, index_range=None):
    if lat_arr.shape[0] != lon_arr.shape[0]:
        raise ValueError("expected lat and lon arrays to have same shape")
    extent = lat_arr.shape[0]
    if extent == 0:
        return None
    if index_range is None:
        index_range = Range(0, extent)

    logging.debug(f"get_range {index_range.min}:{index_range.max}")

    lat_range = get_minmax(lat_arr, index_range)
    lon_range = get_minmax(lon_arr, index_range)

    # if entirely out of bbox, return None
    if lat_range.min > bbox.max_lat:
        return None
    if lat_range.max < bbox.min_lat:
        return None
    if lon_range.min > bbox.max_lon:
        return None
    if lon_range.max < bbox.min_lon:
        return None
    
    # if entirely in bbox, return current range
    if lat_range.min >= bbox.min_lat:
        if lat_range.max <= bbox.max_lat:
            if lon_range.min >= bbox.min_lon:
                if lon_range.max <= bbox.max_lon:
                    logging.debug('return inside bbox')
                    return index_range 
    
    mid_index = (index_range.min + index_range.max) // 2
    range_low =  get_range(lat_arr, lon_arr, bbox, index_range=Range(index_range.min, mid_index))
    range_high = get_range(lat_arr, lon_arr, bbox, index_range=Range(mid_index, index_range.max))
    if range_low is None:
        logging.debug("return range high")
        return range_high
    if range_high is None:
        logging.debug("return range low")
        return range_low
    # concatenate the two ranges
    logging.debug("return range concat")
    return Range(range_low.min, range_high.max)
    

# get min max index for the given lat/lon bounds
def get_index_range(fin, ground_track, bbox):
    logging.debug(f"get_index_range ground_track: {ground_track}")
    rp_lat = fin[f"{ground_track}/geolocation/reference_photon_lat"]
    rp_lat_arr = rp_lat[:]
    rp_lon = fin[f"{ground_track}/geolocation/reference_photon_lon"]
    rp_lon_arr = rp_lon[:]
    
    index_range = get_range(rp_lat_arr, rp_lon_arr, bbox)
    logging.debug("get_index_range, using index: {index_range}")
    if index_range:
        arr = rp_lat_arr[index_range[0]:index_range[1]]
        logging.debug(f"lat arr min: {arr.min():.2f} max: {arr.max():.2f}")
        rp_lon = fin[f"{ground_track}/geolocation/reference_photon_lon"]
        rp_lon_arr = rp_lon[index_range[0]:index_range[1]]
        logging.debug(f"lon arr min: {rp_lon_arr.min():.2f} max: {rp_lon_arr.max():.2f}")
        
    return index_range

# copy given index range from source dataset to destination dataset
def copy_dataset_range(fin, fout, h5path, index_range):
    dset_src = fin[h5path]
    parts = h5path.split("/")
    dset_name = parts[-1]
    extent = int(index_range.max - index_range.min)
    logging.info(f"creating dataset {dset_name} with extent {extent}")
    grp = fout
    for i in range(len(parts) - 1):
        if not parts[i]:
            continue
        grp_name = parts[i]
        if grp_name not in grp:
            grp.create_group(grp_name)
        grp = grp[grp_name]
    dt = dset_src.dtype
    # create the dataset
    shape = [extent,]
    # for multidimensional datasets, copy the remaing dimensions
    for n in dset_src.shape[1:]:
        shape.append(n)
    dset_des = grp.create_dataset(dset_name, dtype=dt, shape=shape)
    # copy data
    # TBD - paginate
    arr = dset_src[index_range.min:index_range.max]
    dset_des[:] = arr[:]

# sum up all the elements from 0 to index
def get_photon_count_range(fin, h5path, index_range):
    dset = fin[h5path]
    arr = dset[0:index_range.max]
    sum_base = np.sum(arr[0:index_range.min])
    sum_inc = np.sum(arr[index_range.min:index_range.max])
    sum_range = Range(sum_base, sum_base+sum_inc)
    logging.info(f"got photon count range {h5path} for {index_range} of {sum_range}")
    return sum_range


def save_georegion(fout, bbox):
    # save bbox lat/lon as attributes in root group
    # TBD: use CF convention?
    fout.attrs["min_lat"] = bbox.min_lat
    fout.attrs["max_lat"] = bbox.max_lat
    fout.attrs["min_lon"] = bbox.min_lon
    fout.attrs["max_lon"] = bbox.max_lon

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

#
# main
#
if len(sys.argv) > 1:
    run_number = int(sys.argv[1])
else:
    run_number = 1

# setup logging
logfname = config.get("log_file")
loglevel = get_loglevel()
logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
logging.debug(f"set log_level to {loglevel}")

input_dirname = config.get("input_foldername")
print(f"input_dirname: [{input_dirname}]")
if not input_dirname or input_dirname[-1] != '/':
    sys.exit("expected input_foldername to end with '/'")
input_filename = config.get("input_filename")
input_filepath = f"{input_dirname}{input_filename}"
logging.info(f"input filepath: {input_filepath}")
output_dirname = config.get("output_foldername")
page_buf_size_exp = 0
if input_filename.startswith("PAGE10MiB"):
    page_buf_size_exp = int(config.get("page_buf_size_exp"))
    logging.info(f"page_buf_size_exp: {page_buf_size_exp}")
    if page_buf_size_exp > 0:
        page_buf_size = 2 ** page_buf_size_exp
    else:
        page_buf_size = None  
else:
    page_buf_size = None
if not output_dirname or output_dirname[-1] != '/':
    sys.exit("expected output_foldername to end with '/'")
output_filename = config.get("output_filename")
output_filepath = f"{output_dirname}{output_filename}"
logging.info(f"output filepath: {output_filepath}")

min_lon = config.get("min_lon")

if min_lon < -180.0 or min_lon > 180.0:
    sys.exit(f"invalid min_lon value: {min_lon}")
  
max_lon = config.get("max_lon")
if max_lon < -180.0 or max_lon > 180.0 or max_lon <= min_lon:
    sys.exit(f"invalid max_lon value: {max_lon}")

min_lat = config.get("min_lat")
if min_lat < -90.0 or min_lat > 90.0:
    sys.exit(f"invalid min_lat value: {min_lat}")

max_lat = config.get("max_lat")
if max_lat < -90.0 or max_lat > 90.0 or max_lat <= min_lat:
    sys.exit(f"invalid max_lat value: {max_lat}")

logging.basicConfig(format='%(asctime)s %(message)s', level=loglevel)

bbox = BBox(min_lon, max_lon, min_lat, max_lat)

logging.info(f"lat range: {bbox.min_lat:.4f} - {bbox.max_lat:.4f}")
logging.info(f"lon range: {bbox.min_lon:.4f} - {bbox.max_lon:.4f}")

start_time = time.time()
dt = datetime.fromtimestamp(start_time)

with h5File(input_filepath) as fin, h5File(output_filepath, "w", page_buf_size=page_buf_size) as fout:
    copy_root_attrs(fin, fout)
    copy_scalar_datasets(fin, fout)
    save_georegion(fout, bbox)
    for ground_track in ground_tracks:
        grp = fout.create_group(ground_track)
        index_range = get_index_range(fin, ground_track, bbox)
        if not index_range:
            msg = f"no index range found for ground track: {ground_track}"
            logging.warning(msg)
            grp.attrs["index_range_min"] = -1
            grp.attrs["index_range_max"] = -1
            continue

        logging.info(f"got_index_range: {index_range[0]}:{index_range[1]}")
        grp.attrs["index_range_min"] = index_range[0]
        grp.attrs["index_range_max"] = index_range[1]
        # copy lat, lon, and photo count markers
        for ref_path in reference_datasets:
            h5path = f"{ground_track}/{ref_path}"
            copy_dataset_range(fin, fout, h5path, index_range)

        # sum up photon counts for later indexing
        h5path = f"{ground_track}/geolocation/segment_ph_cnt"
        count_range = get_photon_count_range(fin, h5path, index_range)
        
        logging.info(f"photon count_range: {count_range}")
        for ref_path in ph_count_datasets:
           h5path = f"{ground_track}/{ref_path}"
           copy_dataset_range(fin, fout, h5path, count_range)

stop_time = time.time()
dt = datetime.fromtimestamp(start_time)
start_str = dt.strftime("%Y-%m-%d %H:%M:%S")
dt = datetime.fromtimestamp(stop_time)
stop_str = dt.strftime("%Y-%m-%d %H:%M:%S")
elapsed = stop_time - start_time
machine = config.get("machine")
# print result for inclusion in benchmark csv
csv_str = f"{run_number}, {start_str}, {stop_str}, {elapsed:5.1f}, {machine}, "
csv_str += f"{input_dirname}, {output_dirname}, {input_filename},    , , , , , , "
if page_buf_size_exp > 0:
    csv_str += f"page_buf_size_exp: {page_buf_size_exp}"
print(csv_str)


        
 





