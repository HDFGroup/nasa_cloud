from collections import namedtuple

import sys
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
def get_range(lat_arr, lon_arr, bbox, range=None):
    if lat_arr.shape[0] != lon_arr.shape[0]:
        raise ValueError("expected lat and lon arrays to have same shape")
    extent = lat_arr.shape[0]
    if extent == 0:
        return None
    if range is None:
        range = Range(0, extent)

    logging.debug(f"get_range {range.min}:{range.max}")

    lat_range = get_minmax(lat_arr, range)
    lon_range = get_minmax(lon_arr, range)

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
                    return range 
    
    mid_index = (range.min + range.max) // 2
    range_low =  get_range(lat_arr, lon_arr, bbox, range=Range(range.min, mid_index))
    range_high = get_range(lat_arr, lon_arr, bbox, range=Range(mid_index, range.max))
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
    extent = index_range.max - index_range.min
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

def h5File(filepath, mode='r'):
    if filepath.startswith("hdf5://"):
        f = h5pyd.File(filepath, mode=mode)
    elif filepath.startswith("s3://"):
        if mode != 'r':
            raise ValueError("s3fs can only be used with read access mode")
        s3 = s3fs.S3FileSystem()
        f = h5py.File(s3.open(filepath, 'rb'), mode=mode)
    else:
        f = h5py.File(filepath, mode=mode)
    return f

#
# main
#

# setup logging
logfname = config.get("log_file")
loglevel = get_loglevel()
logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
logging.debug(f"set log_level to {loglevel}")

input_dirname = config.get("input_foldername")
input_filename = config.get("input_filename")
input_filepath = f"{input_dirname}{input_filename}"
logging.info(f"input filepath: {input_filepath}")
output_dirname = config.get("output_foldername")
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


with h5File(input_filepath) as fin, h5File(output_filepath, "w") as fout:
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
        
 





