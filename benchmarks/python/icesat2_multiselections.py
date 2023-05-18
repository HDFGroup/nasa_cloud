from collections import namedtuple
import sys
import logging
import s3fs
import h5py
import h5pyd
import numpy as np
import config
from object_manager import ObjectManager

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

#
# h5py(d) futures
#
def create_groups(parent, group_names):
    """ create the specified sub-groups """
    for group_name in group_names:
        parent.create_group(group_name)

def get_objects(parent, h5paths):
    """ return a list of object references for each of 
        the given h5paths"""
    objs = []
    for h5path in h5paths:
        try:
            obj = parent[h5path]
        except KeyError:
            # not found
            obj = None
        objs.append(obj)


def set_attributes(parent, items):
    """ create/update attributes given by the item list """
    for item in items:
        if "name" not in item:
            raise KeyError("name key not found")
        attr_name = item["name"]
        if "value" not in item:
            raise KeyError("value key not found")
        attr_value = item["value"]
        if "obj" in item:
            obj = item["obj"]
        else:
            obj = parent
        obj.attrs[attr_name] = attr_value

def get_attributes(parent, items):
    """ get attributes given by the item list """
    values = []
    for item in items:
        if "name" not in item:
            raise KeyError("name key not found")
        attr_name = item["name"]
        if "obj" in item:
            obj = item["obj"]
        else:
            obj = parent
        value = obj.attrs[attr_name]
        values.append(value)

def dataset_write_selections(parent, items, sel=None, data=None):
    """ write values to one or more datasets """
    for item in items:
        if "sel" not in item:
            raise KeyError("selection key not found")
        if "sel" in item:
            item_sel = item["sel"]
        else:
            item_sel = sel

        if "data" in item:
            item_data = item["sel"]
        else:
            item_data = data

        if "obj" in item:
            obj = item["obj"]
        else:
            obj = parent
        obj.__setitem__(item_sel, item_data)

def dataset_read_selections(parent, items, sel=None):
    """ read values one or more datasets """
    values = []
    for item in items:
        if "sel"  in item:
            item_sel = item["sel"]
        else:
            item_sel = sel
        
        if "obj" in item:
            obj = item["obj"]
        else:
            obj = parent
        value = obj.__getitem__(item_sel)
        values.append(value)
        
    

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
def get_minmax(arr, index_range):
    data = arr[index_range[0]:index_range[1]]
    return Range(data.min(), data.max())


# return min, max indexes of the given array where array values fall 
# within min/max val
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
def get_index_ranges(mm_in, bbox):
    logging.debug(f"get_index_range ground_tracks: {mm_in.names}")
    rp_lat_mm = ObjectManager(mm_in, ["geolocation/reference_photon_lat",])
    rp_lat_arrs = rp_lat_mm.read_all()
    rp_lon_mm = ObjectManager(mm_in, ["/geolocation/reference_photon_lon",])
    rp_lon_arrs = rp_lon_mm.read_all()

    index_ranges = []
    
    for i in range(len(rp_lat_arrs)):
        rp_lat_arr = rp_lat_arrs[i]
        rp_lon_arr = rp_lon_arrs[i]
        index_range = get_range(rp_lat_arr, rp_lon_arr, bbox)
        logging.debug(f"get_index_set for array {mm_in.names[i]}, using range: {index_range}")        
        arr = rp_lat_arr.__getitem__(slice(index_range[0], index_range[1]))
        logging.debug(f"lat arr min: {arr.min():.2f} max: {arr.max():.2f}")
        arr = rp_lon_arr.__getitem__(slice(index_range[0], index_range[1]))
        logging.debug(f"lon arr min: {arr.min():.2f} max: {arr.max():.2f}")
        index_ranges.append(index_range)
        
    return index_ranges

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
    elif filepath.startswith("http"):
        # use ros3 driver
        aws_region = config.get("aws_region").encode("utf-8")
        secret_id = config.get("aws_access_key_id").encode("utf-8")
        secret_key = config.get("aws_secret_access_key").encode("utf-8")
        f = h5py.File(filepath, mode=mode, driver="ros3", aws_region=aws_region, secret_id=secret_id, secret_key=secret_key)
    else:
        f = h5py.File(filepath, mode=mode)
    return f


def process(fin, fout, bbox):
    copy_root_attrs(fin, fout)
    copy_scalar_datasets(fin, fout)
    save_georegion(fout, bbox)

    mm_in = ObjectManager(fin, ground_tracks)
    mm_out = ObjectManager(fout)
    mm_out.create_groups(ground_tracks)
    ranges = get_index_ranges(mm_in, bbox)
    print("ranges:", ranges)


    """
                     
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
    """

#
# main
#

# setup logging
logfname = config.get("log_file")
loglevel = get_loglevel()
logging.basicConfig(filename=logfname, format='%(levelname)s %(asctime)s %(message)s', level=loglevel)
logging.debug(f"set log_level to {loglevel}")

input_dirname = config.get("input_foldername")
if not input_dirname or input_dirname[-1] != '/':
    sys.exit("expected input_foldername to end with '/'")
input_filename = config.get("input_filename")
input_filepath = f"{input_dirname}{input_filename}"
logging.info(f"input filepath: {input_filepath}")
output_dirname = config.get("output_foldername")
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



with h5File(input_filepath) as fin, h5File(output_filepath, "w") as fout:
    process(fin, fout, bbox=bbox)

print("done!")