import sys
import logging
import s3fs
import h5py
import h5pyd
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


# check if hdf5 library version supports chunk iteration
hdf_library_version  = h5py.version.hdf5_version_tuple
library_has_chunk_iter = hdf_library_version >= (1, 14, 0)


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

def get_chunk_stats(dset):
    chunk_count = 0
    allocated_bytes = 0

    if library_has_chunk_iter:
        def init_chunktable_callback(chunk_info):
            # Use chunk offset as index 
            nonlocal chunk_count
            nonlocal allocated_bytes
            chunk_count += 1
            allocated_bytes +=  chunk_info[3]    
        dset.id.chunk_iter(init_chunktable_callback)
    else: 
        # Using old HDF5 version without H5Dchunk_iter
        spaceid = dset.id.get_space()
        chunk_count = dset.id.get_num_chunks(spaceid)

        for i in range(chunk_count):
            chunk_info = dset.id.get_chunk_info(i, spaceid)
            allocated_bytes += chunk_info[3]\
            
    return (chunk_count, allocated_bytes)
             
    

def get_dataset_stats(dset):
    logical_bytes = dset.dtype.itemsize
    for extent in dset.shape:
        logical_bytes *= extent
    
    if dset.chunks is None:
        chunk_size = logical_bytes
    else:
        chunk_size = dset.dtype.itemsize
        for extent in dset.chunks:
            chunk_size *= extent

    if dset.chunks is None:
        retval = (1, logical_bytes)

    else:
        retval = get_chunk_stats(dset)

    chunk_count = retval[0]
    allocated_bytes = retval[1]

    print(f"{dset.name}, {logical_bytes}, {chunk_size}, {chunk_count}, {allocated_bytes}")




#
# main
#

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

# print headers
print("name, logical_bytes, chunk_size, chunk_count, allocated_bytes")


with h5File(input_filepath) as f:
    for ground_track in ground_tracks:
        for ref_path in reference_datasets:
            h5path = f"{ground_track}/{ref_path}"
            dset = f[h5path]
            get_dataset_stats(dset)

        h5path = f"{ground_track}/geolocation/segment_ph_cnt"
        dset = f[h5path]
        get_dataset_stats(dset)
        
        for ref_path in ph_count_datasets:
           h5path = f"{ground_track}/{ref_path}"
           dset = f[h5path]
           get_dataset_stats(dset)