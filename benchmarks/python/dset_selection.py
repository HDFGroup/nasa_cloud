import h5pyd as h5py

filepath = "hdf5://home/test_user1/icesat2/ATL03_20181017222812_02950102_005_01.h5"  # native
# filepath =  "hdf5://home/test_user1/icesat2/linked/ATL03_20181017222812_02950102_005_01.h5"  # linked
# filepath = "hdf5://home/test_user1/icesat2/irangeget/ATL03_20181017222812_02950102_005_01.h5" # hyperchunked

f = h5py.File(filepath, use_cache=False)
dset = f["/gt3r/heights/h_ph"]
print(dset)
print(f"chunk shape: {dset.chunks}")
print(f"compression: {dset.compression}")

BLOCK_SIZE = 320000
extent = dset.shape[0]

start = 0
while (start < extent): 
    stop = start + BLOCK_SIZE 
    if stop > extent:
        stop = extent
    arr = dset[start:stop]
    print(f"{arr.min():.2f} {arr.max():.2f} {arr.mean():.2f}")
    start += BLOCK_SIZE

