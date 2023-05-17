import sys
import os
import h5py
import s3fs


if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
    sys.exit(f"usage: python {sys.argv[0]} <filepath or s3uri>")

print(f"h5py version: {h5py.version.version}")
print(f"hdf5 version: {h5py.version.hdf5_version}")

filepath = sys.argv[1]

kwargs = {}

ros3_params = {"aws_region": "AWS_REGION",
               "secret_id": "AWS_ACCESS_KEY_ID",
               "secret_key": "AWS_SECRET_ACCESS_KEY"}

if filepath.startswith("s3://"):
    s3 = s3fs.S3FileSystem()
    f = h5py.File(s3.open(filepath, 'rb'), **kwargs)
elif filepath.startswith("http"):
    kwargs["driver"] = "ros3"
    for ros3_param in ros3_params:
        env_name = ros3_params[ros3_param]
        if env_name not in os.environ:
            sys.exit(f"env variable: {env_name} must be set for ros3")
        env_value = os.environ[env_name]
        kwargs[ros3_param] = env_value.encode("utf-8")
    f = h5py.File(filepath, **kwargs)
else:
    f = h5py.File(filepath, **kwargs)
    
print(f"got file: {f}")
fcpl = f.id.get_create_plist()
page_size = fcpl.get_file_space_page_size()
print(f"page size: {page_size}")

fapl = f.id.get_access_plist()
page_buffer_size = fapl.get_page_buffer_size()
print(f"page buffer size: {page_buffer_size}")
f.close()

