#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <yaml.h>
#include <math.h>

#include "hdf5.h"

#define SUCCEED 0
#define FAIL (-1)

#define FILEPATH_BUFFER_SIZE 1024

#define PATH_DELIMITER "/"

#include "rest_vol_public.h"

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (compatible with v1 and v2 errors)
 */
#define FUNC_GOTO_ERROR(err_msg)      \
	fprintf(stderr, "%s\n", err_msg); \
	fprintf(stderr, "\n");            \
	exit(1);

// TODO Maybe replace this with something more lightweight
/* Print if program is run with debug flag */
#define PRINT_DEBUG(...)              \
	if (debug)                        \
	{                                 \
		fprintf(stdout, __VA_ARGS__); \
	}

#define CONFIG_FILENAME "../config/config.yml"

#define PATH_PREFIX "/home/test_user1/"

bool debug = false;
bool check_output = false;
bool readonly = false;
bool use_ros3 = false;
bool use_rest_vol = false;

char *ground_tracks[] = {"gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r", 0};

const char *scalar_datasets[] = {"/orbit_info/sc_orient",
								 "/ancillary_data/start_rgt",
								 "/ancillary_data/start_cycle",
								 0};

char *reference_datasets[] = {"geolocation/reference_photon_lat",
							  "geolocation/reference_photon_lon",
							  "geolocation/segment_ph_cnt",
							  0};

char *ph_count_datasets[] = {
	"heights/dist_ph_along",
	"heights/h_ph",
	"heights/signal_conf_ph",
	"heights/quality_ph",
	"heights/lat_ph",
	"heights/lon_ph",
	"heights/delta_time",
	0};

const char *geolocation_lat = "/geolocation/reference_photon_lat";
const char *geolocation_lon = "/geolocation/reference_photon_lon";

typedef struct BBox
{
	double min_lon;
	double max_lon;
	double min_lat;
	double max_lat;
} BBox;

typedef struct Range_Indices
{
	size_t min;
	size_t max;
} Range_Indices;

typedef struct Range_Doubles
{
	double min;
	double max;
} Range_Doubles;

typedef struct ConfigValues
{
	char *loglevel;
	char *logfile;
	char *input_foldername;
	char *input_filename;
	char *output_foldername;
	char *output_filename;

	double min_lat;
	double max_lat;
	double min_lon;
	double max_lon;

	int page_buf_size_exp;
} ConfigValues;

typedef enum ConfigType
{
	CONFIG_UNKNOWN_T,
	CONFIG_STRING_T,
	CONFIG_DOUBLE_T,
	CONFIG_INT_T
} ConfigType;

/* Copy each attribute from fin to the file whose hid_t is pointed to by fout_data */
herr_t copy_attr_callback(hid_t fin, const char *attr_name, const H5A_info_t *ainfo, void *fout_data)
{
	herr_t ret_value = SUCCEED;

	hid_t fout = *((hid_t *)fout_data);
	hid_t fin_aapl_id = H5I_INVALID_HID;
	hid_t fin_attr = H5I_INVALID_HID;

	hid_t acpl_id = H5I_INVALID_HID;
	hid_t dtype_id = H5I_INVALID_HID;
	hid_t dstype_id = H5I_INVALID_HID;
	hid_t fout_attr = H5I_INVALID_HID;

	size_t dtype_size = 0;
	size_t num_elems = 0;

	void *attr_data = NULL;

	if ((fin_attr = H5Aopen(fin, attr_name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("can't open file attribute in copy callback");
	}

	if ((dtype_id = H5Aget_type(fin_attr)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to get datatype of attribute")
	}

	if ((dstype_id = H5Aget_space(fin_attr)) < 0)
	{
		FUNC_GOTO_ERROR("Failed to get dataspace of attribute")
	}

	if ((acpl_id = H5Aget_create_plist(fin_attr)) < 0)
	{
		FUNC_GOTO_ERROR("Failed to get acpl")
	}

	if ((dtype_size = H5Tget_size(dtype_id)) == 0)
	{
		FUNC_GOTO_ERROR("Failed to get size of datatype")
	}

	if ((num_elems = H5Sget_simple_extent_npoints(dstype_id)) < 0)
	{
		FUNC_GOTO_ERROR("Failed to get number of elements")
	}

	if ((attr_data = malloc(dtype_size * num_elems)) == 0)
	{
		FUNC_GOTO_ERROR("Failed to allocate memory for dtype")
	}

	if (H5Aread(fin_attr, dtype_id, attr_data) < 0)
	{
		FUNC_GOTO_ERROR("Failed to read from attribute")
	}

	if (!readonly)
	{
		if ((fout_attr = H5Acreate(fout, attr_name, dtype_id, dstype_id, acpl_id, H5P_DEFAULT)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to create attribute in output file")
		}

		if (H5Awrite(fout_attr, dtype_id, attr_data) < 0)
		{
			FUNC_GOTO_ERROR("Failed to write to copied attribute")
		}

		if (H5Aclose(fout_attr) < 0)
		{
			FUNC_GOTO_ERROR("Failed to close output attribute")
		}
	}

	free(attr_data);

	if (H5Aclose(fin_attr) < 0)
	{
		FUNC_GOTO_ERROR("Failed to close input attribute")
	}

	return ret_value;
}

/* Copy datasets and their paths from file fin to file fout */
herr_t copy_scalar_datasets(hid_t fin, hid_t fout)
{
	hid_t parent_group = H5I_INVALID_HID;
	hid_t child_group = H5I_INVALID_HID;

	hid_t dset = H5I_INVALID_HID;
	hid_t dtype = H5I_INVALID_HID;
	hid_t dstype = H5I_INVALID_HID;
	hid_t dcpl = H5I_INVALID_HID;
	hid_t dapl = H5I_INVALID_HID;
	hid_t copied_scalar_dataset = H5I_INVALID_HID;

	size_t num_elems = 0;
	size_t elem_size = 0;

	char *prev_group_name;
	char *group_name;
	char *dset_name;
	char *dset_path;
	char dset_path_buffer[FILEPATH_BUFFER_SIZE];

	const char **current_dset = scalar_datasets;

	void *data = NULL;

	/* Iterate over path to make sure all parent groups exist and get dset name */
	while (*current_dset != 0)
	{

		/* Copy the dataset path for strtok */
		dset_path = &(dset_path_buffer[0]);
		strncpy(dset_path, *current_dset, strlen(*current_dset) + 1);

		PRINT_DEBUG("Copying scalar dset %s\n", dset_path);

		/* Separate filepath into groups */
		group_name = strtok_r(dset_path, PATH_DELIMITER, &dset_path);

		parent_group = fout;

		/* Until end of path */
		if (!readonly)
		{
			while (group_name != NULL)
			{
				/* If the end of the path is reached, keep track of dset name and do not create it as a group*/
				prev_group_name = group_name;
				if ((group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path)) == NULL)
				{
					dset_name = prev_group_name;
					break;
				}

				/* If this group does not exist, attempt to create it */
				H5E_BEGIN_TRY
				{
					if ((child_group = H5Gopen(parent_group, prev_group_name, H5P_DEFAULT)) == H5I_INVALID_HID)
					{
						if ((child_group = H5Gcreate(parent_group, prev_group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) == H5I_INVALID_HID)
						{
							FUNC_GOTO_ERROR("Failed to create child group")
						}
					}
				}
				H5E_END_TRY

				/* Only close intermediate groups that were just created, not the file itself */
				if (parent_group != fout)
				{
					if (H5Gclose(parent_group) < 0)
					{
						FUNC_GOTO_ERROR("Failed to create group")
					}
				}

				parent_group = child_group;
			}
		}

		/* Access information about dset */
		if ((dset = H5Dopen(fin, *current_dset, H5P_DEFAULT)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to open dset")
		}

		if ((dtype = H5Dget_type(dset)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to get dtype")
		}

		if ((dstype = H5Dget_space(dset)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to get dstype")
		}

		if ((dcpl = H5Dget_create_plist(dset)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to get dcpl")
		}

		if ((dapl = H5Dget_access_plist(dset)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to get dapl")
		}

		if ((num_elems = H5Sget_simple_extent_npoints(dstype)) < 0)
		{
			FUNC_GOTO_ERROR("Failed to get number of elements")
		}

		if ((elem_size = H5Tget_size(dtype)) == 0)
		{
			FUNC_GOTO_ERROR("Failed to get dtype size")
		}

		data = calloc(num_elems, elem_size);

		if (H5Dread(dset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0)
		{
			FUNC_GOTO_ERROR("Failed to read dataset while copying scalar")
		}

		if (!readonly)
		{
			if ((copied_scalar_dataset = H5Dcreate(parent_group, dset_name, dtype, dstype, H5P_DEFAULT, dcpl, dapl)) == H5I_INVALID_HID)
			{
				FUNC_GOTO_ERROR("Failed to create dset")
			}

			if (H5Dwrite(copied_scalar_dataset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0)
			{
				FUNC_GOTO_ERROR("Failed to write to dataset while copying scalar")
			}

			if (H5Dclose(copied_scalar_dataset) < 0)
			{
				FUNC_GOTO_ERROR("Failed to close copied scalar dataset")
			}
		}

		free(data);

		if (H5Dclose(dset) < 0)
		{
			FUNC_GOTO_ERROR("Failed to close scalar dset")
		}

		if (H5Pclose(dcpl) < 0)
		{
			FUNC_GOTO_ERROR("Failed to close dcpl")
		}

		if (H5Pclose(dapl) < 0)
		{
			FUNC_GOTO_ERROR("Failed to close dapl")
		}

		++current_dset;
	}
}

/* Copy any attributes from input root group to output root group
 * Return non-negative on success, negative on failure. */
herr_t copy_root_attrs(hid_t fin, hid_t fout)
{
	herr_t ret_value = SUCCEED;
	H5Aiterate(fin, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attr_callback, &fout);
	return ret_value;
}

/* Get min and max values for the given array within the given range*/
Range_Doubles get_minmax(double arr[], Range_Indices range)
{
	Range_Doubles out_range;

	PRINT_DEBUG("Minmax search range is %ld - %ld\n", range.min, range.max)

	/* Initialize fields */
	out_range.min = arr[range.min];
	out_range.max = arr[range.min];

	for (size_t i = range.min; i < range.max; i++)
	{
		double elem = arr[i];

		if (elem < out_range.min)
		{
			out_range.min = elem;
		}

		if (elem > out_range.max)
		{
			out_range.max = elem;
		}
	}

	PRINT_DEBUG("Minmax values of array in the range are %lf, %lf\n", out_range.min, out_range.max)

	return out_range;
}

/* Return the lowest and highest indices of the given array where the lat/lon values fall within the given bounding box */
Range_Indices *get_range(double lat_arr[], size_t lat_size, double lon_arr[], size_t lon_size,
						 BBox *bbox, Range_Indices *range)
{
	Range_Indices *ret_range = malloc(sizeof(*range));
	ret_range->min = 0;
	ret_range->max = 0;

	Range_Indices default_range;
	default_range.min = 0;
	default_range.max = lat_size;

	if (lat_size != lon_size)
	{
		FUNC_GOTO_ERROR("expected lat and lon arrays to have same shape")
	}

	if (range == NULL)
	{
		range = &default_range;
	}

	PRINT_DEBUG("get_range range has min %zu and max %zu\n", range->min, range->max)

	Range_Doubles lat_range = get_minmax(lat_arr, *range);
	Range_Doubles lon_range = get_minmax(lon_arr, *range);

	/* If entirely outside bbox, return NULL */
	if (lat_range.min > bbox->max_lat ||
		lat_range.max < bbox->min_lat ||
		lon_range.min > bbox->max_lon ||
		lon_range.max < bbox->min_lon)
	{
		PRINT_DEBUG("%s\n", "Entirely outside bbox")
		free(ret_range);
		ret_range = NULL;
	}
	else if (lat_range.min >= bbox->min_lat &&
			 lat_range.max <= bbox->max_lat &&
			 lon_range.min >= bbox->min_lon &&
			 lon_range.max <= bbox->max_lon)
	{
		PRINT_DEBUG("%s\n", "Entirely within bbox")
		memcpy(ret_range, range, sizeof(*range));
	}
	else
	{
		/* If entirely in bbox, return current range */
		size_t middle_index = (size_t)((range->min + range->max) / 2);

		Range_Indices range_select_low;
		range_select_low.min = range->min;
		range_select_low.max = middle_index;
		Range_Indices *range_low = get_range(lat_arr, lat_size, lon_arr, lon_size, bbox, &range_select_low);

		Range_Indices range_select_high;
		range_select_high.min = middle_index;
		range_select_high.max = range->max;
		Range_Indices *range_high = get_range(lat_arr, lat_size, lon_arr, lon_size, bbox, &range_select_high);

		if (range_low == NULL)
		{
			PRINT_DEBUG("Return range high\n")
			ret_range->min = range_high->min;
			ret_range->max = range_high->max;
			free(range_high);
			return ret_range;
		}
		else if (range_high == NULL)
		{
			PRINT_DEBUG("Return range low\n")
			ret_range->min = range_low->min;
			ret_range->max = range_low->max;
			free(range_low);
			return ret_range;
		}

		/* If neither is empty, concatenate the ranges */
		PRINT_DEBUG("Concatenating ranges\n")
		ret_range->min = range_low->min;
		ret_range->max = range_high->max;

		free(range_low);
		free(range_high);
	}

	return ret_range;
}

/* Get min/max index for the given lat/lon bounds */
Range_Indices *get_index_range(hid_t fin, char *ground_track, BBox *bbox)
{
	PRINT_DEBUG("get_index_range with ground_track = %s\n", ground_track)

	hid_t lat_dset = H5I_INVALID_HID;
	hid_t lon_dset = H5I_INVALID_HID;

	/* Read data from dsets into arrays */
	char *lat_dset_name = malloc(strlen(ground_track) + strlen(geolocation_lat) + 1);
	strncpy(lat_dset_name, ground_track, strlen(ground_track) + 1);
	strcat(lat_dset_name, geolocation_lat);

	if ((lat_dset = H5Dopen(fin, lat_dset_name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to open lat datset")
	}

	hid_t dspace_id = H5Dget_space(lat_dset);
	hssize_t num_elems_lat = H5Sget_simple_extent_npoints(dspace_id);
	PRINT_DEBUG("Number of elements in lat dataset is %zu\n", num_elems_lat)
	double *lat_arr = malloc(sizeof(double) * num_elems_lat);

	if (H5Dread(lat_dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, lat_arr) < 0)
	{
		FUNC_GOTO_ERROR("Failed to read from lat dataset")
	}

	/* Sanity check for testing */
	/*
	for (size_t i = 0; i < num_elems_lat; i++) {
		if (lat_arr[i] != 0) {
			break;
		}

		if (i == num_elems_lat - 1) {
			FUNC_GOTO_ERROR("All lat elems read as zero!")
		}
	}
	*/

	char *lon_dset_name = malloc(strlen(ground_track) + strlen(geolocation_lon) + 1);
	strncpy(lon_dset_name, ground_track, strlen(ground_track) + 1);
	strcat(lon_dset_name, geolocation_lon);

	if ((lon_dset = H5Dopen(fin, lon_dset_name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to open lon dataset")
	}

	dspace_id = H5Dget_space(lon_dset);
	hssize_t num_elems_lon = H5Sget_simple_extent_npoints(dspace_id);

	double *lon_arr = malloc(sizeof(double) * num_elems_lon);

	if (H5Dread(lon_dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, lon_arr) < 0)
	{
		FUNC_GOTO_ERROR("Failed to read from lon dataset")
	}

	Range_Indices *index_range = get_range(lat_arr, num_elems_lat, lon_arr, num_elems_lon, bbox, NULL);

	if (index_range)
	{
		PRINT_DEBUG("get_index_range using index with min %zu and max %zu\n", index_range->min, index_range->max)
	}

	H5Dclose(lon_dset);
	H5Dclose(lat_dset);
	free(lat_arr);
	free(lat_dset_name);
	free(lon_dset_name);
	free(lon_arr);

	return index_range;
}

/* Copy given index range from source dataset to destination dataset */
void copy_dataset_range(hid_t fin, hid_t fout, char *h5path, Range_Indices *index_range)
{
	hid_t source_dset = H5I_INVALID_HID;
	hid_t child_group = H5I_INVALID_HID;
	hid_t parent_group = H5I_INVALID_HID;
	hid_t copy_dset = H5I_INVALID_HID;

	hid_t dtype = H5I_INVALID_HID;
	hid_t file_dataspace = H5I_INVALID_HID;
	hid_t memory_dataspace = H5I_INVALID_HID;
	hid_t dcpl = H5I_INVALID_HID;
	hid_t dapl = H5I_INVALID_HID;

	hsize_t *stride_arr = NULL;
	hsize_t *block_size_arr = NULL;
	hsize_t *start_arr = NULL;

	int ndims = 0;
	hsize_t *dims = NULL;

	char *prev_group_name;
	char *group_name;
	char *dset_name;
	char *dset_path;
	char dset_path_buffer[FILEPATH_BUFFER_SIZE];

	void *data;

	size_t extent = index_range->max - index_range->min;
	size_t total_num_elems = 1;
	size_t elem_size = 0;

	PRINT_DEBUG("Creating dataset %s with extent %lu\n", h5path, extent)

	/* Copy scalar dataset paths to stack for strtok */
	dset_path = &(dset_path_buffer[0]);
	strncpy(dset_path, h5path, strlen(h5path) + 1);

	parent_group = fout;

	/* Separate filepath into groups */
	group_name = strtok_r(dset_path, PATH_DELIMITER, &dset_path);

	if (!readonly)
	{
		while (group_name != NULL)
		{

			// If the end of the path is reached, keep track of dset name and don't create it as a group
			prev_group_name = group_name;

			if ((group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path)) == NULL)
			{
				dset_name = prev_group_name;
				break;
			}

			H5E_BEGIN_TRY
			{
				if ((child_group = H5Gopen(parent_group, prev_group_name, H5P_DEFAULT)) == H5I_INVALID_HID)
				{
					if ((child_group = H5Gcreate(parent_group, prev_group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) == H5I_INVALID_HID)
					{
						FUNC_GOTO_ERROR("Failed to create child group")
					}
				}
			}
			H5E_END_TRY

			// Only close intermediate groups that were just created, not the file itself
			if (parent_group != fout)
			{
				if (H5Gclose(parent_group) < 0)
				{
					FUNC_GOTO_ERROR("Failed to close parent group")
				}
			}

			parent_group = child_group;
		}
	}

	/* Copy the data in the source dataset to a new dataset*/

	/* Access data from old dset */
	if ((source_dset = H5Dopen(fin, h5path, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to open source dataset")
	}

	if ((dtype = H5Dget_type(source_dset)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to get dtype")
	}

	if ((file_dataspace = H5Dget_space(source_dset)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to get dataspace from source")
	}

	if ((ndims = H5Sget_simple_extent_ndims(file_dataspace)) < 0)
	{
		FUNC_GOTO_ERROR("Failed to get number of dims")
	}

	/* Create memory dataspace */

	dims = calloc(ndims, sizeof(hsize_t));

	// TODO - should be possible for this to be multidimensional?
	if (H5Sget_simple_extent_dims(file_dataspace, dims, NULL) <= 0)
	{
		FUNC_GOTO_ERROR("Failed to get dataspace dim size")
	}

	dims[0] = extent;

	if ((memory_dataspace = H5Screate_simple(ndims, dims, NULL)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to create simple dataspace")
	}

	/* Create file dataspace */
	start_arr = calloc(ndims, sizeof(hsize_t));
	stride_arr = calloc(ndims, sizeof(size_t));
	block_size_arr = calloc(ndims, sizeof(size_t));

	for (size_t i = 0; i < ndims; i++)
	{
		if (i == 0)
		{
			start_arr[i] = index_range->min;
		}
		else
		{
			start_arr[i] = 0;
		}

		stride_arr[i] = 1;
		block_size_arr[i] = 1;
	}

	if (0 > H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET, start_arr, stride_arr, dims, block_size_arr))
	{
		FUNC_GOTO_ERROR("Failed to select hyperslab in get_photon_count_range")
	}

	/* Get remaining plists */
	if ((dcpl = H5Dget_create_plist(source_dset)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to get dcpl")
	}

	if ((dapl = H5Dget_access_plist(source_dset)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to get dapl")
	}

	/* Store entire dataset as one chunk */
	if (H5Pset_chunk(dcpl, ndims, dims) < 0)
	{
		FUNC_GOTO_ERROR("Failed to set chunk size")
	}

	// if (H5Pset_layout(dcpl, H5D_CONTIGUOUS) < 0) {
	//	FUNC_GOTO_ERROR("Failed to make layout contiguous")
	// }

	for (size_t i = 0; i < ndims; i++)
	{
		total_num_elems *= dims[i];
	}

	hid_t native_dtype = H5Tget_native_type(dtype, H5T_DIR_DEFAULT);

	if ((elem_size = H5Tget_size(native_dtype)) == 0)
	{
		FUNC_GOTO_ERROR("Failed to get size of dtype")
	}

	data = calloc(total_num_elems, elem_size);

	/* Read and copy selected data */

	PRINT_DEBUG("Attempting to read %s, with a dataspace whose first dim is size %zu and whose elem size is %zu, into a buffer of size %zu\n", h5path, dims[0], elem_size, elem_size * total_num_elems)

	if (H5Dread(source_dset, native_dtype, memory_dataspace, file_dataspace, H5P_DEFAULT, data) < 0)
	{
		FUNC_GOTO_ERROR("Failed to read from dset with hyperslab selection")
	}

	if (!readonly)
	{
		PRINT_DEBUG("Attempting to write to copy of %s\n", h5path)

		if ((copy_dset = H5Dcreate(parent_group, dset_name, dtype, memory_dataspace, H5P_DEFAULT, dcpl, dapl)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to create copy dset")
		}

		/* mem_space_id is H5S_ALL so that memory_dataspace is used for filespace and memory space */
		if (H5Dwrite(copy_dset, native_dtype, H5S_ALL, memory_dataspace, H5P_DEFAULT, data) < 0)
		{
			FUNC_GOTO_ERROR("Failed to write data when copying range")
		}

		if (H5Dclose(copy_dset) < 0)
		{
			FUNC_GOTO_ERROR("Failed to close copy dset")
		}
	}

	free(start_arr);
	free(stride_arr);
	free(block_size_arr);
	free(data);
	free(dims);

	if (H5Pclose(dcpl) < 0)
	{
		FUNC_GOTO_ERROR("Failed to close dcpl")
	}

	if (H5Pclose(dapl) < 0)
	{
		FUNC_GOTO_ERROR("Failed to close dapl")
	}
}

/* Sum up elements from 0 to index in given dataset*/
Range_Indices *get_photon_count_range(hid_t fin, char *h5path, Range_Indices *range)
{
	Range_Indices *ret_range = malloc(sizeof(Range_Indices));
	hid_t dset;
	hid_t fspace;
	int *data;
	size_t sum_base = 0;
	size_t sum_inc = 0;

	PRINT_DEBUG("Counting photo for %s from %zu to %zu\n", h5path, range->min, range->max)

	if (H5I_INVALID_HID == (dset = H5Dopen(fin, h5path, H5P_DEFAULT)))
	{
		FUNC_GOTO_ERROR("Failed to open dset in get_photon_count_range")
	}

	/* Create hyperslab selection to read up to range max */
	fspace = H5Dget_space(dset);
	hid_t photon_num_elems = H5Sget_simple_extent_npoints(fspace);

	if (0 > H5Sselect_hyperslab(fspace, H5S_SELECT_SET, (hsize_t[]){0},
								(hsize_t[]){1},
								(hsize_t[]){range->max},
								(hsize_t[]){1}))
	{
		FUNC_GOTO_ERROR("Failed to select hyperslab in get_photon_count_range")
	}

	hid_t dtype = H5Dget_type(dset);
	hid_t native = H5Tget_native_type(dtype, H5T_DIR_DEFAULT);

	data = calloc(range->max, sizeof(int));

	if (0 > H5Dread(dset, native, H5S_ALL, fspace, H5P_DEFAULT, data))
	{
		FUNC_GOTO_ERROR("Failed to read from data in get_photon_count_range")
	}

	for (size_t i = 0; i < range->min; i++)
	{
		sum_base = sum_base + (size_t)data[i];
	}

	for (size_t j = range->min; j < range->max; j++)
	{
		sum_inc = sum_inc + (size_t)data[j];
	}

	ret_range->min = sum_base;
	ret_range->max = sum_base + sum_inc;

	PRINT_DEBUG("Got photon count range %s for (%zu, %zu) of (%zu, %zu)\n", h5path, range->min, range->max, ret_range->min, ret_range->max)

	free(data);
	H5Dclose(dset);
	return ret_range;
}

// TODO Move process_layer and get_config_values to another file

/* Process one value from the yaml file. If the value is determined to be a keyname,
 *	this will recurse to assign the next parsed value as its keyvalue.
 *
 *	If type is CONFIG_UNKNOWN_T, then it expects to be the outermost layer, and for the next value to be a keyname.
 *	Otherwise, it expects to be a recursion, and for the next value to be a keyvalue of ConfigType type.
 */
void process_layer(yaml_parser_t *parser, void *storage_location, ConfigType type)
{
	ConfigValues *config2;
	ConfigType new_type;
	yaml_event_t event;
	yaml_char_t *value;

	if (type == CONFIG_UNKNOWN_T)
		config2 = (ConfigValues *)storage_location;

	yaml_parser_parse(parser, &event);
	value = event.data.scalar.value;

	while (event.type != YAML_STREAM_END_EVENT)
	{

		switch (event.type)
		{
		case YAML_SCALAR_EVENT:
			switch (type)
			{
			case CONFIG_STRING_T:
			{
				char *storage_loc = (char *)storage_location;
				strcpy(storage_loc, value);

				PRINT_DEBUG("Key assigned value %s from original %s\n", storage_loc, value)
				break;
			}
			case CONFIG_DOUBLE_T:
			{
				double dval = strtod((char *)value, NULL);

				double *storage_loc = (double *)storage_location;
				*storage_loc = dval;

				PRINT_DEBUG("Key assigned value %lf\n", *((double *)storage_location))
				break;
			}
			case CONFIG_INT_T:
			{
				int intval = strtod((char *)value, NULL);

				int *storage_loc = (int *)storage_location;
				*storage_loc = intval;
				PRINT_DEBUG("Key assigned value %d\n", *((int *)storage_location))

				break;
			}
			default:
			{
				void *next_storage_location = NULL;

				if (!strcmp("loglevel", value))
				{
					next_storage_location = config2->loglevel;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("logfile", value))
				{
					next_storage_location = config2->logfile;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("input_foldername", value))
				{
					next_storage_location = config2->input_foldername;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("input_filename", value))
				{
					next_storage_location = config2->input_filename;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("output_foldername", value))
				{
					next_storage_location = config2->output_foldername;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("output_filename", value))
				{
					next_storage_location = config2->output_filename;
					new_type = CONFIG_STRING_T;
				}
				else if (!strcmp("min_lat", value))
				{
					next_storage_location = (void *)&(config2->min_lat);
					new_type = CONFIG_DOUBLE_T;
				}
				else if (!strcmp("max_lat", value))
				{
					next_storage_location = (void *)&(config2->max_lat);
					new_type = CONFIG_DOUBLE_T;
				}
				else if (!strcmp("min_lon", value))
				{
					next_storage_location = (void *)&(config2->min_lon);
					new_type = CONFIG_DOUBLE_T;
				}
				else if (!strcmp("max_lon", value))
				{
					next_storage_location = (void *)&(config2->max_lon);
					new_type = CONFIG_DOUBLE_T;
				}
				else if (!strcmp("page_buf_size_exp", value))
				{
					next_storage_location = (void *)&(config2->page_buf_size_exp);
					new_type = CONFIG_INT_T;
				}
				else
				{
					PRINT_DEBUG("Key named %s not found, skipping\n", value)
					break;
				}

				PRINT_DEBUG("%s%s\n", "Recursing in yaml parsing to assign key ", value)
				process_layer(parser, next_storage_location, new_type);
				break;
			}
			}

			break;
		}

		/* yaml_parser_parse allocates memory for event that must be freed */
		yaml_event_delete(&event);

		/* If this is a recursion, return to next layer up */
		if (type != CONFIG_UNKNOWN_T)
		{
			break;
		}

		yaml_parser_parse(parser, &event);
		value = event.data.scalar.value;
	}
}

/* Allocate internal memory for config and begin parsing the yaml file. */
ConfigValues *get_config_values(char *yaml_config_filename, ConfigValues *config)
{
	config->loglevel = malloc(FILEPATH_BUFFER_SIZE);
	config->logfile = malloc(FILEPATH_BUFFER_SIZE);
	config->input_foldername = malloc(FILEPATH_BUFFER_SIZE);
	config->input_filename = malloc(FILEPATH_BUFFER_SIZE);
	config->output_foldername = malloc(FILEPATH_BUFFER_SIZE);
	config->output_filename = malloc(FILEPATH_BUFFER_SIZE);

	yaml_parser_t parser;
	yaml_parser_initialize(&parser);

	FILE *config_input = fopen(yaml_config_filename, "r");

	if (config_input == NULL)
	{
		FUNC_GOTO_ERROR("failed to open config.yml");
	}

	yaml_parser_set_input_file(&parser, config_input);

	/* Get values */
	process_layer(&parser, (void *)config, CONFIG_UNKNOWN_T);

	/* Cleanup */
	yaml_parser_delete(&parser);
	fclose(config_input);

	return config;
}

int main(int argc, char **argv)
{

	hid_t fapl_id_in = H5I_INVALID_HID;
	hid_t fapl_id_out = H5I_INVALID_HID;
	hid_t fcpl_id = H5I_INVALID_HID;
	hid_t fin = H5I_INVALID_HID;
	hid_t fout = H5I_INVALID_HID;

	char *input_path = NULL;
	char *output_path = NULL;

	ConfigValues *config = NULL;

	BBox bbox;

	for (size_t optind = 1; optind < argc; optind++)
	{
		if (strcmp(argv[optind], "-debug") == 0)
		{
			debug = true;
		}

		if (strcmp(argv[optind], "-readonly") == 0)
		{
			readonly = true;
		}

		if (strcmp(argv[optind], "-use_ros3") == 0)
		{
			use_ros3 = true;
		}

		if (strcmp(argv[optind], "-use_rest_vol") == 0)
		{
			use_rest_vol = true;
		}
	}

	fcpl_id = H5Pcreate(H5P_FILE_CREATE);

	if ((fapl_id_in = H5Pcreate(H5P_FILE_ACCESS)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to create FAPL")
	}

	if ((fapl_id_out = H5Pcreate(H5P_FILE_ACCESS)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to create FAPL2")
	}

	H5FD_ros3_fapl_t param;

	const char *aws_region = "us-west-2";

	strcpy(param.aws_region, aws_region);

	param.version = 1;
	param.authenticate = 0;

	if (use_ros3)
	{
		if (H5Pset_fapl_ros3(fapl_id_in, &param) < 0)
		{
			FUNC_GOTO_ERROR("Failed to set ros3 in FAPL")
		}
	}
	else if (use_rest_vol)
	{
		H5rest_init();
		H5Pset_fapl_rest_vol(fapl_id_in);
		PRINT_DEBUG("== Using REST VOL == \n")
	}

	config = malloc(sizeof(*config));
	config = get_config_values(CONFIG_FILENAME, config);

	if (!strncmp(config->input_filename, "PAGE10MiB", strlen("PAGE10MiB")))
	{
		size_t page_buf_size = pow(2, config->page_buf_size_exp);

		if (H5Pset_page_buffer_size(fapl_id_in, page_buf_size, 0, 0) < 0)
		{
			FUNC_GOTO_ERROR("Failed to set page buffer size")
		}

		if (!readonly)
		{
			if (H5Pset_page_buffer_size(fapl_id_out, page_buf_size, 0, 0) < 0)
			{
				FUNC_GOTO_ERROR("Failed to set page buffer size")
			}

			if (H5Pset_file_space_strategy(fcpl_id, H5F_FSPACE_STRATEGY_PAGE, 0, 0) < 0)
			{
				FUNC_GOTO_ERROR("Failed to set page strategy for output file")
			}
		}
	}

	input_path = malloc(strlen(config->input_filename) + strlen(config->input_foldername) + 1);
	strcpy(input_path, config->input_foldername);
	strcat(input_path, config->input_filename);

	if ((fin = H5Fopen(input_path, H5F_ACC_RDONLY, fapl_id_in)) == H5I_INVALID_HID)
	{
		FUNC_GOTO_ERROR("Failed to open input file")
	}

	output_path = malloc(strlen(config->output_filename) + strlen(config->output_foldername) + 1);
	strcpy(output_path, config->output_foldername);
	strcat(output_path, config->output_filename);

	if (!readonly)
	{
		if ((fout = H5Fcreate(output_path, H5F_ACC_TRUNC, fcpl_id, fapl_id_out)) == H5I_INVALID_HID)
		{
			FUNC_GOTO_ERROR("Failed to create output file")
		}
	}

	PRINT_DEBUG("Input filepath = %s%s\n", config->input_foldername, config->input_filename)
	if (!readonly)
	{
		PRINT_DEBUG("Output filepath = %s%s\n", config->output_foldername, config->output_filename)
	}

	double min_lon = config->min_lon;

	if (min_lon < -180.0 || min_lon > 180.0)
	{
		PRINT_DEBUG("Invalid min_lon value: %lf\n", min_lon)
		exit(1);
	}

	double max_lon = config->max_lon;

	if (max_lon < -180.0 || max_lon > 180.0 || max_lon <= min_lon)
	{
		PRINT_DEBUG("Invalid max_lon value: %lf\n", max_lon)
		exit(1);
	}

	double min_lat = config->min_lat;

	if (min_lat < -90.0 || min_lat > 90.0)
	{
		PRINT_DEBUG("Invalid min_lat value: %lf\n", min_lat)
		exit(1);
	}

	double max_lat = config->max_lat;

	if (max_lat < -90.0 || max_lat > 90.0 || max_lat <= min_lat)
	{
		PRINT_DEBUG("Invalid max_lat error: %lf\n", max_lat)
		exit(1);
	}

	bbox.min_lon = min_lon;
	bbox.max_lon = max_lon;
	bbox.min_lat = min_lat;
	bbox.max_lat = max_lat;

	PRINT_DEBUG("Lat Range: %lf - %lf\n", bbox.min_lat, bbox.max_lat)
	PRINT_DEBUG("Lon Range: %lf - %lf\n", bbox.min_lon, bbox.max_lon)

	copy_root_attrs(fin, fout);
	copy_scalar_datasets(fin, fout);

	char **current_ground_track = ground_tracks;

	while (*current_ground_track != 0)
	{
		char *h5path = NULL;
		hid_t attr_id = H5I_INVALID_HID;
		hid_t group = H5I_INVALID_HID;

		if (!readonly)
		{
			group = H5Gcreate(fout, *current_ground_track, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
		}

		Range_Indices *index_range = get_index_range(fin, *current_ground_track, &bbox);

		if (index_range == NULL)
		{
			PRINT_DEBUG("No index range found for ground track: %s\n", *current_ground_track)

			hid_t dspace = H5Screate_simple(1, (hsize_t[]){1}, NULL);
			int bad_value = -1;

			if (!readonly)
			{
				if (0 > (attr_id = H5Acreate(group, "index_range_min", H5T_NATIVE_INT, dspace, H5P_DEFAULT, H5P_DEFAULT)))
				{
					FUNC_GOTO_ERROR("Failed to create attribute on no index range")
				}

				if (0 > H5Awrite(attr_id, H5T_NATIVE_INT, &bad_value))
				{
					FUNC_GOTO_ERROR("Failed to write to attribute on no index range")
				}

				if (0 > (attr_id = H5Acreate(group, "index_range_max", H5T_NATIVE_INT, dspace, H5P_DEFAULT, H5P_DEFAULT)))
				{
					FUNC_GOTO_ERROR("Failed to create attribute on no index range")
				}

				if (0 > H5Awrite(attr_id, H5T_NATIVE_INT, &bad_value))
				{
					FUNC_GOTO_ERROR("Failed to write to attribute on no index range")
				}
			}

			/* Go to next iteration */
			current_ground_track++;
			continue;
		}

		PRINT_DEBUG("Got index_range (%zu, %zu)\n", index_range->min, index_range->max)

		hid_t dspace_scalar = H5Screate(H5S_SCALAR);
		if (!readonly)
		{
			if (0 > (attr_id = H5Acreate(group, "index_range_min", H5T_NATIVE_INT, dspace_scalar, H5P_DEFAULT, H5P_DEFAULT)))
			{
				FUNC_GOTO_ERROR("Failed to create attribute on no index range")
			}

			if (0 > H5Awrite(attr_id, H5T_NATIVE_INT, &(index_range->min)))
			{
				FUNC_GOTO_ERROR("Failed to write to attribute on no index range")
			}

			if (0 > (attr_id = H5Acreate(group, "index_range_max", H5T_NATIVE_INT, dspace_scalar, H5P_DEFAULT, H5P_DEFAULT)))
			{
				FUNC_GOTO_ERROR("Failed to create attribute on no index range")
			}

			if (0 > H5Awrite(attr_id, H5T_NATIVE_INT, &(index_range->max)))
			{
				FUNC_GOTO_ERROR("Failed to write to attribute on no index range")
			}
		}
		/* Copy lat, lon, and photo count markers */

		char **current_ref_path = reference_datasets;

		while (*current_ref_path != 0)
		{
			/* Add slash between path names */
			h5path = malloc(strlen(*current_ground_track) + strlen(*current_ref_path) + 2);
			strncpy(h5path, *current_ground_track, strlen(*current_ground_track) + 1);
			h5path[strlen(*current_ground_track)] = '/';
			h5path[strlen(*current_ground_track) + 1] = '\0';

			strcat(h5path, *current_ref_path);
			copy_dataset_range(fin, fout, h5path, index_range);

			free(h5path);
			current_ref_path++;
		}

		/* Sum up photon counts for later indexing */
		char *geoloc = "/geolocation/segment_ph_cnt";
		h5path = malloc(strlen(*current_ground_track) + strlen(geoloc) + 1);
		strncpy(h5path, *current_ground_track, strlen(*current_ground_track) + 1);
		strcat(h5path, geoloc);

		Range_Indices *count_range = get_photon_count_range(fin, h5path, index_range);

		free(h5path);

		PRINT_DEBUG("Photon count range: (%zu, %zu)\n", count_range->min, count_range->max)

		current_ref_path = ph_count_datasets;

		while (*current_ref_path != 0)
		{
			h5path = malloc(strlen(*current_ground_track) + strlen(*current_ref_path) + 2);
			strncpy(h5path, *current_ground_track, strlen(*current_ground_track));
			h5path[strlen(*current_ground_track)] = '/';
			h5path[strlen(*current_ground_track) + 1] = '\0';
			strcat(h5path, *current_ref_path);

			copy_dataset_range(fin, fout, h5path, count_range);

			free(h5path);
			current_ref_path++;
		}

		if (!readonly)
		{
			H5Gclose(group);
		}

		free(count_range);
		free(index_range);

		current_ground_track++;
	}

	/* Close open objects */
	PRINT_DEBUG("Selection test complete\n");

#ifdef USE_REST_VOL
	H5rest_term();
#endif

	H5Pclose(fapl_id_in);
	H5Pclose(fapl_id_out);
	H5Pclose(fcpl_id);
	H5Fclose(fin);

	if (!readonly)
	{
		H5Fclose(fout);
	}

	free(input_path);
	free(output_path);

	free(config->loglevel);
	free(config->logfile);

#ifndef USE_REST_VOL
	free(config->input_foldername);
	free(config->output_foldername);
#endif

	free(config->input_filename);
	free(config->output_filename);
	free(config);

	return 0;
}
