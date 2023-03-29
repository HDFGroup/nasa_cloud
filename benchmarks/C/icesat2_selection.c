#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <yaml.h>

#include "hdf5.h"

//#include "rest_vol_public.h"

#define SUCCEED 0
#define FAIL (-1)

#define FILEPATH_BUFFER_SIZE 1024

#define PATH_DELIMITER "/"

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (compatible with v1 and v2 errors)
 */
#define FUNC_GOTO_ERROR(err_msg)                    \
	fprintf(stderr, "%s\n", err_msg);                                              \
	fprintf(stderr, "\n");                                                         \
	exit(1);																	   \

// TODO Maybe replace this with something more lightweight
/* Print if program is run with debug flag */
#define PRINT_DEBUG(...)                                                                   \
                if (debug) { 													       \
					fprintf(stdout, __VA_ARGS__);                                  \
				}                                                                  \

#define CONFIG_FILENAME "../config/config.yml"

bool debug = false;

char *ground_tracks[] = {"gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r", 0};

const char *scalar_datasets[] = {"/orbit_info/sc_orient", 
								 "/ancillary_data/start_rgt", 
								 "/ancillary_data/start_cycle",
								 0}; 

const char *reference_datasets[] = {"geolocation/reference_photon_lat", 
									"geolocation/reference_photon_lon", 
									"geolocation/segment_ph_cnt",
									0};

const char *ph_count_datasets[] = {
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

typedef struct BBox {
	double min_lon;
	double max_lon;
	double min_lat;
	double max_lat;
} BBox;

typedef struct Range_Indices {
	size_t min;
	size_t max;
} Range_Indices;

typedef struct Range_Doubles {
	double min;
	double max;
} Range_Doubles;

typedef struct ConfigValues {
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
} ConfigValues;

typedef enum ConfigType {
	CONFIG_UNKNOWN_T,
	CONFIG_STRING_T,
	CONFIG_DOUBLE_T
} ConfigType;

/* Testing callback for H5Aiterate that prints the name of each attribute of the parent object. */
herr_t test_attr_callback(hid_t location_id, const char* attr_name, const H5A_info_t *ainfo, void *op_data) {
	herr_t ret_value = SUCCEED;

	PRINT_DEBUG("%s\n", attr_name);

	return ret_value;
}

/* Copy each attribute from fin to the file whose hid_t is pointed to by fout_data */
herr_t copy_attr_callback(hid_t fin, const char* attr_name, const H5A_info_t *ainfo, void *fout_data) {
	herr_t ret_value = SUCCEED;

	hid_t fout = *((hid_t*) fout_data);
	hid_t fin_aapl_id;
	hid_t fin_attr;
	
	if (H5I_INVALID_HID == (fin_attr = H5Aopen(fin, attr_name, H5P_DEFAULT))) {
		FUNC_GOTO_ERROR("can't open file attribute in copy callback");
	}

	hid_t dtype_id = H5Aget_type(fin_attr);
	hid_t dstype_id = H5Aget_space(fin_attr);

	H5Acreate(fout, attr_name, dtype_id, dstype_id, H5P_DEFAULT, H5P_DEFAULT);
	H5Aclose(fin_attr);

	return ret_value;
}

/* Copy datasets and their paths from file fin to file fout */
herr_t copy_scalar_datasets(hid_t fin, hid_t fout) {
	hid_t parent_group = H5I_INVALID_HID;
	hid_t child_group = H5I_INVALID_HID;
	hid_t dset = H5I_INVALID_HID;

	char *prev_group_name;
	char *group_name;
	char *dset_name;
	char *dset_path;
	char dset_path_buffer[FILEPATH_BUFFER_SIZE];
	const char **current_dset = scalar_datasets;

	double *data = NULL;

	/* For each dataset in scalar_datasets */
	while (*current_dset != 0) {
		
		/* Copy scalar dataset paths to stack for strtok */
		dset_path = &(dset_path_buffer[0]);
		strncpy(dset_path, *current_dset, strlen(*current_dset) + 1);

		PRINT_DEBUG("Copying scalar dset %s\n", dset_path);

		dset = H5Dopen(fin, dset_path, H5P_DEFAULT);

		/* Separate filepath into groups */
		group_name = strtok_r(dset_path, PATH_DELIMITER, &dset_path);
		
		if (group_name == NULL) {
			group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path);
		}

		parent_group = fout;

		while(group_name != NULL) {
			if (strcmp("", group_name) == 0) {
				group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path);
				continue;
			}

			H5E_BEGIN_TRY {
				child_group = H5Gopen(parent_group, group_name, H5P_DEFAULT);
			} H5E_END_TRY

			if (H5I_INVALID_HID == child_group) {
				child_group = H5Gcreate(parent_group, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
			}

			/* Only close intermediate groups that were just created, not the file itself */
			if (parent_group != fout) {
				H5Gclose(parent_group);
			}
			
			parent_group = child_group;

			/* If the end of the path is reached, keep track of dset name */
			prev_group_name = group_name;
			if (NULL == (group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path))) {
				dset_name = prev_group_name;
			}
			
		}

		hid_t dtype = H5Dget_type(dset);
		hid_t dstype = H5Dget_space(dset);
		hid_t dcpl = H5Dget_create_plist(dset);
		hid_t dapl = H5Dget_access_plist(dset);
		hid_t copied_scalar_dataset = H5Dcreate(parent_group, dset_name, dtype, dstype, H5P_DEFAULT, dcpl, dapl);
		
		
		size_t num_elems = H5Sget_simple_extent_npoints(dstype);
		data = malloc(num_elems * sizeof(double));

		if (H5Dread(dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
			FUNC_GOTO_ERROR("Failed to read dataset while copying scalar")
		}

		if (H5Dwrite(copied_scalar_dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
			FUNC_GOTO_ERROR("Failed to write to dataset while copying scalar")
		}

		free(data);
		H5Dclose(copied_scalar_dataset);
		H5Dclose(dset);
		H5Pclose(dcpl);
		H5Pclose(dapl);
		++current_dset;
	}
}

/* Copy any attributes from input root group to output root group 
 * Return non-negative on success, negative on failure. */
herr_t copy_root_attrs(hid_t fin, hid_t fout) {
	herr_t ret_value = SUCCEED;
	H5Aiterate(fin, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attr_callback, &fout);
	return ret_value;
}

void attr_iteration_test() {
	hid_t file1 = H5Fcreate("/home/test_user1/temp1", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	hid_t file2 = H5Fcreate("/home/test_user1/temp2", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	hid_t null_dstype= H5Screate(H5S_NULL);

	hid_t attr1 = H5Acreate(file1, "attr1", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	hid_t attr2 = H5Acreate(file1, "attr2", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	hid_t attr3 = H5Acreate(file1, "attr3", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);

	copy_root_attrs(file1, file2);
	H5Aiterate(file2, H5_INDEX_NAME, H5_ITER_INC, NULL, test_attr_callback, &file2);

	H5Fclose(file1);
	H5Fclose(file2);
	H5Aclose(attr1);
	H5Aclose(attr2);
	H5Aclose(attr3);
}

/* Get min and max values for the given array within the given range*/
Range_Doubles get_minmax(double arr[], Range_Indices range) {
	Range_Doubles out_range;

	/* Initialize fields */
	out_range.min = arr[range.min];
	out_range.max = arr[range.min];

	for (size_t i = range.min; i < range.max; i++) {
		double elem = arr[i];

		if (elem < out_range.min)
			out_range.min = elem;
		if (elem > out_range.max)
			out_range.max = elem;
	}

	return out_range;
}

bool test_get_minmax() {
	Range_Indices ri;
	ri.min = 2;
	ri.max = 6;

	double arr[] = {0.0, 1.1, 2.2, 3.3, 0.4, 5.5, 0.06, 7.7};

	Range_Doubles rd = get_minmax(arr, ri);

	return true;
}

/* Return the lowest and highest indices of the given array where the lat/lon values fall within the given bounding box */
Range_Indices* get_range(double lat_arr[], size_t lat_size, double lon_arr[], size_t lon_size,
 			   BBox *bbox, Range_Indices *range) 
{
	Range_Indices *ret_range = malloc(sizeof(*range));
	ret_range->min = 0;
	ret_range->max = 0;

	Range_Indices default_range;
	default_range.min = 0;
	default_range.max = lat_size;

	if (lat_size != lon_size) {
		FUNC_GOTO_ERROR("expected lat and lon arrays to have same shape")
	}

	if (range == NULL) {
		range = &default_range;
	}

	PRINT_DEBUG("get_range range has min %zu and max %zu\n", range->min, range->max)

	Range_Doubles lat_range = get_minmax(lat_arr, *range);
	Range_Doubles lon_range = get_minmax(lon_arr, *range);

	/* If entirely outside bbox, return NULL */
	if (lat_range.min > bbox->max_lat ||
		lat_range.max < bbox->min_lat ||
		lon_range.min > bbox->max_lon ||
		lon_range.max < bbox->min_lon) {
			PRINT_DEBUG("%s\n", "Entirely outside bbox")
			free(ret_range);
			ret_range = NULL;
	} else if (lat_range.min >= bbox->min_lat &&
			   lat_range.max <= bbox->max_lat &&
			   lon_range.min >= bbox->min_lon &&
			   lon_range.max <= bbox->max_lon) {
			PRINT_DEBUG("%s\n", "Entirely within bbox")
			memcpy(ret_range, range, sizeof(*range));
	} else {
		/* If entirely in bbox, return current range */
		size_t middle_index = (size_t) ((range->min + range->max) / 2);

		Range_Indices range_select_low;
		range_select_low.min = range->min;
		range_select_low.max = middle_index;
		Range_Indices *range_low = get_range(lat_arr, lat_size, lon_arr, lon_size, bbox, &range_select_low);

		Range_Indices range_select_high;
		range_select_high.min = middle_index;
		range_select_high.max = range->max;
		Range_Indices *range_high = get_range(lat_arr, lat_size, lon_arr, lon_size, bbox, &range_select_high);

		if (range_low == NULL) {
			PRINT_DEBUG("Return range high\n")
			ret_range->min = range_high->min;
			ret_range->max = range_high->max;
			free(range_high);
			return ret_range;
		} else if (range_high == NULL) {
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

bool test_get_range() {
	double lat_arr[] = {0.0, 1.1, 2.2, 3.3};
	double lon_arr[] = {0.0, 1.1, 2.2, 3.3};
	BBox bbox;
	BBox *bbox_ptr = &bbox;
	bbox.max_lat = 2.0;
	bbox.min_lat = 0.0;
	bbox.max_lon = 3.0;
	bbox.min_lon = 1.0;

	Range_Indices *range = NULL;

	size_t lat_size = sizeof(lat_arr) / sizeof(lat_arr[0]);
	size_t lon_size = sizeof(lon_arr) / sizeof(lon_arr[0]);

	Range_Indices *ri = get_range(lat_arr, lat_size, lon_arr, lon_size, bbox_ptr, range);
	free(ri);
}

/* Get min/max index for the given lat/lon bounds */
Range_Indices* get_index_range(hid_t fin, char* ground_track, BBox *bbox) {
	PRINT_DEBUG("get_index_range with ground_track = %s\n", ground_track)

	hid_t lat_dset = H5I_INVALID_HID;
	hid_t lon_dset = H5I_INVALID_HID;

	/* Read data from dsets into arrays */
	char *lat_dset_name = malloc(strlen(ground_track) + strlen(geolocation_lat) + 1);
	strncpy(lat_dset_name, ground_track, strlen(ground_track) + 1);
	strcat(lat_dset_name, geolocation_lat);

	if ((lat_dset = H5Dopen(fin, lat_dset_name, H5P_DEFAULT)) == H5I_INVALID_HID) {
		FUNC_GOTO_ERROR("Failed to open lat datset")
	}

	hid_t dspace_id = H5Dget_space(lat_dset);
	hssize_t num_elems_lat = H5Sget_simple_extent_npoints(dspace_id);
	PRINT_DEBUG("Number of elements in lat dataset is %zu\n", num_elems_lat)
	double *lat_arr = malloc(sizeof(double) * num_elems_lat);

	if (H5Dread(lat_dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, lat_arr) < 0) {
		FUNC_GOTO_ERROR("Failed to read from lat dataset")
	}


	char *lon_dset_name = malloc(strlen(ground_track) + strlen(geolocation_lon) + 1);
	strncpy(lon_dset_name, ground_track, strlen(ground_track) + 1);
	strcat(lon_dset_name, geolocation_lon);

	if ((lon_dset = H5Dopen(fin, lon_dset_name, H5P_DEFAULT)) == H5I_INVALID_HID) {
		FUNC_GOTO_ERROR("Failed to open lon dataset")
	}

	dspace_id = H5Dget_space(lon_dset);
	hssize_t num_elems_lon = H5Sget_simple_extent_npoints(dspace_id);

	double *lon_arr = malloc(sizeof(double) * num_elems_lon);

	if (H5Dread(lon_dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, lon_arr) < 0) {
		FUNC_GOTO_ERROR("Failed to read from lon dataset")
	}

	Range_Indices *index_range = get_range(lat_arr, num_elems_lat, lon_arr, num_elems_lon, bbox, NULL);
	
	if (index_range) {
		PRINT_DEBUG("get_index_range using idex with min %zu and max %zu\n", index_range->min, index_range->max)
	}

	H5Dclose(lon_dset);
	H5Dclose(lat_dset);
	free(lat_arr);
	free(lat_dset_name);
	free(lon_dset_name);
	free(lon_arr);

	return index_range;
}

void test_get_index_range(hid_t fin) {
	/* Entirely within */
	BBox bbox;
	bbox.min_lat = -115;
	bbox.max_lat = 60;
	bbox.min_lon = -115;
	bbox.max_lon = 70;

	Range_Indices *ri = get_index_range(fin, ground_tracks[0], &bbox);
	free(ri);

	/* Entirely outside */
	bbox.min_lat = 0;
	bbox.max_lat = 0;
	bbox.min_lon = 0;
	bbox.max_lon = 0;

	ri = get_index_range(fin, ground_tracks[0], &bbox);

	if (ri) {
		free(ri);
	}

	/* Partially within bbox */
	bbox.min_lat = -55;
	bbox.max_lat = 40;
	bbox.min_lon = -115;
	bbox.max_lon = 70;

	ri = get_index_range(fin, ground_tracks[0], &bbox);

	if (ri) {
		free(ri);
	}
}

/* Copy given index range from source dataset to destination dataset */
void copy_dataset_range(hid_t fin, hid_t fout, char *h5path, Range_Indices *index_range) {
	hid_t source_dset = H5I_INVALID_HID;
	hid_t child_group = H5I_INVALID_HID;
	hid_t parent_group = H5I_INVALID_HID;
	hid_t copy_dset = H5I_INVALID_HID;

	char *prev_group_name;
	char *group_name;
	char *dset_name;
	char *dset_path;
	char dset_path_buffer[FILEPATH_BUFFER_SIZE];

	double* data;

	size_t extent = index_range->max - index_range->min;

	PRINT_DEBUG("Creating dataset %s with extent %lu\n", h5path, extent)

	/* Copy scalar dataset paths to stack for strtok */
	dset_path = &(dset_path_buffer[0]);
	strncpy(dset_path, h5path, strlen(h5path) + 1);

	parent_group = fout;

	/* Separate filepath into groups */
	group_name = strtok_r(dset_path, PATH_DELIMITER, &dset_path);

	while(group_name != NULL) {
			if (strcmp("", group_name) == 0) {
				group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path);
				continue;
			}

			H5E_BEGIN_TRY {
				child_group = H5Gopen(parent_group, group_name, H5P_DEFAULT);
			} H5E_END_TRY

			if (H5I_INVALID_HID == child_group) {
				child_group = H5Gcreate(parent_group, group_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
			}

			// Only close intermediate groups that were just created, not the file itself
			if (parent_group != fout) {
				H5Gclose(parent_group);
			}
			
			parent_group = child_group;

			// If the end of the path is reached, keep track of dset name
			prev_group_name = group_name;
			if (NULL == (group_name = strtok_r(NULL, PATH_DELIMITER, &dset_path))) {
				dset_name = prev_group_name;
			}

			if (group_name) 
				PRINT_DEBUG("New group name = %s\n", group_name)
	}

	/* Copy the data in the source dataset to a new dataset*/

	/* Access data from old dset */
	if ((source_dset = H5Dopen(fin, h5path, H5P_DEFAULT)) == H5I_INVALID_HID) {
		FUNC_GOTO_ERROR("Failed to open source dataset")
	}

	/* Create new dset */
	hid_t dtype = H5Dget_type(source_dset);
	hid_t fspace = H5Screate_simple(1, (hsize_t[]){extent}, NULL);

	if (fspace == H5I_INVALID_HID) {
		FUNC_GOTO_ERROR("Unable to create dstype")
	}

	hid_t dcpl = H5Dget_create_plist(source_dset);
	hid_t dapl = H5Dget_access_plist(source_dset);
	copy_dset = H5Dcreate(parent_group, dset_name, dtype, fspace, H5P_DEFAULT, dcpl, dapl);

	/* Create selection corresponding to range */
	if (0 > H5Sselect_hyperslab(fspace, H5S_SELECT_SET, (hsize_t[]){index_range->min},
														(hsize_t[]){1},
														(hsize_t[]){extent},
														(hsize_t[]){1})) {
		FUNC_GOTO_ERROR("Failed to select hyperslab")
	}

	data = malloc(extent * sizeof(double));

	/* Read and copy selected data */
	if (H5Dread(source_dset, H5T_NATIVE_DOUBLE, H5S_ALL, fspace, H5P_DEFAULT, data) < 0) {
		FUNC_GOTO_ERROR("Failed to read from dset with hyperslab selection")
	}

	if (H5Dwrite(copy_dset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data) < 0) {
		FUNC_GOTO_ERROR("Failed to write data when copying range")
	}

	free(data);
	H5Dclose(copy_dset);
	H5Pclose(dcpl);
	H5Pclose(dapl);
}

void test_copy_dataset_range(hid_t fin, hid_t fout) {
	char *h5path = "gt1r/geolocation/reference_photon_lat";
	Range_Indices ri;
	ri.max = 25000;
	ri.min = 0;

	copy_dataset_range(fin, fout, h5path, &ri);

}

// TODO Move process_layer and get_config_values to another file

/* Process one value from the yaml file. If the value is determined to be a keyname, 
 *	this will recurse to assign the next parsed value as its keyvalue.
 *
 *	If type is CONFIG_UNKNOWN_T, then it expects to be the outermost layer, and for the next value to be a keyname. 
 *	Otherwise, it expects to be a recursion, and for the next value to be a keyvalue of ConfigType type. 
 */
void process_layer(yaml_parser_t *parser, void *storage_location, ConfigType type) {
	ConfigValues *config2;
	ConfigType    new_type;
	yaml_event_t  event;
	yaml_char_t  *value;

	if (type == CONFIG_UNKNOWN_T) 
		config2 = (ConfigValues*) storage_location;

	yaml_parser_parse(parser, &event);
	value = event.data.scalar.value;

	while(event.type != YAML_STREAM_END_EVENT) {

		switch(event.type) {
			case YAML_SCALAR_EVENT:
				switch(type) {
					case CONFIG_STRING_T: {
						char *storage_loc = (char*) storage_location;
						strcpy(storage_loc, value);

						PRINT_DEBUG("Key assigned value %s from original %s\n", storage_loc, value)
						break;
					}
					case CONFIG_DOUBLE_T: {
						double dval = strtod((char *) value, NULL);

						double *storage_loc = (double*) storage_location;
						*storage_loc = dval;

						PRINT_DEBUG("Key assigned value %lf\n", *((double*) storage_location))
						break;
					}
					default: {
						void *next_storage_location = NULL;

						if (!strcmp("loglevel", value)) {
							next_storage_location = config2->loglevel;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("logfile", value)) {
							next_storage_location = config2->logfile;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("input_foldername", value)) {
							next_storage_location = config2->input_foldername;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("input_filename", value)) {
							next_storage_location = config2->input_filename;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("output_foldername", value)) {
							next_storage_location = config2->output_foldername;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("output_filename", value)) {
							next_storage_location = config2->output_filename;
							new_type = CONFIG_STRING_T;
						} else if (!strcmp("min_lat", value)) {
							next_storage_location = (void*) &(config2->min_lat);
							new_type = CONFIG_DOUBLE_T;
						} else if (!strcmp("max_lat", value)) {
							next_storage_location = (void*) &(config2->max_lat);
							new_type = CONFIG_DOUBLE_T;
						} else if (!strcmp("min_lon", value)) {
							next_storage_location = (void*) &(config2->min_lon);
							new_type = CONFIG_DOUBLE_T;
						} else if (!strcmp("max_lon", value)) {
							next_storage_location = (void*) &(config2->max_lon);
							new_type = CONFIG_DOUBLE_T;
						} else {
							PRINT_DEBUG("Key name of %s not found\n", value)
							FUNC_GOTO_ERROR("Invalid yaml option received")
						}

						PRINT_DEBUG("%s%s\n", "Recursing in yaml parsing to assign key ", value)
						process_layer(parser, next_storage_location, new_type);
						break;
					}
				}
						
				break;

				PRINT_DEBUG("%s\n", "Non-scalar event")
				break;
		}

		/* yaml_parser_parse allocates memory for event that must be freed */
		yaml_event_delete(&event);

		/* If this is a recursion, return to next layer up */
		if (type != CONFIG_UNKNOWN_T) {
			break;
		}

		yaml_parser_parse(parser, &event);
		value = event.data.scalar.value;
	}
}

/* Allocate internal memory for config and begin parsing the yaml file. */
ConfigValues* get_config_values(char *yaml_config_filename, ConfigValues *config) {
	config->loglevel = malloc(FILEPATH_BUFFER_SIZE);
	config->logfile = malloc(FILEPATH_BUFFER_SIZE);
	config->input_foldername = malloc(FILEPATH_BUFFER_SIZE);
	config->input_filename = malloc(FILEPATH_BUFFER_SIZE);
	config->output_foldername = malloc(FILEPATH_BUFFER_SIZE);
	config->output_filename = malloc(FILEPATH_BUFFER_SIZE);

	yaml_parser_t parser;
	yaml_parser_initialize(&parser);

	FILE *config_input = fopen(yaml_config_filename, "r");

	if (config_input == NULL) {
		FUNC_GOTO_ERROR("failed to open config.yml");
	}

	yaml_parser_set_input_file(&parser, config_input);

	/* Get values */
	process_layer(&parser, (void*) config, CONFIG_UNKNOWN_T);

	/* Cleanup */
	yaml_parser_delete(&parser);
	fclose(config_input);

	return config;
}

void test_getname(ConfigValues *config) {
	char *filename = "/home/test_user1/file_name_test.h5";
	hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	hid_t fcpl_id = H5Pcreate(H5P_FILE_CREATE);
	H5Fcreate(filename, H5F_ACC_TRUNC, fcpl_id, fapl_id);
	hid_t file = H5Fopen(filename, H5F_ACC_RDWR, fapl_id);
	char name_buf[FILEPATH_BUFFER_SIZE];


	// File
	H5Fget_name(file, name_buf, FILEPATH_BUFFER_SIZE);
	PRINT_DEBUG("Name of given file is %s\n", name_buf)
	memset(name_buf, 0, FILEPATH_BUFFER_SIZE);

	// Group
	hid_t group = H5Gcreate(file, "group_name_test", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	H5Fget_name(group, name_buf, FILEPATH_BUFFER_SIZE);
	PRINT_DEBUG("Name of file containing group is %s\n", name_buf)
	memset(name_buf, 0, FILEPATH_BUFFER_SIZE);

	// Dataset
	hid_t dstype = H5Screate_simple(1, (hsize_t[]){10}, NULL);
	hid_t dset = H5Dcreate(file, "dset_name_test", H5T_NATIVE_INT, dstype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	H5Fget_name(dset, name_buf, FILEPATH_BUFFER_SIZE);
	PRINT_DEBUG("Name of file containing group is %s\n", name_buf)
	memset(name_buf, 0, FILEPATH_BUFFER_SIZE);

	// Attribute
	hid_t null_dstype= H5Screate(H5S_NULL);
	hid_t attr1 = H5Acreate(file, "attr_name_test", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	H5Fget_name(attr1, name_buf, FILEPATH_BUFFER_SIZE);
	PRINT_DEBUG("Name of file containing group is %s\n", name_buf)
	memset(name_buf, 0, FILEPATH_BUFFER_SIZE);

	// Named Datatype
	hid_t named_dtype = H5Tcreate(H5T_STRING, 32);
	H5Tcommit(file, "named_dtype_name_test", named_dtype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	H5Fget_name(named_dtype, name_buf, FILEPATH_BUFFER_SIZE);
	PRINT_DEBUG("Name of file containing group is %s\n", name_buf)
	memset(name_buf, 0, FILEPATH_BUFFER_SIZE);

	H5Tclose(named_dtype);
	H5Pclose(fapl_id);
	H5Pclose(fcpl_id);
	H5Gclose(group);
	H5Fclose(file);
}

int main(int argc, char **argv) 
{

	for (size_t optind = 1; optind < argc; optind++) {
		if (strcmp(argv[optind], "-debug") == 0) {
			debug = true;
		}
	}

	hid_t fapl_id;
	hid_t fcpl_id;

	/* Initialize REST VOL connector */
	//H5rest_init();
	fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	//H5Pset_fapl_rest_vol(fapl_id);
	fcpl_id = H5Pcreate(H5P_FILE_CREATE);


	ConfigValues *config = malloc(sizeof(*config));
	config = get_config_values(CONFIG_FILENAME, config);

	//test_getname(config);

	



	char *input_path = malloc(strlen(config->input_filename) + strlen(config->input_foldername) + 1);
	strcpy(input_path, config->input_foldername);
	strcat(input_path, config->input_filename);
	hid_t atl = H5Fopen(input_path, H5F_ACC_RDWR, fapl_id);

	char *output_path = malloc(strlen(config->output_filename) + strlen(config->output_foldername) + 1);
	strcpy(output_path, config->output_foldername);
	strcat(output_path, config->output_filename);
	hid_t atl_out = H5Fcreate(output_path, H5F_ACC_TRUNC, fcpl_id, fapl_id);

	//attr_iteration_test();
	//bool out = test_get_minmax();
	//test_get_range();
	
	
	//test_get_index_range(atl);

	copy_scalar_datasets(atl, atl_out);

	test_copy_dataset_range(atl, atl_out);

	/* Close open objects */
	H5Pclose(fapl_id);
	H5Pclose(fcpl_id);
	H5Fclose(atl);
	H5Fclose(atl_out);
	
	free(input_path);
	free(output_path);

	free(config->loglevel);
	free(config->logfile);
	free(config->input_foldername);
	free(config->input_filename);
	free(config->output_foldername);
	free(config->output_filename);
	free(config);
	
	/* Terminate the REST VOL connector. */
	//H5rest_term();
	return 0;
}
