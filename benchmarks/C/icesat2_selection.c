#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <yaml.h>

#include "hdf5.h"

#include "rest_vol_public.h"

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

/* Print if program is run with debug flag */
#define PRINT_DEBUG(...)                                                                   \
                if (debug) { 													       \
					fprintf(stdout, __VA_ARGS__);                                  \
				}                                                                  \

#define CONFIG_FILENAME "../config/config.yml"

bool debug = false;

const char *ground_tracks[] = {"gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r", 0};

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
	char *current_dset_path;
	char dset_buffer[FILEPATH_BUFFER_SIZE];
	
	const char **current_dset = scalar_datasets;

	/* For each dataset in scalar_datasets */
	while (*current_dset != 0) {
		
		/* Copy scalar dataset paths to stack for strtok */
		current_dset_path = &(dset_buffer[0]);
		strncpy(current_dset_path, *current_dset, strlen(*current_dset) + 1);

		PRINT_DEBUG("Copying scalar dset %s\n", current_dset_path);

		dset = H5Dopen(fin, current_dset_path, H5P_DEFAULT);

		/* Separate filepath into groups */
		char *group_name = strtok_r(current_dset_path, PATH_DELIMITER, &current_dset_path);
		
		if (group_name == NULL) {
			group_name = strtok_r(NULL, PATH_DELIMITER, &current_dset_path);
		}

		parent_group = fout;

		while(group_name != NULL) {
			if (strcmp("", group_name) == 0) {
				group_name = strtok_r(NULL, PATH_DELIMITER, &current_dset_path);
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
			if (NULL == (group_name = strtok_r(NULL, PATH_DELIMITER, &current_dset_path))) {
				dset_name = prev_group_name;
			}
			
		}

		hid_t dtype = H5Dget_type(dset);
		hid_t dstype = H5Dget_space(dset);
		hid_t copied_scalar_dataset = H5Dcreate(parent_group, dset_name, dtype, dstype, 
																						H5P_DEFAULT, \
																						H5Dget_create_plist(dset), \
																						H5Dget_access_plist(dset));
		H5Dclose(copied_scalar_dataset);
		H5Dclose(dset);
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

	for (size_t i = range.min; i <= range.max; i++) {
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

int main(int argc, char **argv) 
{

	for (size_t optind = 1; optind < argc; optind++) {
		if (strcmp(argv[optind], "-debug") == 0) {
			debug = true;
		}
	}

	ConfigValues *config = malloc(sizeof(*config));
	config = get_config_values(CONFIG_FILENAME, config);

	hid_t fapl_id;
	hid_t fcpl_id;

	/* Initialize REST VOL connector */
	//H5rest_init();
	fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	//H5Pset_fapl_rest_vol(fapl_id);
	fcpl_id = H5Pcreate(H5P_FILE_CREATE);

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
	copy_scalar_datasets(atl, atl_out);

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
