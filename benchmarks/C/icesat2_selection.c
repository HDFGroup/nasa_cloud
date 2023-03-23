#include <stdlib.h>
#include <stdio.h>
#include <string.h>
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
#define FUNC_GOTO_ERROR(err_major, err_minor, ret_val, err_msg)                    \
	fprintf(stderr, "%s\n", err_msg);                                              \
	fprintf(stderr, "\n");                                                         \
	exit(1);																	   \

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

typedef struct Range {
	double min;
	double max;
} Range;

/* Testing callback for H5Aiterate that prints the name of each attribute of the parent object. */
herr_t test_attr_callback(hid_t location_id, const char* attr_name, const H5A_info_t *ainfo, void *op_data) {
	herr_t ret_value = SUCCEED;

	printf("%s\n", attr_name);

	return ret_value;
}

/* Copy each attribute from fin to the file whose hid_t is pointed to by fout_data */
herr_t copy_attr_callback(hid_t fin, const char* attr_name, const H5A_info_t *ainfo, void *fout_data) {
	herr_t ret_value = SUCCEED;

	hid_t fout = *((hid_t*) fout_data);
	hid_t fin_aapl_id;
	hid_t fin_attr;
	
	if (H5I_INVALID_HID == (fin_attr = H5Aopen(fin, attr_name, H5P_DEFAULT))) {
		FUNC_GOTO_ERROR(H5E_ATTR, H5E_CANTOPENOBJ, NULL, "can't open file attribute in copy callback");
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
	char current_dset_path[FILEPATH_BUFFER_SIZE];	
	
	const char **current_dset = scalar_datasets;

	/* For each dataset in scalar_datasets */
	while (*current_dset != 0) {
		
		/* Copy scalar dataset paths to stack for strtok */
		strncpy(current_dset_path, *current_dset, strlen(*current_dset) + 1);

		printf("Copying scalar dset %s\n", current_dset_path);

		dset = H5Dopen(fin, current_dset_path, H5P_DEFAULT);

		/* Separate filepath into groups */
		char *group_name = strtok(current_dset_path, PATH_DELIMITER);
		
		if (group_name == NULL) {
			group_name = strtok(NULL, PATH_DELIMITER);
		}

		parent_group = fout;

		while(group_name != NULL) {
			if (strcmp("", group_name) == 0) {
				group_name = strtok(NULL, PATH_DELIMITER);
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
			if (NULL == (group_name = strtok(NULL, PATH_DELIMITER))) {
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

int main() 
{
	hid_t fapl_id;
	hid_t fcpl_id;


	/* Initialize REST VOL connector */
	//H5rest_init();
	fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	//H5Pset_fapl_rest_vol(fapl_id);
	fcpl_id = H5Pcreate(H5P_FILE_CREATE);

	hid_t atl = H5Fopen("/home/matthewlarson/Documents/nasa_cloud/data/ATL03_20181017222812_02950102_005_01.h5", H5F_ACC_RDWR, fapl_id);
	hid_t atl_out = H5Fcreate("/home/matthewlarson/Documents/nasa_cloud/data/atl_out.h5", H5F_ACC_TRUNC, fcpl_id, fapl_id);
	

	copy_scalar_datasets(atl, atl_out);


	/* Close open objects */
	// TODO: Error handling
	H5Pclose(fapl_id);
	H5Pclose(fcpl_id);
	H5Fclose(atl);
	//H5Fclose(atl_out);
	
	/* Terminate the REST VOL connector. */
	//H5rest_term();
	return 0;
}
