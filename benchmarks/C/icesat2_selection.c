#include <stdlib.h>
#include <stdio.h>
#include "hdf5.h"
#include "rest_vol_public.h"

#define SUCCEED 0
#define FAIL (-1)

/*
 * Macro to push the current function to the current error stack
 * and then goto the "done" label, which should appear inside the
 * function. (compatible with v1 and v2 errors)
 */
#define FUNC_GOTO_ERROR(err_major, err_minor, ret_val, err_msg)                    \
	fprintf(stderr, "%s\n", err_msg);                                              \
	fprintf(stderr, "\n");                                                         \
	exit(1);																	   \

const char *ground_tracks[] = {"gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"};

const char *scalar_datasets[] = {"/orbit_info/sc_orient", "/ancillary_data/start_rgt", "/ancillary_data/start_cycle"}; 

const char *reference_datasets[] = {"geolocation/reference_photon_lat", "geolocation/reference_photon_lon", "geolocation/segment_ph_cnt"};

const char *ph_count_datasets[] = {
								"heights/dist_ph_along",
								"heights/h_ph",
								"heights/signal_conf_ph",
								"heights/quality_ph",
								"heights/lat_ph",
								"heights/lon_ph",
								"heights/delta_time"};

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

/* Copy any attributes from input root group to output root group 
 * Return non-negative on success, negative on failure. */
herr_t copy_root_attrs(hid_t fin, hid_t fout) {
	herr_t ret_value = SUCCEED;
	H5Aiterate(fin, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attr_callback, &fout);
	return ret_value;
}

int main() 
{
	hid_t file_id;
	hid_t file_id2;
	hid_t fapl_id;
	hid_t fcpl_id;
	//hid_t dset_id;
	hid_t attr_id;
	hid_t attr_id2;
	hid_t attr_id3;


	/* Initialize REST VOL connector */
	H5rest_init();
	fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_fapl_rest_vol(fapl_id);
	fcpl_id = H5Pcreate(H5P_FILE_CREATE);
	file_id = H5Fcreate("/home/test_user1/my_file.h5", H5F_ACC_TRUNC, fcpl_id, fapl_id);
	file_id2 = H5Fcreate("/home/test_user1/other_file.h5", H5F_ACC_TRUNC, fcpl_id, fapl_id);

	hid_t null_dstype = H5Screate(H5S_NULL); 

	//dset_id = H5Dcreate(file_id, "test_dset", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
	/* Operate on file */
	
	attr_id = H5Acreate(file_id, "test_attribute", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	attr_id2 = H5Acreate(file_id, "test_attribute_2", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	attr_id3 = H5Acreate(file_id, "test_attribute_3", H5T_C_S1, null_dstype, H5P_DEFAULT, H5P_DEFAULT);
	copy_root_attrs(file_id, file_id2);

	// Verify the copy occurred by printing attribute names from 2nd file
	H5Aiterate(file_id2, H5_INDEX_NAME, H5_ITER_INC, NULL, test_attr_callback, &file_id2);

	/* Close open objects */
	// TODO: Error handling
	H5Aclose(attr_id);
	H5Aclose(attr_id2);
	H5Aclose(attr_id3);
	//H5Dclose(dset_id);
	H5Pclose(fapl_id);
	H5Pclose(fcpl_id);
	H5Fclose(file_id);
	H5Fclose(file_id2);
	
	/* Terminate the REST VOL connector. */
	H5rest_term();
	return 0;
}
