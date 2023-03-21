#include <stdio.h>
#include "hdf5.h"
#include "rest_vol_public.h"

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

int main() 
{
	hid_t file_id;
	hid_t fapl_id;
	hid_t fcpl_id;

	/* Initialize REST VOL connector */
	H5rest_init();
	fapl_id = H5Pcreate(H5P_FILE_ACCESS);
	H5Pset_fapl_rest_vol(fapl_id);
	fcpl_id = H5Pcreate(H5P_FILE_CREATE);
	file_id = H5Fcreate("/home/test_user1/my_file.5", H5F_ACC_RDWR, fcpl_id, fapl_id);

	/* Operate on file */
	H5Pclose(fapl_id);
	H5Fclose(file_id);
	
	/* Terminate the REST VOL connector. */
	H5rest_term();
	return 0;
}
