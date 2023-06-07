CC=gcc
CFLAGS=-I$(HDF5_PATH)/include -I$(REST_VOL_PATH)/src -g -O0
LIBS=-L$(HDF5_PATH)/lib/ -lm -lhdf5_debug -L$(REST_VOL_PATH)/build/bin -lhdf5_vol_rest -lyaml

benchmark: icesat2_selection.c
	$(CC) -o icesat2_selection $(CFLAGS)  icesat2_selection.c $(LIBS)
