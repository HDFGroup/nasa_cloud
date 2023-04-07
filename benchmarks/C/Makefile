CC=gcc
CFLAGS=-I$(HDF5_PATH)/include
LIBS=-L$(HDF5_PATH)/lib/ -lhdf5 -lyaml

benchmark: icesat2_selection.c
	$(CC) -o icesat2_selection $(CFLAGS)  icesat2_selection.c $(LIBS)
