CC := gcc
SRC1 := dberror.c storage_mgr.c buffer_mgr_stat.c buffer_mgr.c test_assign2_1.c
OBJ1 := dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o

SRC2 := dberror.c storage_mgr.c buffer_mgr_stat.c buffer_mgr.c test_assign2_2.c
OBJ2 := dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_2.o

all:test_assign2_1 test_assign2_2
test_assign2_1: $(OBJ1)
	$(CC) -o test_assign2_1 $(OBJ1)

test_assign2_2: $(OBJ2)
	$(CC) -o test_assign2_2 $(OBJ2)

%.o: %.c
	$(CC) -w -g -c $<

run1: test_assign2_1
	./test_assign2_1 

run2: test_assign2_2
	./test_assign2_2

clean:
	rm -rf test_assign2_1 $(OBJ1) testbuffer1.bin test_assign2_2 $(OBJ2) testbuffer2.bin

