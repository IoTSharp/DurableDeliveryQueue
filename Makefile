CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Werror -pedantic
LDFLAGS ?=

LIB = libddq.a
OBJS = ddq.o

.PHONY: all test test-reserved clean

all: $(LIB) ddq-test

$(LIB): $(OBJS)
	ar rcs $@ $^

ddq.o: ddq.c ddq.h
	$(CC) $(CFLAGS) -c ddq.c -o $@

ddq-test: ddq.c ddq.h test_ddq.c
	$(CC) $(CFLAGS) $(LDFLAGS) ddq.c test_ddq.c -o $@

ddq-reserve-test: ddq.c ddq.h test_reserved_handles.c
	$(CC) $(CFLAGS) -finput-charset=CP936 -fexec-charset=UTF-8 $(LDFLAGS) ddq.c test_reserved_handles.c -Wl,--wrap=open -Wl,--wrap=fsync -o $@

test-reserved: ddq-reserve-test
	timeout 30s ./ddq-reserve-test "$${DDQ_TEST_ROOT:?Set DDQ_TEST_ROOT to an owned scratch directory}" full

test: ddq-test
	timeout 20s ./ddq-test

clean:
	rm -f $(LIB) $(OBJS) ddq-test ddq-reserve-test
