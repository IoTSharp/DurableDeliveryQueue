CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Werror -pedantic
LDFLAGS ?=

LIB = libddq.a
OBJS = ddq.o

.PHONY: all test clean

all: $(LIB) ddq-test

$(LIB): $(OBJS)
	ar rcs $@ $^

ddq.o: ddq.c ddq.h
	$(CC) $(CFLAGS) -c ddq.c -o $@

ddq-test: ddq.c ddq.h test_ddq.c
	$(CC) $(CFLAGS) $(LDFLAGS) ddq.c test_ddq.c -o $@

test: ddq-test
	timeout 20s ./ddq-test

clean:
	rm -f $(LIB) $(OBJS) ddq-test
