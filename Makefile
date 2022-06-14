IDIR =include
CC=mpicc
CFLAGS=-I$(IDIR) -Wall -g
LDFLAGS= -g

BINDIR=bin
SRCDIR=src
ODIR=src/obj
LDIR=lib
LIBS=-lucs -lucp

_DEPS = bk_ss_lib.h
DEPS = $(patsubst %,$(IDIR)/%,$(_DEPS))

_OBJ = bk_ss_lib.o eval_ss.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

$(ODIR)/%.o: $(SRCDIR)/%.c $(DEPS)
	mkdir -p $(ODIR)
	$(CC) -c -o $@ $< $(CFLAGS)

$(BINDIR)/eval_ss: $(OBJ)
	mkdir -p $(BINDIR)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

.PHONY: clean test

clean:
	rm $(ODIR)/*.o $(BINDIR)/*

test: $(BINDIR)/eval_ss
	mpirun -n 4 ./bin/eval_ss
