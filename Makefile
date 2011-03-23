ARCH := $(shell uname -m)
X64 = x86_64
PROGS = magent

LIBS = -levent -lm $(shell pkg-config --libs glib-2.0)

CFLAGS = -Wall -g  `pkg-config --cflags glib-2.0`

all: $(PROGS)

STPROG = magent.o ketama.o log.o 

magent: $(STPROG) 
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)
magent.o: magent.c  log.h config.h 
	$(CC) $(CFLAGS) -c -o $@ magent.c
ketama.o: ketama.c ketama.h
	$(CC) $(CFLAGS) -c -o $@ ketama.c

	
log.o: log.c log.h
	$(CC) $(CFLAGS) -c -o $@ log.c


install:
	mkdir -p /home/memagent
	mkdir -p /home/memagent/bin
	mkdir -p /home/memagent/log
	mkdir -p /home/memagent/conf
	cp -f  magent /home/memagent/bin
	cp  -f ./config/magent.conf /home/memagent/conf/magent.conf

clean: 
	rm -f *.o *~ $(PROGS)
