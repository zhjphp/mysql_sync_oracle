LIBDIR=/usr/lib/mysql/plugin/

install:
	gcc -Wall -I/usr/include/mysql -I. -shared lib_mysqludf_sys.c -o $(LIBDIR)/lib_mysqludf_sys.so
