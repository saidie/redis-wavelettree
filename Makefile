all: module

module: src/*.c src/*.h
	cd src && gcc -O2 -shared -fPIC *.c -o ../build/libwvltr.so
