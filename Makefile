all: module

module: src/*.c src/*.h
	cd src && gcc -O2 -shared -fPIC module.c -o ../build/libwvltr.so
