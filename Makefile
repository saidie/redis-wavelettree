all: module

module: src/module.c
	cd src && gcc -O2 -shared -fPIC module.c -o ../build/libwvtre.so
