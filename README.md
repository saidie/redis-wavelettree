# redis-wavelettree

A Redis module implementing the Wavelet Tree data structure.

The Wavelet Tree is a versatile data structure which provides some primitive operations over a sequence with less time and memory complexity and many useful applications has been found on top of it.
See [Wavelet Tree - Wikipedia](https://en.wikipedia.org/wiki/Wavelet_Tree) for more detail and for further reference.

## Installation

To build the module run

```
make
```

Then load the built module `build/libwvltr.so` to Redis server.

## Available commands

### `wvltr.lbuild`

### `wvltr.access`

### `wvltr.rank`

### `wvltr.select`

### `wvltr.quantile`

### `wvltr.rangefreq`

### `wvltr.rangelist`

### `wvltr.prevvalue`

### `wvltr.nextvalue`

### `wvltr.topk`

### `wvltr.rangemink`

### `wvltr.rangemaxk`

