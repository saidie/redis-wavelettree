# redis-wavelettree

A Redis module implementing the Wavelet Tree data structure.

<a title="By Giuseppe Ottaviano (Own work) [CC BY-SA 3.0 (http://creativecommons.org/licenses/by-sa/3.0)], via Wikimedia Commons" href="https://commons.wikimedia.org/wiki/File%3AWavelet_tree.png"><img width="256" alt="Wavelet tree" src="https://upload.wikimedia.org/wikipedia/commons/0/01/Wavelet_tree.png"/></a>

The Wavelet Tree is a versatile data structure which provides some primitive operations over a sequence with less time and memory complexity and many useful applications has been found on top of it.
See [Wavelet Tree - Wikipedia](https://en.wikipedia.org/wiki/Wavelet_Tree) for more detail and for further reference.

## Notice

This module is under development. Module interfaces, internal implementations and complexity could be changed in near future.

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

## License

Please see [LICENSE](https://github.com/saidie/redis-wavelettree/blob/master/LICENSE).
