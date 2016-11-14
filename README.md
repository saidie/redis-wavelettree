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

### `wvltr.lbuild destination key`

- Time complexity: O(N log A)
- Space complexity: O(N log A)

Builds a wavelet tree from the list given by the specified `key` and stores it in `destination`.

### `wvltr.access key index`

- Time complexity: O(log A)

Returns the element at index `index` in the wavelet tree stored at `key`.

### `wvltr.rank key value index`

- Time complexity: O(log A)

Returns the number of occurrences of `value` in elements of the wavelet tree stored at `key` before the index `index`.

### `wvltr.select key value count`

- Time complexity: O(log^2 A)

Return the index of `value` at the `count`-th element of the wavelet tree stored at `key`.

### `wvltr.quantile key from to count`

- Time complexity: O(log A)

Return the `count`-th smallest element in the elements of the wavlet tree stored at `key` within the given index range [`from`, `to`).

### `wvltr.rangefreq`

### `wvltr.rangelist`

### `wvltr.prevvalue`

### `wvltr.nextvalue`

### `wvltr.topk`

### `wvltr.rangemink`

### `wvltr.rangemaxk`

## License

Please see [LICENSE](https://github.com/saidie/redis-wavelettree/blob/master/LICENSE).
