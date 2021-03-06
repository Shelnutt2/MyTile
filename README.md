# MyTile

[![Build Status](https://travis-ci.org/Shelnutt2/MyTile.svg?branch=master)](https://travis-ci.org/Shelnutt2/MyTile)

MariaDB storage engine based on [TileDB](https://tiledb.io/)

## Requirements

Requires MariaDB 10.2 or newer. It is untested on older versions.

## Installation

### Inside MariaDB Source Tree (Recommended)
The first is inside a MariaDB source tree (recommended).

```bash
git clone git@github.com:MariaDB/server.git -b 10.2
cd server
git submodule add https://github.com/Shelnutt2/MyTile.git storage/mytile
mkdir build && cd build
cmake ..
make -j4
```

## Features

- Based on TileDB's Key-Value store
- No write locking due to TileDB's eventual consistency model

## Known Issues

- Geometry is not supported [#6](https://github.com/Shelnutt2/MyTile/issues/6)

## Planned Features
- Transaction support [#8](https://github.com/Shelnutt2/MyTile/issues/8)
- Secondary Index Support [#9](https://github.com/Shelnutt2/MyTile/issues/9)
- Spatial Indexing [#7](https://github.com/Shelnutt2/MyTile/issues/7)
- Consolidation/optimize table for compaction of kv store [#10](https://github.com/Shelnutt2/MyTile/issues/10)
- Add support for specify tiledb parameter as table options [#11](https://github.com/Shelnutt2/MyTile/issues/11)

