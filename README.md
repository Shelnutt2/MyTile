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

- Nulls are not supported yet
- Geometry is not supported

