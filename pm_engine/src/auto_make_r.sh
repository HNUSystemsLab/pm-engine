#!/bin/bash
value=$1
alloc=$2
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
if [[ $value == "normal" ]]; then
   cd ../../pm_allocator/benchmark
   make clean
   make alloc=-D$alloc all
   cd ../../nstore-lsm/src
   make clean
   make type=-DALLOCATOR
fi
if [[ $vlaue == "recover" ]]; then
   cd ../../pm_allocator/benchmark
   make clean
   make alloc=-D$alloc recover=-DRECOVER all
   cd ../../nstore-lsm/src
   make clean
   make type=-DALLOCATOR
fi
#export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig

# make alloc=-D
# make alloc=-DMAKALU 