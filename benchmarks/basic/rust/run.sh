#!/bin/bash
source ../config.sh
docker run --rm --memory=$mem --memory-swap=$memSwap --cpus=$cpus -it --network host benchmark-rust /source/basic "$@"
