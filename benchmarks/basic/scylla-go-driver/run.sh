#!/bin/bash
docker run --rm -it --network host rust-driver-benchmarks-basic-scylla-go-driver /source/basic "$@"
