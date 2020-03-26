#!/bin/bash

# SPDX-License-Identifier: MIT
# Copyright (c) 2020 Hadrien Chauvin

set -eou pipefail

docker build -t opendata-spark deployments/images/spark

docker-compose -f tests/docker-compose.yml up -d

./tests/cluster-test.sc

docker-compose -f tests/docker-compose.yml down