#!/bin/zsh

set -e

WAREHOUSES=10
DISTRICTS=10
CLIENTS=100

for c in `seq $CLIENTS`
do
    ./tpcc-kv run consus --output bench-${c}.dat --operations 1000 --warehouses $WAREHOUSES --districts $DISTRICTS &
done
for c in `seq $CLIENTS`
do
    wait
done
