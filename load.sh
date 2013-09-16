#!/bin/zsh

set -e

WAREHOUSES=10
DISTRICTS=10

./tpcc-kv setup consus
./tpcc-kv load-items consus &
for W in `seq $WAREHOUSES`
do
    ./tpcc-kv load-warehouse consus --warehouses $WAREHOUSES --warehouse $W &
done
wait
for W in `seq $WAREHOUSES`
do
    wait
done
for W in `seq $WAREHOUSES`
do
    for D in `seq $DISTRICTS`
    do
        ./tpcc-kv load-district consus --warehouses $WAREHOUSES --warehouse $W --districts $DISTRICTS --district $D &
    done
done
for W in `seq $WAREHOUSES`
do
    for D in `seq $DISTRICTS`
    do
        wait
    done
done
