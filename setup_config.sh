#!/bin/bash

NUM_SHARDS=1
if [ ! -z "$1" ]; then
    NUM_SHARDS=$1
fi

NUM_REPLICAS=1
if [ ! -z "$2" ]; then
    NUM_REPLICAS=$2
fi

NUM_SERVERS=$((1+(NUM_SHARDS*NUM_REPLICAS)))
SCRIPT=`readlink -f $0`
SCRIPTPATH=`dirname $SCRIPT`
CURDIR=`pwd`
cd $SCRIPTPATH/cfg

CONFIG_TMP=derecho.cfg
LAYOUT_TMP=layout.json.tmp
DLL_TMP=udl_dlls.cfg.tmp
DFG_TMP=dfgs.json.tmp

LAYOUT_CFG=layout.json
DLL_CFG=udl_dlls.cfg
DFG_CFG=dfgs.json
DERECHO_CFG=derecho.cfg

# read base values from derecho config
gms_port=`grep "^gms_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " "`
state_transfer_port=`grep "^state_transfer_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " "`
sst_port=`grep "^sst_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " "`
rdmc_port=`grep "^rdmc_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " "`
external_port=`grep "^external_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " "`

# cfg files
for i in `seq 0 $((NUM_SERVERS-1))`; do
    mkdir -p n$i
    ln -sf ../$LAYOUT_CFG n$i/$LAYOUT_CFG
    ln -sf ../$DLL_TMP n$i/$DLL_CFG
    ln -sf ../$DFG_TMP n$i/$DFG_CFG

    sed "s@^local_id = .*@local_id = $i@g" $CONFIG_TMP |
        sed "s@^gms_port = .*@gms_port = $gms_port@g" |
        sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
        sed "s@^sst_port = .*@sst_port = $sst_port@g" |
        sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
        sed "s@^external_port = .*@external_port = $external_port@g" > n$i/$DERECHO_CFG

    # cascade k/v store
    let gms_port++
    let state_transfer_port++
    let sst_port++
    let rdmc_port++
    let external_port++
done

mkdir -p client
ln -sf ../$LAYOUT_CFG client/$LAYOUT_CFG
ln -sf ../$DLL_TMP client/$DLL_CFG
ln -sf ../$DFG_TMP client/$DFG_CFG
ln -sf ../../run_benchmark client/run_benchmark
ln -sf ../../generate_workload client/generate_workload
ln -sf ../../metrics.py client/metrics.py

sed "s@^local_id = .*@local_id = $((NUM_SERVERS+100))@g" $CONFIG_TMP |
    sed "s@^gms_port = .*@gms_port = $gms_port@g" |
    sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
    sed "s@^sst_port = .*@sst_port = $sst_port@g" |
    sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
    sed "s@^external_port = .*@external_port = $external_port@g" > client/$DERECHO_CFG

# layout
num_nodes=""
delivery=""
reserved=""
profiles=""
for s in `seq 0 $((NUM_SHARDS-1))`; do
    res=""
    for r in `seq 1 $((NUM_REPLICAS))`; do
        nid=$((s*NUM_REPLICAS+r))

        if (( r == 1 )); then
            res="[\"$nid\""
        else
            res="$res,\"$nid\""
        fi

    done
    res="$res]"

    if (( s == 0 )); then
        num_nodes="\"$NUM_REPLICAS\""
        delivery="\"Ordered\""
        reserved="$res"
        profiles="\"DEFAULT\""
    else
        num_nodes="$num_nodes,\"$NUM_REPLICAS\""
        delivery="$delivery,\"Ordered\""
        reserved="$reserved,$res"
        profiles="$profiles,\"DEFAULT\""
    fi
done

sed "s@XXX_MIN_NODES_XXX@$num_nodes@g" $LAYOUT_TMP | 
    sed "s@XXX_MAX_NODES_XXX@$num_nodes@g" |
    sed "s@XXX_DELIVERY_MODES_XXX@$delivery@g" |
    sed "s@XXX_RESERVED_IDS_XXX@$reserved@g" |
    sed "s@XXX_PROFILES_XXX@$profiles@g" > $LAYOUT_CFG

cd $CURDIR

