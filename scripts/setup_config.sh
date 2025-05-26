#!/bin/bash

NUM_SHARDS=2
if [ ! -z "$1" ]; then
    NUM_SHARDS=$2
fi

NUM_REPLICAS=4
if [ ! -z "$2" ]; then
    NUM_REPLICAS=$4
fi

NUM_SERVERS=$((1 + (NUM_SHARDS * NUM_REPLICAS)))
SCRIPT=$(readlink -f $0)
SCRIPTPATH=$(dirname $SCRIPT)
CURDIR=$(pwd)
cd $SCRIPTPATH/cfg

CONFIG_TMP=derecho.cfg.tmp
WANAGENT_CFG=wanagent.json
DERECHO_NODE_TMP=derecho_node.cfg.tmp
DERECHO_NODE_CFG=derecho_node.cfg
# LAYOUT_TMP=layout.json.tmp
DLL_TMP=udl_dlls.cfg
DFG_TMP=dfgs.json

BACKUP_LAYOUT=backup_layout.json
LAYOUT_CFG=layout.json
DLL_CFG=udl_dlls.cfg
DFG_CFG=dfgs.json
DERECHO_CFG=derecho.cfg
GEN_KEYS=gen_keys.sh

# read base values from derecho config
gms_port=$(grep "^gms_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " ")
state_transfer_port=$(grep "^state_transfer_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " ")
sst_port=$(grep "^sst_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " ")
rdmc_port=$(grep "^rdmc_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " ")
external_port=$(grep "^external_port =" $CONFIG_TMP | cut -d "=" -f2 | tr -d " ")
private_port=$(grep -o '"private_port": [0-9]*' $WANAGENT_CFG | grep -o '[0-9]*')
backup_contact_port=23585

# cfg files

for i in $(seq 0 $((NUM_SERVERS - 1))); do
    mkdir -p n$i
    ln -sf ../$LAYOUT_CFG n$i/$LAYOUT_CFG
    ln -sf ../$DLL_TMP n$i/$DLL_CFG
    ln -sf ../$DFG_TMP n$i/$DFG_CFG

    if ((i < 4)); then
        sed "s@^local_id = .*@local_id = $i@g" $DERECHO_NODE_TMP |
            sed "s@^gms_port = .*@gms_port = $gms_port@g" |
            sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
            sed "s@^sst_port = .*@sst_port = $sst_port@g" |
            sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
            sed "s@^external_port = .*@external_port = $external_port@g" >n$i/$DERECHO_NODE_CFG
        sed "s/\"private_port\": [^,]*,/\"private_port\": $private_port,/" $WANAGENT_CFG >n$i/$WANAGENT_CFG

        # sed 's/"private_port" : .*@private_port = $i@' $WANAGENT_CFG >n$i/$WANAGENT_CFG
        # sed 's/"private_port" : 0,/"private_port" : 1,/' $WANAGENT_CFG >n$i/$WANAGENT_CFG

        # cp $WANAGENT_CFG n$i/$WANAGENT_CFG
        sed "s@^local_id = .*@local_id = $i@g" $CONFIG_TMP |
            sed "s@^gms_port = .*@gms_port = $gms_port@g" |
            sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
            sed "s@^sst_port = .*@sst_port = $sst_port@g" |
            sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
            sed "s@^external_port = .*@external_port = $external_port@g" >n$i/$DERECHO_CFG
        ln -sf ../$LAYOUT_CFG n$i/$LAYOUT_CFG
    else
        sed "s@^local_id = .*@local_id = $i@g" $DERECHO_NODE_TMP |
            sed "s@^gms_port = .*@gms_port = $gms_port@g" |
            sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
            sed "s@^sst_port = .*@sst_port = $sst_port@g" |
            sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
            sed "s@^external_port = .*@external_port = $external_port@g" |
            sed "s@^is_primary_site = true.*@is_primary_site = false@g" >n$i/$DERECHO_NODE_CFG
        sed "s/\"private_port\": [^,]*,/\"private_port\": $private_port,/" $WANAGENT_CFG |
            sed 's/"local_site_id" : 0,/"local_site_id" : 1,/' >n$i/$WANAGENT_CFG
        sed "s@^local_id = .*@local_id = $i@g" $CONFIG_TMP |
            sed "s@^contact_port = .*@contact_port = $backup_contact_port@g" |
            sed "s@^gms_port = .*@gms_port = $gms_port@g" |
            sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
            sed "s@^sst_port = .*@sst_port = $sst_port@g" |
            sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
            sed "s@^external_port = .*@external_port = $external_port@g" >n$i/$DERECHO_CFG
        ln -sf ../$BACKUP_LAYOUT n$i/$LAYOUT_CFG
    fi

    # cascade k/v store
    let gms_port++
    let state_transfer_port++
    let sst_port++
    let rdmc_port++
    let external_port++
    let private_port++
done

ln -sf ../$GEN_KEYS $GEN_KEYS
ln -sf ../$DLL_TMP client/$DLL_CFG
ln -sf ../$DFG_TMP client/$DFG_CFG
mkdir -p client
ln -sf ../../run_benchmark client/run_benchmark
ln -sf ../../generate_workload client/generate_workload
ln -sf ../../metrics.py client/metrics.py

sed "s@^local_id = .*@local_id = $((NUM_SERVERS + 100))@g" $CONFIG_TMP |
    sed "s@^gms_port = .*@gms_port = $gms_port@g" |
    sed "s@^state_transfer_port = .*@state_transfer_port = $state_transfer_port@g" |
    sed "s@^sst_port = .*@sst_port = $sst_port@g" |
    sed "s@^rdmc_port = .*@rdmc_port = $rdmc_port@g" |
    sed "s@^external_port = .*@external_port = $external_port@g" >client/$DERECHO_CFG

# layout
num_nodes=""
delivery=""
reserved=""
profiles=""
for s in $(seq 0 $((NUM_SHARDS - 1))); do
    res=""
    for r in $(seq 1 $((NUM_REPLICAS))); do
        nid=$((s * NUM_REPLICAS + r))

        if ((r == 1)); then
            res="[\"$nid\""
        else
            res="$res,\"$nid\""
        fi

    done
    res="$res]"

    if ((s == 0)); then
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

# sed "s@XXX_MIN_NODES_XXX@$num_nodes@g" $LAYOUT_TMP |
#     sed "s@XXX_MAX_NODES_XXX@$num_nodes@g" |
#     sed "s@XXX_DELIVERY_MODES_XXX@$delivery@g" |
#     sed "s@XXX_RESERVED_IDS_XXX@$reserved@g" |
#     sed "s@XXX_PROFILES_XXX@$profiles@g" >$LAYOUT_CFG

cd $CURDIR
