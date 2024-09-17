#!/usr/bin/env python3

import numpy as np
import os
import sys
import argparse

# tags
CBDC_TAG_CLIENT_DEPLOYMENT_INFO = 100005        # deployment info: node/shard association
CBDC_TAG_CLIENT_TRANSFER_START = 100010         # client started preparing the transfer
CBDC_TAG_CLIENT_TRANSFER_QUEUE = 100020         # client pushed the request to the thread
CBDC_TAG_CLIENT_TRANSFER_SENDING = 100050       # client is calling put_and_forget
CBDC_TAG_CLIENT_TRANSFER_SENT = 100080          # put is finished at the client
CBDC_TAG_CLIENT_STATUS = 100100                 # status/version of a TX
CBDC_TAG_CLIENT_BATCHING = 100110               # client request batching

CBDC_TAG_UDL_HANDLER_START = 200010             # UDL main thread received a request
CBDC_TAG_UDL_HANDLER_QUEUING = 200020           # UDL main thread is adding the request to a thread's queue
CBDC_TAG_UDL_HANDLER_END = 200030               # UDL main thread is finished processing the request

CBDC_TAG_UDL_OPERATION_START = 200040           # UDL worker thread is starting processing a request from the queue
CBDC_TAG_UDL_OPERATION_END = 200050             # UDL worker thread finished processing a request from the queue
CBDC_TAG_UDL_ENQUEUE_END = 200060               # TX is enqueued by the worker thread (which includes checking conflicts)
CBDC_TAG_UDL_WALLET_PERSIST_START = 200070      # UDL worker thread is calling put_and_forget for a committed wallet
CBDC_TAG_UDL_WALLET_PERSIST_END = 200080        # put_and_forget is finished for a wallet
CBDC_TAG_UDL_TX_PERSIST_START = 200090          # The first UDL worker thread in the chain is calling put_and_forget for a committed TX
CBDC_TAG_UDL_TX_PERSIST_END = 200100            # put_and_forget is finished for a TX
CBDC_TAG_UDL_NEW_START = 200110                 # UDL worker thread received a new TX (mint,redeem, or transfer)
CBDC_TAG_UDL_RUN_START = 200115                 # UDL worker thread is running a new TX (from main loop)
CBDC_TAG_UDL_COMMIT_START = 200120              # UDL worker thread is committing a TX (from main loop)
CBDC_TAG_UDL_ABORT_START = 200130               # UDL worker thread is aborting a TX (from main loop)
CBDC_TAG_UDL_FORWARD_START = 200140             # UDL worker thread is calling put_and_forget to forward a TX
CBDC_TAG_UDL_FORWARD_END = 200150               # put_and_forget finished to forward a TX
CBDC_TAG_UDL_BACKWARD_START = 200160            # UDL worker thread is calling put_and_forget to backward a TX
CBDC_TAG_UDL_BACKWARD_END = 200170              # put_and_forget finished to backward a TX
CBDC_TAG_UDL_WALLET_BATCHING = 200180           # wallet persistence batching
CBDC_TAG_UDL_CHAIN_BATCHING = 200190            # chaining protocol batching
CBDC_TAG_UDL_TX_BATCHING = 200200               # tx persistence batching

TLT_PERSISTED = 5001                            # time in which a given version was persisted

SKIP = 0.1

def load_logs(file_list):
    data = {}
    tx_version = {}
    tx_shard = {}
    persisted_time = {}
    tx_persisted_time = {}
    tx_list = []
    node_shard = {}
    client_batching = []
    wallet_batching = []
    tx_batching = []
    chain_batching = []
    node_min = {}
    node_max = {}
        
    for fname in file_list:
        with open(fname,"r") as f:
            for line in f:
                if line.startswith('#'): continue

                # tag timestamp, node, txid, extra, extra2
                # example: 100050 1721120759117708544 2 562949953423312 1047 0
                tag,ts,node,txid,extra,extra2 = [int(x) for x in line.split()]

                if txid not in data: data[txid] = {}

                # client timestamps
                if tag in (CBDC_TAG_CLIENT_TRANSFER_START,CBDC_TAG_CLIENT_TRANSFER_SENDING,CBDC_TAG_CLIENT_TRANSFER_SENT):
                    data[txid][tag] = ts
                    if tag == CBDC_TAG_CLIENT_TRANSFER_START:
                        tx_list.append((ts,txid))
                    elif tag == CBDC_TAG_CLIENT_TRANSFER_SENT:
                        if node not in node_min: node_min[node] = sys.maxsize
                        if node not in node_max: node_max[node] = 0
                        node_min[node] = min(node_min[node],ts)
                        node_max[node] = max(node_max[node],ts)
                elif tag == CBDC_TAG_CLIENT_STATUS:
                    tx_version[txid] = extra
                elif tag == CBDC_TAG_CLIENT_DEPLOYMENT_INFO:
                    node_shard[node] = txid

                # UDL main thread timestamps
                if tag in (CBDC_TAG_UDL_HANDLER_START,CBDC_TAG_UDL_HANDLER_QUEUING,CBDC_TAG_UDL_HANDLER_END):
                    if tag not in data[txid]: data[txid][tag] = {}
                    data[txid][tag][extra] = ts

                # UDL worker thread timestamps
                if tag >= CBDC_TAG_UDL_OPERATION_START and (tag <= CBDC_TAG_UDL_BACKWARD_END):
                    if tag in (CBDC_TAG_UDL_TX_PERSIST_START,CBDC_TAG_UDL_TX_PERSIST_END):
                        data[txid][tag] = ts
                        if tag == CBDC_TAG_UDL_TX_PERSIST_START:
                            tx_shard[txid] = extra
                    else:
                        if tag not in data[txid]: data[txid][tag] = {}
                        data[txid][tag][extra] = ts

                # Cascade timestamps
                if tag == TLT_PERSISTED:
                    if node not in persisted_time: persisted_time[node] = {}
                    persisted_time[node][extra] = ts

                # batching
                if tag == CBDC_TAG_CLIENT_BATCHING:
                    client_batching.append(txid)
                elif tag == CBDC_TAG_UDL_WALLET_BATCHING:
                    wallet_batching.append(txid)
                elif tag == CBDC_TAG_UDL_TX_BATCHING:
                    tx_batching.append(txid)
                elif tag == CBDC_TAG_UDL_CHAIN_BATCHING:
                    chain_batching.append(txid)

    first_ts = 0
    last_ts = sys.maxsize
    for node in node_min:
        first_ts = max(first_ts,node_min[node])
        last_ts = min(last_ts,node_max[node])

    # filter out non-transfer TXs and non-overlapping TXs between different clients
    for txid in list(data.keys()):
        if CBDC_TAG_CLIENT_TRANSFER_START not in data[txid]:
            data.pop(txid)
            if txid in tx_version: tx_version.pop(txid)
        elif (data[txid][CBDC_TAG_CLIENT_TRANSFER_SENT] < first_ts) or (data[txid][CBDC_TAG_CLIENT_TRANSFER_SENT] > last_ts): 
            data.pop(txid)
            if txid in tx_version: tx_version.pop(txid)
    
    # remove first SKIP TXs
    tx_list.sort()
    exclude_count = int(SKIP * len(tx_list))
    if exclude_count > 0:
        half = int(exclude_count/2)

        # exclude first txs
        for ts,txid in tx_list[0:half]:
            if txid in data: data.pop(txid)
            if txid in tx_version: tx_version.pop(txid)

        # exclude last txs
        for ts,txid in tx_list[-half:]:
            if txid in data: data.pop(txid)
            if txid in tx_version: tx_version.pop(txid)
 
    # expand persisted versions
    persisted_time_shard = {}
    for node in persisted_time:
        sorted_versions = sorted(persisted_time[node])
        shard = node_shard[node]
        persisted_time_shard[shard] = {}
        persisted_time_shard[shard][sorted_versions[0]] = persisted_time[node][sorted_versions[0]]
        i = 1
        while i < len(sorted_versions):
            ver1 = sorted_versions[i-1] + 1
            ver2 = sorted_versions[i]
            ts = persisted_time[node][ver2]
            persisted_time_shard[shard][ver2] = ts
            
            while ver1 < ver2:
                persisted_time_shard[shard][ver1] = ts
                ver1 += 1

            i += 1

    for txid in tx_version:
        shard = tx_shard[txid]
        ver = tx_version[txid]

        if ver in persisted_time_shard[shard]:
            tx_persisted_time[txid] = persisted_time_shard[shard][ver]
        else:
            data.pop(txid)

    return data,tx_persisted_time,(client_batching,wallet_batching,chain_batching,tx_batching)

def compute_throughput(data):
    timestamps = data[0]
    persisted = data[1]

    last_persisted = max(persisted.values())
    first_client_tx = sys.maxsize
    last_client_tx = 0
    first_thread_tx = sys.maxsize
    last_thread_tx = 0
    e2e = []
    for txid in timestamps:
        first_client_tx = min(first_client_tx,timestamps[txid][CBDC_TAG_CLIENT_TRANSFER_START])
        last_client_tx = max(last_client_tx,timestamps[txid][CBDC_TAG_CLIENT_TRANSFER_START])

        first_thread_tx = min(first_thread_tx,timestamps[txid][CBDC_TAG_CLIENT_TRANSFER_SENDING])
        last_thread_tx = max(last_thread_tx,timestamps[txid][CBDC_TAG_CLIENT_TRANSFER_SENT])
        
        # e2e latency
        lat = persisted[txid] - timestamps[txid][CBDC_TAG_CLIENT_TRANSFER_SENT]
        e2e.append(lat)

    # persisted throughput
    #elapsed = float(last_persisted - first_udl_tx) / 1e+9 # to seconds
    elapsed = float(last_persisted - first_client_tx) / 1e+9 # to seconds
    num_persisted = len(persisted)
    thr = num_persisted / elapsed
    persisted_thr = (thr,num_persisted,elapsed)

    # client sending rate
    elapsed = float(last_client_tx - first_client_tx) / 1e+9 # to seconds
    num_sent = len(timestamps)
    thr = num_sent / elapsed
    sending_rate = (thr,num_sent,elapsed)

    # real sending rate
    elapsed = float(last_thread_tx - first_thread_tx) / 1e+9 # to seconds
    num_sent = len(timestamps)
    thr = num_sent / elapsed
    real_sending_rate = (thr,num_sent,elapsed)

    return sending_rate,real_sending_rate,persisted_thr,e2e

def print_throughput(thr_data):
    sending_rate,real_sending_rate,persisted_thr,e2e = thr_data
    
    thr,count,elapsed = sending_rate
    print(f"client sending rate: {thr:.2f} tx/s ({count} TXs in {elapsed:.2f} seconds)")
    
    thr,count,elapsed = real_sending_rate
    print(f"real sending rate: {thr:.2f} tx/s ({count} TXs in {elapsed:.2f} seconds)")

    thr,count,elapsed = persisted_thr
    print(f"throughput: {thr:.2f} tx/s ({count} TXs in {elapsed:.2f} seconds)")
    
    array = np.array(e2e) / 1e+6 # to milliseconds

    avg = np.mean(array)
    std = np.std(array)
    med = np.median(array)
    min_v = np.min(array)
    max_v = np.max(array)
    p95 = np.percentile(array,95)
    p99 = np.percentile(array,99)

    print(f"e2e latency: avg {avg:6.3f} | std {std:6.3f} | med {med:6.3f} | min {min_v:6.3f} | max {max_v:6.3f} | p95 {p95:6.3f} | p99 {p99:6.3f}")

def compute_batching(data):
    results = []
    for batching in data[2]:
        if len(batching) > 0:
            array = np.array(batching)

            avg = np.mean(array)
            std = np.std(array)
            med = np.median(array)
            min_v = np.min(array)
            max_v = np.max(array)
            p95 = np.percentile(array,95)
            p99 = np.percentile(array,99)

            results.append((avg,std,med,min_v,max_v,p95,p99))
        else:
            results.append((0.0,0.0,0.0,0.0,0.0,0.0,0.0))

    return results

def print_batching(bat_data):
    labels = ['client_batching','wallet_batching','chain_batching','tx_batching']

    print("\nbatching statistics:")
    for label,results in zip(labels,bat_data):
        avg,std,med,min_v,max_v,p95,p99 = results
        print(f"  {label}:".ljust(20),f"avg {avg:6.3f} | std {std:6.3f} | med {med:6.3f} | min {min_v:6.3f} | max {max_v:6.3f} | p95 {p95:6.3f} | p99 {p99:6.3f}")

def compute_breakdown(data):
    timestamps = data[0]
    persisted = data[1]

    e2e = []                # end-to-end: from client sending the tx to persistence
    handler = []            # UDL handler end-to-end
    queue = []              # time in the thread queue
    thread = []             # thread loop e2e
    stabilization = []      # tx put to stabilization
    conflict = []           # checking conflicts
    wallet = []             # wallet put
    txput = []              # tx put
    forward = []            # forward put
    backward = []           # backward put

    for txid in timestamps:
        tx = timestamps[txid]

        # e2e
        lat = persisted[txid] - tx[CBDC_TAG_CLIENT_TRANSFER_SENT]
        e2e.append(lat)

        # handler
        for w in tx[CBDC_TAG_UDL_HANDLER_END]:
            lat = tx[CBDC_TAG_UDL_HANDLER_END][w] - tx[CBDC_TAG_UDL_HANDLER_START][w]
            handler.append(lat)

        # queue
        for w in tx[CBDC_TAG_UDL_HANDLER_END]:
            lat = tx[CBDC_TAG_UDL_OPERATION_START][w] - tx[CBDC_TAG_UDL_HANDLER_END][w]
            queue.append(lat)

        # thread
        for w in tx[CBDC_TAG_UDL_OPERATION_END]:
            lat = tx[CBDC_TAG_UDL_OPERATION_END][w] - tx[CBDC_TAG_UDL_OPERATION_START][w]
            thread.append(lat)

        # stabilization
        lat = persisted[txid] - tx[CBDC_TAG_UDL_TX_PERSIST_END]
        stabilization.append(lat)

        # conflict
        for w in tx[CBDC_TAG_UDL_ENQUEUE_END]:
            lat = tx[CBDC_TAG_UDL_ENQUEUE_END][w] - tx[CBDC_TAG_UDL_NEW_START][w]
            conflict.append(lat)
        
        # wallet
        for w in tx[CBDC_TAG_UDL_WALLET_PERSIST_END]:
            lat = tx[CBDC_TAG_UDL_WALLET_PERSIST_END][w] - tx[CBDC_TAG_UDL_WALLET_PERSIST_START][w]
            wallet.append(lat)

        # tx put
        lat = tx[CBDC_TAG_UDL_TX_PERSIST_END] - tx[CBDC_TAG_UDL_TX_PERSIST_START]
        txput.append(lat)

        # forward
        for w in tx[CBDC_TAG_UDL_FORWARD_END]:
            lat = tx[CBDC_TAG_UDL_FORWARD_END][w] - tx[CBDC_TAG_UDL_FORWARD_START][w]
            forward.append(lat)
        
        # backward
        for w in tx[CBDC_TAG_UDL_BACKWARD_END]:
            lat = tx[CBDC_TAG_UDL_BACKWARD_END][w] - tx[CBDC_TAG_UDL_BACKWARD_START][w]
            backward.append(lat)

    return e2e,handler,queue,thread,stabilization,conflict,wallet,txput,forward,backward

def print_breakdown(lat):
    e2e,handler,queue,thread,stabilization,conflict,wallet,txput,forward,backward = lat

    print("\nlatency breakdown:")
    for label,values in [('e2e',e2e),('handler',handler),('queue',queue),('thread',thread),('stable',stabilization),('conflict',conflict),('wallet',wallet),('txput',txput),('forward',forward),('backward',backward)]:
        array = np.array(values) / 1e+6 # to milliseconds

        avg = np.mean(array)
        std = np.std(array)
        med = np.median(array)
        min_v = np.min(array)
        max_v = np.max(array)
        p95 = np.percentile(array,95)
        p99 = np.percentile(array,99)

        print(f"  {label}:".ljust(12),f"avg {avg:6.3f} | std {std:6.3f} | med {med:6.3f} | min {min_v:6.3f} | max {max_v:6.3f} | p95 {p95:6.3f} | p99 {p99:6.3f}")

def main(argv):
    # command line arguments
    parser = argparse.ArgumentParser(
            prog='metrics',
            description='Compute metrics from Cascade timestamp log files. Always compute throughput, other metrics are optional.')

    parser.add_argument('files',nargs='+',help="Cascade timestamp log files")
    parser.add_argument('-b','--batching',action='store_true',default=False,help="compute batching statistics")
    parser.add_argument('-l','--latency',action='store_true',default=False,help="compute latency breakdown")
    args = parser.parse_args()

    data = load_logs(args.files)
    thr = compute_throughput(data)
    print_throughput(thr)
    
    if args.batching:
        bat = compute_batching(data)
        print_batching(bat)
    
    if args.latency:
        lat = compute_breakdown(data)
        print_breakdown(lat)

if __name__ == "__main__":
    main(sys.argv)

