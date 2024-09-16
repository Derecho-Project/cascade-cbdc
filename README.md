# CascadeCBDC
CascadeCBDC is a Central Bank Digital Coin (CBDC) built on top of Cascade. It
makes use of the fast K/V store and the User Defined Logic (UDL) framework
provided by Cascade. Replication and sharding features are inherited from
Cascade (and the Derecho library underneath), while CascadeCBDC implements a
protocol based on chain replication to ensure the consistency of multi-shard
transactions. Furthermore, CascadeCBDC leverages CascadeChain for WAN
replication and auditability. CascadeChain creates a tamper-proof auditable log,
structured as cryptographically linked batches of transactions, which is
replicated across geographically distributed sites. Innovations include our
security model, which matches well with CBDC real-world requirements, and a
series of architectural design choices that move most operations off the
critical path in order to ensure high throughput and low latency.

Table of contents:

- [Requirements](#requirements)
- [Overview](#overview)
- [Setup](#setup)
- [Benchmark tools](#benchmark-tools)
- [Configuration options](#configuration-options)


## Requirements

The easiest way to try out this project is to use our docker image with all the requirements pre-installed:
```sh
$ sudo docker run -d --hostname cascade-cbdc --name cascade-cbdc -it tgarr/cascade-cbdc:latest
$ sudo docker exec -w /root -it cascade-cbdc /bin/bash -l
```

We assume the use of the docker image in the examples and instructions provided in this document. In case you do not wish to use it, the following is required to compile and run CascadeCBDC:

- Cascade (https://github.com/Derecho-Project/cascade), branch `single_shard_multiobject_put` (the master branch won't work)
- gzstream and zlib (in Ubuntu: `sudo apt install libgzstream-dev zlib1g-dev`)

## Overview

## Setup
This section will give instructions on how to compile and run CascadeCBDC in a basic deployment, with two Cascader servers and one client, all in the same host (in this case, a docker container running our image). In order to increase the number of servers/clients, or to run processes in multiples network nodes, see [Configuration options](#configuration-options).

### Compilation
```sh
root@cascade-cbdc:~# git clone https://github.com/Derecho-Project/cascade-cbdc.git
root@cascade-cbdc:~# mkdir cascade-cbdc/build && cd cascade-cbdc/build
root@cascade-cbdc:~/cascade-cbdc/build# cmake .. && make -j
```
This will create the following in the `build` directory (we omit below other files that are not relevant):

- `cfg`: folder containing the configuration files necessary to run Cascade server processes and a CascadeCBDC client
    - `dfgs.json`: UDL configuration, containing all UDLs to be loaded by Cascade, how each one is triggered, and custom configuration options for each UDL. In CascadeCBDC, there is only one UDL that implements the CascadeCBDC service, with several options for performance tuning. This file must be the same for all server processes.
    - `layout.json`: this configures the layout of the deployment, i.e. how many shards and processes per shard, as well as the exact shard membership. This file must be the same for all server and client processes.
    - `udl_dlls.cfg`: this lists the shared libraries (containing UDLs) that should be loaded by the Cascade servers. This file must be the same for all server processes.
    - `n0`: folder containing configuration files to run a Cascader server with ID 0
        - `derecho.cfg`: Cascade and Derecho configuration file, setting the process ID and network configuration (addresses,ports,protocols).
        - `dfgs.json`,`layout.json`,`udl_dlls.cfg`: links to the corresponding files in the parent folder.
    - `n1`: folder containing configuration files to run Cascader server with ID 1
        - `derecho.cfg`: Cascade and Derecho configuration file, setting the process ID and network configuration (addresses,ports,protocols).
        - `dfgs.json`,`layout.json`,`udl_dlls.cfg`: links to the corresponding files in the parent folder.
    - `client`: folder containing configuration files to run a CascadeCBDC client
        - `derecho.cfg`: Cascade and Derecho configuration file, setting the process ID and network configuration (addresses,ports,protocols).
        - `dfgs.json`,`layout.json`,`udl_dlls.cfg`: links to the corresponding files in the parent folder.
        - `generate_workload`,`run_benchmark`,`metrics.py`: links to the executable in the `build` folder, for convenience.
- `libcbdc_udl.so`: this shared library is loaded by all Cascade servers. It contains the UDL that implements the CascadeCBDC service.
- `generate_workload`: this executable generates a workload used as input to `run_benchmark` (more details in [Benchmark tools](#benchmark-tools))
- `run_benchmark`: this executable runs a benchmark using a given workload file (more details in [Benchmark tools](#benchmark-tools))
- `metrics.py`: this script takes benchmark log outputs (from client and servers) and computes some simple metrics, such as throughput and latency breakdown (more details in [Benchmark tools](#benchmark-tools)).
- `setup_config.sh`: this script generates, in the `cfg` folder, the necessary configuration files for a given number of shards and processes per shard. More details in [Configuration options](#configuration-options).

### Starting the service
To start the service, it's necessary to run two instances of `cascade_server`: one in the `cfg/n0` folder and another in the `cfg/n1` folder. The first process is the Cascade metadata service, and the second constitues the single shard (with a single process) of the CascadeCBDC service. Example:
<table>
<tr>
<th>@~/cascade-cbdc/build/cfg/n0</th>
<th>@~/cascade-cbdc/build/cfg/n1</th>
</tr>
<tr>
<td>

```sh
$ cascade_server 
Press Enter to Shutdown.

```

</td>
<td>

```sh
$ cascade_server 
Press Enter to Shutdown.

```

</td>
</tr>
</table>

Cascade provides a simple command-line tool for interacting with the K/V store. Let's try it out by running `cascade_client.py` in the `cfg/client` folder. This client provides a shell-like interface. The `help` command lists all available options. Example:
<table>
<tr>
<th>@~/cascade-cbdc/build/cfg/client</th>
</tr>
<tr>
<td>

```
$ cascade_client.py
(cascade.client) list_members
Nodes in Cascade service:[0, 1]
(cascade.client) create_object_pool /test PersistentCascadeStoreWithStringKey 0
[3, 1726478940683116]
(cascade.client) put /test/example hello_world
[1, 1726478948893719]
(cascade.client) list_keys_in_object_pool /test
[
 ['/test/example']
]
(cascade.client) get /test/example
{'key': '/test/example', 'value': b'hello_world', 'version': 1, 'timestamp': 1726478948893719,
'previous_version': 0, 'previous_version_by_key': 0, 'message_id': 0}
(cascade.client) quit
Quitting Cascade Client Shell
```

</td>
</tr>
</table>

Your are ready now to run CascadeCBDC! See below how to use the benchmark tools.

## Benchmark tools


### Generating benchmark workload
The `generate_workload` executable generates a workload that can be used to run a benchmark. The workload is saved in a zipped file.
```
$ ./generate_workload -h
usage: ./generate_workload [options]
options:
 -w <num_wallets>               number of wallets to create
 -n <wallet_start_id>           wallets initial id
 -t <transfers_per_wallet>      how many times each wallet appears across all transfers
 -s <senders_per_transfer>      number of wallets sending coins in each transfer
 -r <receivers_per_transfer>    number of wallets receiving coins in each transfer
 -i <wallet_initial_balance>	initial balance of each wallet
 -v <transfer_value>            amount transfered from each sender
 -g <random_seed>               seed for the RNG
 -o <output_file>               file to write the generated workload (zipped)
 -h                             show this help
```

The default name of the output file is a concatenation of all parameters, with the extension `.gz`. The generated file can be seen uzing `zcat`. For example:
```
$ ./generate_workload -w 4
parameters:
 num_wallets = 4
 wallet_start_id = 0
 transfers_per_wallet = 1
 senders_per_transfer = 1
 receivers_per_transfer = 1
 wallet_initial_balance = 100000
 transfer_value = 10
 random_seed = 3
 output_file = 4_0_1_1_1_100000_10_3.gz

generating ...
writing to '4_0_1_1_1_100000_10_3.gz' ...
done

$ zcat 4_0_1_1_1_100000_10_3.gz
4 0 1 1 1 100000 10 3 2 4 2
0 100000
1 100000
2 100000
3 100000
3 10 1 10 
0 10 2 10 
0 99990
1 100010
2 100010
3 99990
0 1
1 1
```

The generated file can be loaded using the static method `CBDCBenchmarkWorkload::from_file(filename)`. The workload provides three structures that can be used to execute a benchmark:
- Wallets/coins to be minted: obtained by calling `get_wallets()`
    - This is a map from wallet\_id\_t to a wallet\_t, which currently is just a value (the wallet balance, coin\_value\_t). 
- Transfers: obtained by calling `get_transfers()`
    - This is a vector of transfer structures. Each transfer contains a vector of senders and a vector of receivers. Each sender/receiver is a map (wallet\_id\_t to coin\_value\_t).
- Expected balances: obtained by calling `get_expected_balance()`
    - This is a map between a pair wallet\_id\_t and coin\_value\_t, corresponding to the expected balance of each wallet. This should be used to verify the correctness of the benchmark.
- Expected status: obtained by calling `get_expected_status()`
    - This is a map between the transfer index (following the order returned by `get_transfers()`) and whether the corresponding transfer should be successful or not. This is relevant in case of conflicting transfers.

### Running a benchmark
The `run_benchmark` executable runs a benchmark using a given workload file (pre-generated with `generate_workload`). It uses the CascadeCBDC class, which starts a Cascade external client, thus there must be a `derecho.cfg` configuring it.
```
$ ./run_benchmark -h
usage: ./run_benchmark [options] <benchmark_workload_file>
options:
 -o <output_file>   file to write the local measurements log
 -l <remote_log>    file to write the remote measurements logs
 -r <send_rate>     rate (in transfers/second) at which to send transfers (default: unlimited)
 -w <wait_time>     time to wait (in seconds) after each step (default: 2)
 -m                 skip minting step
 -s                 skip transfer step
 -c                 skip check step
 -h                 show this help
```

Example of execution:
```
$ ./run_benchmark 2000_0_1_1_1_100000_10_3.gz -r 5000
setting up ...
minting wallets ...
waiting last mint to finish ...
performing 1000 transfers ...
waiting last TX to finish ...
checking 2000 final balances ...
  0 balance errors found
checking 1000 final status ...
  0 status errors found
writing log to '2000_0_1_1_1_100000_10_3.gz.log' ...
done
```

Please see `run_benchmark.cpp` to understand how to read from `CBDCBenchmarkWorkload` and call the mint and transfer operations in `CascadeCBDC`.

### Metrics
The script `metrics.py` takes benchmark log outputs (from client and servers) and computes some simple metrics, such as throughput and latency breakdown. The log output from servers must be downloaded (they are saved by each server in a file named according to the `-l` option of `run_benchmark` (default cbdc.log). 
```
$ ./metrics.py -h
usage: metrics [-h] [-t] [-l] files [files ...]

Compute metrics from Cascade timestamp log files

positional arguments:
  files             Cascade timestamp log files

options:
  -h, --help        show this help message and exit
  -t, --throughput  compute throughput
  -l, --latency     compute latency breakdown
```

Example:
```
$ ./metrics.py -t -l 2000_0_1_1_1_100000_10_3.gz.log ../n1/cbdc.log 
sending rate: 4995.57 tx/s (900 TXs in 0.18 seconds)
throughput: 4908.09 tx/s (900 TXs in 0.18 seconds)

latency breakdown:
  e2e:       avg  4.211 | std  0.844 | med  4.148 | min  2.596 | max  7.330 | p95  5.821 | p99  6.747
  handler:   avg  0.007 | std  0.012 | med  0.007 | min  0.003 | max  0.349 | p95  0.010 | p99  0.017
  queue:     avg  0.205 | std  0.071 | med  0.191 | min  0.093 | max  0.744 | p95  0.323 | p99  0.437
  thread:    avg  0.098 | std  0.041 | med  0.090 | min  0.035 | max  0.465 | p95  0.170 | p99  0.255
  stable:    avg  3.612 | std  0.837 | med  3.544 | min  2.135 | max  6.799 | p95  5.156 | p99  6.152
  conflict:  avg  0.008 | std  0.003 | med  0.008 | min  0.002 | max  0.030 | p95  0.011 | p99  0.014
  wallet:    avg  0.040 | std  0.034 | med  0.029 | min  0.010 | max  0.416 | p95  0.102 | p99  0.184
  txput:     avg  0.032 | std  0.026 | med  0.022 | min  0.011 | max  0.297 | p95  0.079 | p99  0.138
  forward:   avg  0.004 | std  0.001 | med  0.004 | min  0.002 | max  0.020 | p95  0.006 | p99  0.008
  backward:  avg  0.004 | std  0.002 | med  0.004 | min  0.002 | max  0.022 | p95  0.006 | p99  0.013
```

## Configuration options

### Cascade configuration

#### Increasing servers and clients
To have more clients, shards, and/or processes per shard

#### Network configuration
In case you want to deploy Cascade servers and clients on separate nodes in a network.

In case you want to use RDMA instead of TCP.

### CascadeCBDC client configuration

### CascadeCBDC service configuration
