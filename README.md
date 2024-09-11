# Cascade CBDC
Implementing a Central Banck Digital Coin on top of Cascade. 

## Requirements
The following is necessary for compiling this project:
- Cascade (https://github.com/Derecho-Project/cascade), branch `single_shard_multiobject_put` (the master branch won't work)
- gzstream and zlib (in Ubuntu: `apt install libgzstream-dev zlib1g-dev`)

## Generating benchmark workload
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

## Running a benchmark
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

## Metrics
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

