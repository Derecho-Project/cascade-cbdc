#pragma once

#include "common.hpp"
#include <string>
#include <vector>
#include <tuple>
#include <map>
#include <unordered_map>
#include <utility>
#include <algorithm>
#include <random>
#include <stdexcept>
#include <fstream>
#include <iostream>
#include <list>
#include <iterator>
#include <gzstream.h>

using benchmark_transfer_t = struct benchmark_transfer_t {
    std::unordered_map<wallet_id_t,coin_value_t> senders;
    std::unordered_map<wallet_id_t,coin_value_t> receivers;
};

class CBDCBenchmarkWorkload {
    // benchmark parameters
    wallet_id_t num_wallets = 10;
    wallet_id_t wallet_start_id = 0;
    uint64_t transfers_per_wallet = 1;
    uint64_t senders_per_transfer = 1;
    uint64_t receivers_per_transfer = 1;
    coin_value_t wallet_initial_balance = 100000;
    coin_value_t transfer_value = 10;
    uint64_t random_seed = 3;

    // internal
    bool generated = false;
    std::unordered_map<wallet_id_t,coin_value_t> wallets;
    std::vector<benchmark_transfer_t> transfers;
    std::unordered_map<wallet_id_t,coin_value_t> expected_balance;
    std::unordered_map<uint64_t,bool> expected_status;

    public:
        CBDCBenchmarkWorkload(){}
        CBDCBenchmarkWorkload(
                wallet_id_t num_wallets,
                wallet_id_t wallet_start_id,
                uint64_t transfers_per_wallet,
                uint64_t senders_per_transfer,
                uint64_t receivers_per_transfer,
                coin_value_t wallet_initial_balance,
                coin_value_t transfer_value,
                uint64_t random_seed):
            num_wallets(num_wallets),
            wallet_start_id(wallet_start_id),
            transfers_per_wallet(transfers_per_wallet),
            senders_per_transfer(senders_per_transfer),
            receivers_per_transfer(receivers_per_transfer),
            wallet_initial_balance(wallet_initial_balance),
            transfer_value(transfer_value),
            random_seed(random_seed){}

        void generate();
        void to_file(const std::string fname);
        static CBDCBenchmarkWorkload& from_file(const std::string fname);

        std::tuple<wallet_id_t,wallet_id_t,uint64_t,uint64_t,uint64_t,coin_value_t,coin_value_t,uint64_t> get_parameters(){ return std::make_tuple(num_wallets,wallet_start_id,transfers_per_wallet,senders_per_transfer,receivers_per_transfer,wallet_initial_balance,transfer_value,random_seed); }
        const std::unordered_map<wallet_id_t,coin_value_t>& get_wallets(){ return wallets; }
        const std::vector<benchmark_transfer_t>& get_transfers(){ return transfers; }
        const std::unordered_map<wallet_id_t,coin_value_t>& get_expected_balance(){ return expected_balance; }
        const std::unordered_map<uint64_t,bool>& get_expected_status(){ return expected_status; }
};

