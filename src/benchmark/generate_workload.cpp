
#include "benchmark_workload.hpp"
#include <iostream>
#include <unistd.h>
#include <string>
#include <stdlib.h>

void print_help(const std::string& bin_name){
    std::cout << "usage: " << bin_name << " [options]" << std::endl;
    std::cout << "options:" << std::endl;
    std::cout << " -w <num_wallets>\t\tnumber of wallets to create" << std::endl;
    std::cout << " -n <wallet_start_id>\t\twallets initial id" << std::endl;
    std::cout << " -t <transfers_per_wallet>\thow many times each wallet appears across all transfers" << std::endl;
    std::cout << " -s <senders_per_transfer>\tnumber of wallets sending coins in each transfer" << std::endl;
    std::cout << " -r <receivers_per_transfer>\tnumber of wallets receiving coins in each transfer" << std::endl;
    std::cout << " -i <wallet_initial_balance>\tinitial balance of each wallet" << std::endl;
    std::cout << " -v <transfer_value>\t\tamount transfered from each sender" << std::endl;
    std::cout << " -g <random_seed>\t\tseed for the RNG" << std::endl;
    std::cout << " -o <output_file>\t\tfile to write the generated workload (zipped)" << std::endl;
    std::cout << " -h\t\t\t\tshow this help" << std::endl;
}

int main(int argc, char** argv){
    wallet_id_t num_wallets = 10;
    wallet_id_t wallet_start_id = 0;
    uint64_t transfers_per_wallet = 1;
    uint64_t senders_per_transfer = 1;
    uint64_t receivers_per_transfer = 1;
    coin_value_t wallet_initial_balance = 100000;
    coin_value_t transfer_value = 10;
    uint64_t random_seed = 3;
    std::string fname;

    // parse arguments
    char c;
    while ((c = getopt(argc, argv, "w:n:t:s:r:i:v:g:o:h")) != -1){
        switch(c){
            case 'w':
                num_wallets = strtoul(optarg,NULL,10);
                break;

            case 'n':
                wallet_start_id = strtoul(optarg,NULL,10);
                break;
            
            case 't':
                transfers_per_wallet = strtoul(optarg,NULL,10);
                break;
            
            case 's':
                senders_per_transfer = strtoul(optarg,NULL,10);
                break;
            
            case 'r':
                receivers_per_transfer = strtoul(optarg,NULL,10);
                break;
            
            case 'i':
                wallet_initial_balance = strtoul(optarg,NULL,10);
                break;
            
            case 'v':
                transfer_value = strtoul(optarg,NULL,10);
                break;
            
            case 'g':
                random_seed = strtoul(optarg,NULL,10);
                break;
            
            case 'o':
                fname = optarg;
                break;

            case '?':
            case 'h':
            default:
                print_help(argv[0]);
                return 0;
        }
    }
    
    if(fname.empty()){
        fname = std::to_string(num_wallets) + "_" + 
            std::to_string(wallet_start_id) + "_" + 
            std::to_string(transfers_per_wallet) + "_" + 
            std::to_string(senders_per_transfer) + "_" + 
            std::to_string(receivers_per_transfer) + "_" + 
            std::to_string(wallet_initial_balance) + "_" + 
            std::to_string(transfer_value) + "_" + 
            std::to_string(random_seed) + 
            ".gz";
    }

    // print summary
    std::cout << "parameters:" << std::endl;
    std::cout << " num_wallets = " << num_wallets << std::endl;
    std::cout << " wallet_start_id = " << wallet_start_id << std::endl;
    std::cout << " transfers_per_wallet = " << transfers_per_wallet << std::endl;
    std::cout << " senders_per_transfer = " << senders_per_transfer << std::endl;
    std::cout << " receivers_per_transfer = " << receivers_per_transfer << std::endl;
    std::cout << " wallet_initial_balance = " << wallet_initial_balance << std::endl;
    std::cout << " transfer_value = " << transfer_value << std::endl;
    std::cout << " random_seed = " << random_seed << std::endl;
    std::cout << " output_file = " << fname << std::endl;

    std::cout << std::endl << "generating ..." << std::endl;

    CBDCBenchmarkWorkload benchmark(num_wallets,wallet_start_id,transfers_per_wallet,senders_per_transfer,receivers_per_transfer,wallet_initial_balance,transfer_value,random_seed);
    benchmark.generate();
    
    std::cout << "writing to '" << fname << "' ..." << std::endl;
    benchmark.to_file(fname);

    std::cout << "done" << std::endl;
    return 0;
}

