
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <vector>
#include <unistd.h>
#include <stdlib.h>
#include <unordered_set>
#include "cbdc_client.hpp"
#include "benchmark_workload.hpp"

#define LAST_TX_POLL_INTERVAL_MS 1000
#define DEFAULT_SECONDS_AFTER_STEP 5
#define DEFAULT_BATCH_MIN_SIZE 0
#define DEFAULT_BATCH_MAX_SIZE 150
#define DEFAULT_BATCH_TIME_US 500

void print_help(const std::string& bin_name){
    std::cout << "usage: " << bin_name << " [options] <benchmark_workload_file>" << std::endl;
    std::cout << "options:" << std::endl;
    std::cout << " -o <output_file>\tfile to write the local measurements log" << std::endl;
    std::cout << " -l <remote_log>\tfile to write the remote measurements logs" << std::endl;
    std::cout << " -r <send_rate>\trate (in transfers/second) at which to send transfers (default: unlimited)" << std::endl;
    std::cout << " -w <wait_time>\ttime to wait (in seconds) after each step (default: " << DEFAULT_SECONDS_AFTER_STEP << ")" << std::endl;
    std::cout << " -b <batch_min_size>\tminimum batch size (default: " << DEFAULT_BATCH_MIN_SIZE << ")" << std::endl;
    std::cout << " -x <batch_max_size>\tmaximum batch size (default: " << DEFAULT_BATCH_MAX_SIZE << ")" << std::endl;
    std::cout << " -u <batch_time_us>\tmaximum time to wait for the batch minimum size, in microseconds (default: " << DEFAULT_BATCH_TIME_US << ")" << std::endl;
    std::cout << " -a\t\t\tdo not reset the service (Note: this can lead to incorrect final balances if re-executing the same benchmark)" << std::endl;
    std::cout << " -m\t\t\tskip minting step" << std::endl;
    std::cout << " -s\t\t\tskip transfer step" << std::endl;
    std::cout << " -c\t\t\tskip check step" << std::endl;
    std::cout << " -h\t\t\tshow this help" << std::endl;
}

int main(int argc, char** argv){
    char c;
    std::string fname,remote_logs;
    uint64_t send_rate = 0;
    uint64_t wait_time = DEFAULT_SECONDS_AFTER_STEP;
    bool mint_step = true;
    bool transfer_step = true;
    bool check_step = true;
    bool rate_control = false;
    bool reset_service = true;
    uint64_t batch_min_size = DEFAULT_BATCH_MIN_SIZE;
    uint64_t batch_max_size = DEFAULT_BATCH_MAX_SIZE;
    uint64_t batch_time_us = DEFAULT_BATCH_TIME_US;

    while ((c = getopt(argc, argv, "o:r:w:l:b:x:u:amsch")) != -1){
        switch(c){
            case 'o':
                fname = optarg;
                break;
            case 'l':
                remote_logs = optarg;
                break;
            case 'r':
                send_rate = strtoul(optarg,NULL,10);
                break;
            case 'w':
                wait_time = strtoul(optarg,NULL,10);
                break;
            case 'b':
                batch_min_size = strtoul(optarg,NULL,10);
                break;
            case 'x':
                batch_max_size = strtoul(optarg,NULL,10);
                break;
            case 'u':
                batch_time_us = strtoul(optarg,NULL,10);
                break;
            case 'a':
                reset_service = false;
                break;
            case 'm':
                mint_step = false;
                break;
            case 's':
                transfer_step = false;
                break;
            case 'c':
                check_step = false;
                break;
            case '?':
            case 'h':
            default:
                print_help(argv[0]);
                return 0;
        }
    }

    if(optind >= argc){
        print_help(argv[0]);
        return 0;
    }

    std::string workload_file(argv[optind]);

    if(fname.empty()){
        fname = workload_file + ".log";
    }
    
    if(remote_logs.empty()){
        remote_logs = "cbdc.log";
    }

    CascadeCBDC cbdc;
    CBDCBenchmarkWorkload& benchmark = CBDCBenchmarkWorkload::from_file(workload_file);
    std::unordered_map<uint64_t,transaction_id_t> transfer_id;

    std::cout << "setting up ..." << std::endl;
    std::cout << "  workload_file = " << workload_file << std::endl;
    std::cout << "  send_rate = " << send_rate << std::endl;
    std::cout << "  wait_time = " << wait_time << std::endl;
    std::cout << "  batch_min_size = " << batch_min_size << std::endl;
    std::cout << "  batch_max_size = " << batch_max_size << std::endl;
    std::cout << "  batch_time_us = " << batch_time_us << std::endl;
    std::cout << "  output_file = " << fname << std::endl;
    std::cout << "  remote_log = " << remote_logs << std::endl;

    cbdc.setup(batch_min_size,batch_max_size,batch_time_us); 

    std::chrono::nanoseconds iteration_time;
    if(send_rate != 0){
        rate_control = true;
        iteration_time = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(1)) / send_rate;
    }

    // reset the CBDC UDL state in all nodes
    if(reset_service){
        std::cout << "resetting the CBDC service ..." << std::endl;
        cbdc.reset();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // mint coins
    if(mint_step){
        std::cout << "minting wallets ..." << std::endl;
        auto& wallets = benchmark.get_wallets();

        auto extra_time = std::chrono::nanoseconds(0);
        transaction_id_t last_tx;
        for(auto& wallet : wallets){
            auto start = std::chrono::steady_clock::now();
            last_tx = cbdc.mint(wallet.first,wallet.second);
            auto end = std::chrono::steady_clock::now();

            if(rate_control){
                auto elapsed = end - start + extra_time;
                auto sleep_time = iteration_time - elapsed;
                start = std::chrono::steady_clock::now();
                std::this_thread::sleep_for(sleep_time);
                extra_time = std::chrono::steady_clock::now() - start - sleep_time;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // poll until last TX is finished
        std::cout << "waiting last mint to finish ..." << std::endl;
        auto poll_interval = std::chrono::milliseconds(LAST_TX_POLL_INTERVAL_MS);
        while(cbdc.get_status(last_tx) == transaction_status_t::UNKNOWN){
            std::this_thread::sleep_for(poll_interval);
        }
        std::this_thread::sleep_for(std::chrono::seconds(wait_time));
    }

    // perform transfers
    if(transfer_step){
        auto& transfers = benchmark.get_transfers();
        std::cout << "performing " << transfers.size() << " transfers ..." << std::endl;
       
        auto extra_time = std::chrono::nanoseconds(0);
        for(uint64_t i=0;i<transfers.size();i++){
            auto start = std::chrono::steady_clock::now();
            auto& transfer = transfers[i];
            auto txid = cbdc.transfer(transfer.senders,transfer.receivers);
            transfer_id[i] = txid;
            auto end = std::chrono::steady_clock::now();
            
            if(rate_control){
                auto elapsed = end - start + extra_time;
                auto sleep_time = iteration_time - elapsed;
                start = std::chrono::steady_clock::now();
                std::this_thread::sleep_for(sleep_time);
                extra_time = std::chrono::steady_clock::now() - start - sleep_time;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // poll until last TX is finished
        std::cout << "waiting last TX to finish ..." << std::endl;
        auto last_tx = transfer_id[transfers.size()-1];
        auto poll_interval = std::chrono::milliseconds(LAST_TX_POLL_INTERVAL_MS);
        while(cbdc.get_status(last_tx) == transaction_status_t::UNKNOWN){
            std::this_thread::sleep_for(poll_interval);
        }
        std::this_thread::sleep_for(std::chrono::seconds(wait_time));
    }

    // check final values
    if(check_step){
        auto& expected_balance = benchmark.get_expected_balance();
        auto& expected_status = benchmark.get_expected_status();
        uint64_t error_count = 0;

        // check balances
        std::cout << "checking " << expected_balance.size() << " final balances ..." << std::endl;
        for(auto& item : expected_balance){
            auto wallet = cbdc.get_wallet(item.first);
            auto balance = CBDC_COMPUTE_WALLET_BALANCE(wallet);
            if(balance != item.second){
                error_count++;
                //std::cout << "  - balance error for wallet " << item.first << ": expected " << item.second << " but got " << balance << std::endl;
            }
        }

        std::cout << "  " << error_count << " balance errors found" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // check status
        std::cout << "checking " << expected_status.size() << " final status ..." << std::endl;
        error_count = 0;
        for(uint64_t i = 0;i<expected_status.size();i++){
            auto& txid = transfer_id[i];
            auto status = cbdc.get_status(txid);
            transaction_status_t expected = transaction_status_t::ABORT;
            if(expected_status.at(i)){
                expected = transaction_status_t::COMMIT;
            }

            if(status != expected){
                error_count++;
                //std::cout << "  - status error for TX (" << i << "," << txid << "): expected " << cbdc.status_to_string(expected) << " but got " << cbdc.status_to_string(status) << std::endl;
            }
        }
        
        std::cout << "  " << error_count << " status errors found" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(wait_time));
    }

    // write measurements
    std::cout << "writing log to '" << fname << "' ..." << std::endl;
    cbdc.write_logs(fname,remote_logs);

    std::cout << "done" << std::endl;
    return 0;
}

