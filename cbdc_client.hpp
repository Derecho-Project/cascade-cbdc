#pragma once

#include <cascade/service_client_api.hpp>
#include <string>
#include <chrono>
#include <thread>
#include <memory>
#include <tuple>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <limits>
#include "common.hpp"

using namespace derecho::cascade;

enum class thread_request_t : uint8_t {
    MINT,
    TRANSFER,
    REDEEM
};

using queued_request_t = std::pair<thread_request_t,cbdc_request_t*>;

class CascadeCBDC {
    class ClientThread {
    private:
        std::thread real_thread;
        ServiceClientAPI& capi = ServiceClientAPI::get_service_client();
        uint64_t node_id = capi.get_my_id();
        uint64_t batch_min_size = 0;
        uint64_t batch_max_size = 16;
        uint64_t batch_time_us = 10000;

        bool running = false;
        std::mutex thread_mtx;
        std::condition_variable thread_signal;
        std::unordered_map<uint32_t,std::queue<queued_request_t>> request_queues;

        void main_loop();

    public:
        ClientThread(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us);
        void push_request(queued_request_t &queued_request,uint32_t shard);
        void signal_stop();

        inline void start(){
            running = true;
            real_thread = std::thread(&ClientThread::main_loop,this);
        }

        inline void join(){
            real_thread.join();
        }
    };

    ServiceClientAPI& capi = ServiceClientAPI::get_service_client();
    uint64_t my_id = capi.get_my_id();
    uint64_t tx_count = 0;
    cascade_cbdc_config_t config;
    ClientThread *client_thread;

    std::mutex txid_mtx;
    transaction_id_t next_transaction_id();
    
    public:

    CascadeCBDC();
    ~CascadeCBDC();
    
    void setup(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us);
    
    transaction_id_t mint(wallet_id_t wallet_id,coin_value_t value);
    transaction_id_t transfer(const std::unordered_map<wallet_id_t,coin_value_t>& senders,const std::unordered_map<wallet_id_t,coin_value_t>& receivers);
    transaction_id_t redeem(wallet_id_t wallet_id,coin_value_t value);

    wallet_t get_wallet(wallet_id_t wallet_id);
    transaction_status_t get_status(const transaction_id_t& txid);
    
    void write_logs(const std::string local_log,const std::string remote_logs);

    // helper methods
    static std::string status_to_string(const transaction_status_t status);
};

