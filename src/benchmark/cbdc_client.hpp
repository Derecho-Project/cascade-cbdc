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

using signature_callback_t = std::function<void(persistent::version_t, persistent::version_t,
                                                const std::vector<uint8_t>&, persistent::version_t,
                                                const std::vector<uint8_t>&)>;

class SignatureNotificationHandler {
private:
    std::map<persistent::version_t, signature_callback_t> callbacks_by_version;

public:
    void operator()(const Blob& message_body) {
        // Peek at the data version, which is either the first or second element in the message,
        // depending on if evaluation is enabled (the message ID comes first if evaluation is enabled)
        std::size_t data_version_offset = 0;
#ifdef ENABLE_EVALUATION
        data_version_offset = sizeof(uint64_t);
#endif
        persistent::version_t data_object_version;
        std::memcpy(&data_object_version, message_body.bytes + data_version_offset, sizeof(data_object_version));
        // If there is a callback registered for this version, call it, then delete it
        auto find_callback = callbacks_by_version.find(data_object_version);
        if(find_callback != callbacks_by_version.end()) {
            // Skip past the evaluation message ID if evaluation is enabled
            mutils::deserialize_and_run(nullptr, message_body.bytes + data_version_offset, find_callback->second);
            callbacks_by_version.erase(find_callback);
        }
    }
    void register_callback(persistent::version_t desired_data_version, const signature_callback_t& callback) {
        callbacks_by_version.emplace(desired_data_version, callback);
    }
    SignatureNotificationHandler() = default;
    // Copying this object would mean the main thread loses the ability to register callbacks
    // There needs to be only one copy of it shared between the main thread and the Cascade notification handler
    SignatureNotificationHandler(const SignatureNotificationHandler&) = delete;
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

    std::unique_ptr<openssl::Verifier> service_verifier;
    SignatureNotificationHandler signature_notification_handler;
    std::set<std::string> subscribed_notification_keys;
    std::mutex txid_mtx;
    bool signature_pool_handler_registered = false;
    transaction_id_t next_transaction_id();
    
    public:

    CascadeCBDC();
    ~CascadeCBDC();
    
    void setup(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us);
    
    transaction_id_t mint(wallet_id_t wallet_id,coin_value_t value);
    transaction_id_t transfer(const std::unordered_map<wallet_id_t,coin_value_t>& senders,const std::unordered_map<wallet_id_t,coin_value_t>& receivers);
    transaction_id_t redeem(wallet_id_t wallet_id,coin_value_t value);

    bool put_with_signature(thread_request_t operation, cbdc_request_t& request);

    wallet_t get_wallet(wallet_id_t wallet_id);
    transaction_status_t get_status(const transaction_id_t& txid);
   
    void reset(); 
    void write_logs(const std::string local_log,const std::string remote_logs);

    std::vector<uint8_t> compute_hash(const ObjectWithStringKey& data_obj);
    bool load_service_key(const std::string& pem_path);
    bool verify_object_signature(const ObjectWithStringKey& hash,
                                 const std::vector<uint8_t>& signature,
                                 const std::vector<uint8_t>& previous_signature);

    // helper methods
    static std::string status_to_string(const transaction_status_t status);
};

