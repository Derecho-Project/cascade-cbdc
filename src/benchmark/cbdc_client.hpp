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
    struct SigInfo {
        persistent::version_t data_ver{};
        persistent::version_t hash_ver{};
        persistent::version_t prev_signed_ver{};
        std::vector<uint8_t> sig;
        std::vector<uint8_t> prev_sig;
    };

    std::mutex sig_mtx;
    std::condition_variable sig_cv;
    // buffer by data version -> SigInfo
    std::unordered_map<persistent::version_t, SigInfo> sig_buffer;

public:
    void operator()(const Blob& message_body) {
        std::cout << "[DEBUG] operator is being invoked\n";
        try {
            std::size_t off = 0;
            // If you control the format, add a tiny magic to the first 8 bytes when eval is enabled.
            // Otherwise, only skip if message is large enough AND you know both sides compiled with eval.
#ifdef ENABLE_EVALUATION
            if (message_body.size >= sizeof(uint64_t)) {
                off = sizeof(uint64_t);
            }
#endif
            auto deliver = [this](persistent::version_t data_ver,
                                persistent::version_t hash_ver,
                                const std::vector<uint8_t>& sig,
                                persistent::version_t prev_signed_ver,
                                const std::vector<uint8_t>& prev_sig) {
                signature_callback_t cb_to_call{};
                {
                    std::lock_guard<std::mutex> lk(sig_mtx);
                    sig_buffer[data_ver] = SigInfo{data_ver, hash_ver, prev_signed_ver, sig, prev_sig};
                    auto it = callbacks_by_version.find(data_ver);
                    if (it == callbacks_by_version.end()) it = callbacks_by_version.find(hash_ver);
                    if (it != callbacks_by_version.end()) { cb_to_call = it->second; callbacks_by_version.erase(it); }
                }
                sig_cv.notify_all();
                if (cb_to_call) cb_to_call(data_ver, hash_ver, sig, prev_signed_ver, prev_sig);
            };

            // add logging to confirm we actually enter here
            // std::cerr << "[sig] blob size=" << message_body.size << " off=" << off << "\n";

            try {
                mutils::deserialize_and_run(nullptr, message_body.bytes + off, deliver);
            } catch (const std::exception& e) {
                std::cerr << "[sig] desrialization error: " << e.what() << "\n";
            }


        } catch (const std::exception& e) {
            std::cerr << "[sig] deserialize error: " << e.what() << "\n";
        }
    }

    void register_callback(persistent::version_t desired_data_version,
                        const signature_callback_t& callback) {
        // If the notification already arrived, deliver immediately
        SigInfo info;
        bool have_info = false;
        {
            std::lock_guard<std::mutex> lk(sig_mtx);
            auto it = sig_buffer.find(desired_data_version);
            if (it != sig_buffer.end()) {
                info = it->second;
                sig_buffer.erase(it);
                have_info = true;
            } else {
                callbacks_by_version.emplace(desired_data_version, callback);
            }
        }
        if (have_info) {
            callback(info.data_ver, info.hash_ver, info.sig, info.prev_signed_ver, info.prev_sig);
        }
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

