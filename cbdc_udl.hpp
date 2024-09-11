#pragma once

#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/service_client_api.hpp>
#include <cascade/utils.hpp>
#include <iostream>
#include <string>
#include <algorithm>
#include <utility>
#include <tuple>
#include <shared_mutex>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <queue>
#include <deque>
#include <thread>
#include <mutex>
#include <chrono>
#include "common.hpp"

enum class operation_type_t : uint8_t {
    NONE,
    MINT,
    TRANSFER,
    REDEEM,
    FORWARD,
    COMMIT,
    ABORT
};

using internal_transaction_t = struct internal_transaction_t {
    cbdc_request_t *request;
    transaction_status_t status;
};

using queued_operation_t = std::tuple<operation_type_t,wallet_id_t,internal_transaction_t*>;

using queued_wallet_t = std::tuple<wallet_id_t,wallet_t,transaction_id_t>;

using queued_chain_t = std::tuple<operation_type_t,wallet_id_t,cbdc_request_t*>;

#define CBDC_REQUEST_FORWARD_PREFIX CBDC_REQUEST_PREFIX "/forward/WID_" // + wallet_id
#define CBDC_REQUEST_COMMIT_PREFIX CBDC_REQUEST_PREFIX "/commit/WID_" // + wallet_id
#define CBDC_REQUEST_ABORT_PREFIX CBDC_REQUEST_PREFIX "/abort/WID_" // + wallet_id

inline std::string CBDC_BUILD_FORWARD_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_FORWARD_PREFIX + std::to_string(wallet_id);
}

inline std::string CBDC_BUILD_COMMIT_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_COMMIT_PREFIX + std::to_string(wallet_id);
}

inline std::string CBDC_BUILD_ABORT_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_ABORT_PREFIX + std::to_string(wallet_id);
}

namespace derecho{
namespace cascade{

#define UDL_UUID    "583ba368-eb78-4b59-b44e-cbc51d013c93"
#define UDL_DESC    "UDL implementing the Cascade CBDC service."

class CascadeCBDC: public DefaultOffCriticalDataPathObserver {
    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
    
    class CBDCThread {
    private:
        uint64_t my_thread_id;
        CascadeCBDC* udl;
        node_id_t node_id;
        std::thread real_thread;
        ServiceClientAPI& capi = ServiceClientAPI::get_service_client();

        bool running = false;
        std::mutex thread_mtx;
        std::queue<queued_operation_t*> operation_queue;
        std::condition_variable thread_signal;

        std::unordered_map<wallet_id_t,wallet_t> wallet_cache; // committed state of wallets
        std::unordered_map<wallet_id_t,coin_value_t> committed_balance; // committed state of wallets
        std::unordered_map<wallet_id_t,coin_value_t> virtual_balance; // balance of a wallet when taking into account all running TXs

        std::list<internal_transaction_t*> pending_transactions;
        std::unordered_map<internal_transaction_t*,std::list<internal_transaction_t*>::iterator> pending_transaction_it;
        std::unordered_map<internal_transaction_t*,std::list<internal_transaction_t*>> forward_conflicts;  // TODO use unordered_set here?
        std::unordered_map<internal_transaction_t*,std::list<internal_transaction_t*>> backward_conflicts; // TODO use unordered_set here?
        std::unordered_map<wallet_id_t,std::unordered_set<internal_transaction_t*>> pending_transactions_wallet_dependencies;
        
        std::unordered_map<internal_transaction_t*,std::unordered_map<wallet_id_t,std::unordered_map<operation_type_t,bool>>> already_handled;
        std::unordered_map<internal_transaction_t*,std::list<wallet_id_t>> pending_wallets; // wallets in a running TX pending in this thread

        void main_loop();

        // wallet operations
        void fetch_wallet(wallet_id_t wallet_id);
        coin_value_t add_to_wallet(wallet_t &wallet,coin_value_t value);
        coin_value_t remove_from_wallet(wallet_t &wallet,coin_value_t value);

        // queue and conflict tracking
        void enqueue_transaction(internal_transaction_t* tx,wallet_id_t wallet_id);
        bool dequeue_transaction(internal_transaction_t* tx,wallet_id_t wallet_id);
        bool has_conflict(internal_transaction_t* tx,wallet_id_t wallet_id);
        bool is_valid(internal_transaction_t* tx,wallet_id_t wallet_id);
        
        // recursively check transactions to run
        void tx_run_recursive(internal_transaction_t* tx,wallet_id_t wallet_id);
        void tx_committed_recursive(internal_transaction_t* tx,wallet_id_t wallet_id);
        void tx_aborted_recursive(internal_transaction_t* tx,wallet_id_t wallet_id,bool adjust_virtual);
        
        // chain protocol
        std::tuple<bool,bool,uint32_t> is_mine(internal_transaction_t* tx,wallet_id_t wallet_id,wallet_id_t next_wallet_id); // check if this node is responsible for chaining, and if the next wallet goes to the same shard
        void send_tx_forward(internal_transaction_t* tx,wallet_id_t wallet_id);
        void send_status_backward(internal_transaction_t* tx,wallet_id_t wallet_id);

        // persistence
        void commit_transaction(internal_transaction_t* tx,wallet_id_t wallet_id);
        void persist_transaction(internal_transaction_t* tx);
        void persist_wallet(wallet_id_t wallet_id,internal_transaction_t* tx);
        bool is_my_persistence(uint64_t factor); // check if this node is responsible for persisting

    public:
        CBDCThread(uint64_t my_thread_id,CascadeCBDC *udl);
        void push_operation(queued_operation_t* queued_op);
        void signal_stop();

        inline void start(){
            running = true;
            real_thread = std::thread(&CBDCThread::main_loop,this);
        }

        inline void join(){
            real_thread.join();
        }
    };

    class WalletPersistenceThread {
    private:
        CascadeCBDC* udl;
        node_id_t node_id;
        std::thread real_thread;
        ServiceClientAPI& capi = ServiceClientAPI::get_service_client();

        bool running = false;
        std::mutex thread_mtx;
        std::queue<queued_wallet_t> wallet_queue;
        std::condition_variable thread_signal;

        void main_loop();
    
    public:
        WalletPersistenceThread(CascadeCBDC *udl);
        void push_wallet(queued_wallet_t &queued_wallet);
        void signal_stop();

        inline void start(){
            running = true;
            real_thread = std::thread(&WalletPersistenceThread::main_loop,this);
        }

        inline void join(){
            real_thread.join();
        }
    };
    
    class ChainingThread {
    private:
        CascadeCBDC* udl;
        node_id_t node_id;
        std::thread real_thread;
        ServiceClientAPI& capi = ServiceClientAPI::get_service_client();

        bool running = false;
        std::mutex thread_mtx;
        std::condition_variable thread_signal;
        std::unordered_map<uint32_t,std::queue<queued_chain_t>> chain_queues;

        void main_loop();
    
    public:
        ChainingThread(CascadeCBDC *udl);
        void push_chain(queued_chain_t &queued_chain,uint32_t next_shard);
        void signal_stop();

        inline void start(){
            running = true;
            real_thread = std::thread(&ChainingThread::main_loop,this);
        }

        inline void join(){
            real_thread.join();
        }
    };
    
    class TXPersistenceThread {
    private:
        CascadeCBDC* udl;
        node_id_t node_id;
        std::thread real_thread;
        ServiceClientAPI& capi = ServiceClientAPI::get_service_client();

        bool running = false;
        std::mutex thread_mtx;
        std::condition_variable thread_signal;
        std::unordered_map<uint32_t,std::queue<internal_transaction_t*>> tx_queues;

        void main_loop();
    
    public:
        TXPersistenceThread(CascadeCBDC *udl);
        void push_tx(internal_transaction_t* queued_tx,uint32_t shard);
        void signal_stop();

        inline void start(){
            running = true;
            real_thread = std::thread(&TXPersistenceThread::main_loop,this);
        }

        inline void join(){
            real_thread.join();
        }
    };

    // main thread
    node_id_t my_id = 0;
    std::unordered_map<transaction_id_t,internal_transaction_t*> transaction_database; // TODO manage memory: currently TXs are kept forever in memory

    void start_threads();
    operation_type_t operation_str_to_type(const std::string &operation_str);

    virtual void ocdpo_handler(
            const node_id_t             sender,
            const std::string&          object_pool_pathname,
            const std::string&          key_string,
            const ObjectWithStringKey&  object,
            const emit_func_t&          emit,
            DefaultCascadeContextType*  typed_ctxt,
            uint32_t                    worker_id);
public:
    CascadeCBDC();
    cascade_cbdc_config_t config;
    std::deque<CBDCThread> threads;
    WalletPersistenceThread* wallet_thread;
    ChainingThread* chain_thread;
    TXPersistenceThread* tx_thread;
    
    void set_config(DefaultCascadeContextType* typed_ctxt,const nlohmann::json& config);
    void stop();

    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<CascadeCBDC>();
        }
    }
    
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> CascadeCBDC::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    //initialize observer
    CascadeCBDC::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,const nlohmann::json& config) {
    auto typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    std::static_pointer_cast<CascadeCBDC>(CascadeCBDC::get())->set_config(typed_ctxt,config);
    return CascadeCBDC::get();
}

void release(ICascadeContext* ctxt) {
    std::static_pointer_cast<CascadeCBDC>(CascadeCBDC::get())->stop();
    return;
}

std::string get_uuid() {
    return UDL_UUID;
}

std::string get_description() {
    return UDL_DESC;
}

} // namespace cascade
} // namespace derecho

