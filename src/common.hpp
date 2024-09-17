#pragma once

#include <cstdint>
#include <tuple>
#include <utility>
#include <unordered_map>
#include <string>
#include <vector>

// basic CBDC types
using wallet_id_t = uint64_t;
using coin_value_t = uint64_t;
using wallet_t = coin_value_t; // TODO use separate coins instead of just a balance?
using transaction_id_t = uint64_t; // std::hash | TODO use something bigger for lower chance of collision?
using cbdc_request_t = std::tuple<transaction_id_t,std::unordered_map<wallet_id_t,coin_value_t>,std::unordered_map<wallet_id_t,coin_value_t>,std::vector<wallet_id_t>>; // txid, source, destination, sorted_wallets

enum class transaction_status_t : uint8_t {
    PENDING,
    RUNNING,
    COMMIT,
    ABORT,
    UNKNOWN
};

using transaction_t = std::tuple<cbdc_request_t,transaction_status_t>; // request, status

using cascade_cbdc_config_t = struct cascade_cbdc_config_t {
    bool enable_cross_thread_communication;             // thread send a request directly to another thread if next wallet is in the same shard (instead of multicasting)
    bool enable_wallet_persistence_thread;              // start a thread responsible for putting wallets in batches (instead of individually putting them in each thread)
    bool enable_tx_persistence_thread;                  // start a thread responsible for putting TXs
    bool enable_chaining_thread;                        // start a thread responsible for chaining requests (instead of doing it in each thread)
    bool enable_virtual_balance;                        // ignore conflict if the wallet is handled by the same thread and there are enough virtual funds
    bool enable_source_only_conflicts;                  // ignore destination wallets when checking for conflicts

    uint64_t num_threads;                               // number of worker threads

    uint64_t wallet_persistence_batch_min_size;         // batch minimum size for the wallet persistence thread
    uint64_t wallet_persistence_batch_max_size;         // batch maximum size for the wallet persistence thread
    uint64_t wallet_persistence_batch_time_us;          // maximum time to wait for the batch size (in microseconds)

    uint64_t chaining_batch_min_size;                   // batch minimum size for the chaining thread
    uint64_t chaining_batch_max_size;                   // batch maximum size for the chaining thread
    uint64_t chaining_batch_time_us;                    // maximum time to wait for the batch size (in microseconds)
    
    uint64_t tx_persistence_batch_min_size;             // batch minimum size for the tx persistence thread
    uint64_t tx_persistence_batch_max_size;             // batch maximum size for the tx persistence thread
    uint64_t tx_persistence_batch_time_us;              // maximum time to wait for the batch size (in microseconds)
};

// cascade key paths
#define CBDC_PREFIX "/cbdc"

// keys for client requests
#define CBDC_REQUEST_PREFIX CBDC_PREFIX "/r"
#define CBDC_REQUEST_MINT_PREFIX CBDC_REQUEST_PREFIX "/m/WID_" // + wallet_id
#define CBDC_REQUEST_TRANSFER_PREFIX CBDC_REQUEST_PREFIX "/t/WID_" // + wallet_id
#define CBDC_REQUEST_REDEEM_PREFIX CBDC_REQUEST_PREFIX "/r/WID_" // + wallet_id
#define CBDC_REQUEST_LOG_KEY CBDC_REQUEST_PREFIX "/log"
#define CBDC_REQUEST_INIT_KEY CBDC_REQUEST_PREFIX "/init"
#define CBDC_REQUEST_RESET_KEY CBDC_REQUEST_PREFIX "/reset"

// keys for storing wallets
#define CBDC_WALLET_PREFIX CBDC_PREFIX "/w/WID_" // + wallet_id

// keys for storing transactions
#define CBDC_TRANSACTION_PREFIX CBDC_PREFIX "/tx/" // + transaction_id

// object pool config
#define CBDC_OBJECT_POOL_PREFIX CBDC_PREFIX
#define CBDC_OBJECT_POOL_TYPE PersistentCascadeStoreWithStringKey
#define CBDC_OBJECT_POOL_SUBGROUP 0
#define CBDC_OBJECT_POOL_REGEX "/WID_[0-9]+" // group based on wallet ID

// service config
#define CBDC_CONFIG_KEY CBDC_PREFIX "/config"

// logging

#define CBDC_TAG_CLIENT_DEPLOYMENT_INFO 100005
#define CBDC_TAG_CLIENT_TRANSFER_START 100010
#define CBDC_TAG_CLIENT_TRANSFER_QUEUE 100020
#define CBDC_TAG_CLIENT_TRANSFER_SENDING 100050
#define CBDC_TAG_CLIENT_TRANSFER_SENT 100080
#define CBDC_TAG_CLIENT_STATUS 100100
#define CBDC_TAG_CLIENT_BATCHING 100110

#define CBDC_TAG_UDL_HANDLER_START 200010
#define CBDC_TAG_UDL_HANDLER_QUEUING 200020
#define CBDC_TAG_UDL_HANDLER_END 200030

#define CBDC_TAG_UDL_OPERATION_START 200040
#define CBDC_TAG_UDL_OPERATION_END 200050
#define CBDC_TAG_UDL_ENQUEUE_END 200060
#define CBDC_TAG_UDL_WALLET_PERSIST_START 200070
#define CBDC_TAG_UDL_WALLET_PERSIST_END 200080
#define CBDC_TAG_UDL_TX_PERSIST_START 200090
#define CBDC_TAG_UDL_TX_PERSIST_END 200100
#define CBDC_TAG_UDL_NEW_START 200110
#define CBDC_TAG_UDL_RUN_START 200115
#define CBDC_TAG_UDL_COMMIT_START 200120
#define CBDC_TAG_UDL_ABORT_START 200130
#define CBDC_TAG_UDL_FORWARD_START 200140
#define CBDC_TAG_UDL_FORWARD_END 200150
#define CBDC_TAG_UDL_BACKWARD_START 200160
#define CBDC_TAG_UDL_BACKWARD_END 200170
#define CBDC_TAG_UDL_WALLET_BATCHING 200180
#define CBDC_TAG_UDL_CHAIN_BATCHING 200190
#define CBDC_TAG_UDL_TX_BATCHING 200200

// helpers

inline std::string CBDC_BUILD_WALLET_KEY(wallet_id_t wallet_id){
    return CBDC_WALLET_PREFIX + std::to_string(wallet_id);
}

inline std::string CBDC_BUILD_TRANSACTION_KEY(transaction_id_t txid){
    return CBDC_TRANSACTION_PREFIX + std::to_string(txid);
}

inline std::string CBDC_BUILD_MINT_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_MINT_PREFIX + std::to_string(wallet_id);
}

inline std::string CBDC_BUILD_TRANSFER_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_TRANSFER_PREFIX + std::to_string(wallet_id);
}

inline std::string CBDC_BUILD_REDEEM_KEY(wallet_id_t wallet_id){
    return CBDC_REQUEST_REDEEM_PREFIX + std::to_string(wallet_id);
}

inline coin_value_t CBDC_COMPUTE_WALLET_BALANCE(wallet_t &wallet){
    return wallet;
}

