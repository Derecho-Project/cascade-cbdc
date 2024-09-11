
#include "cbdc_udl.hpp"

namespace derecho{
namespace cascade{

CascadeCBDC::CascadeCBDC(){
    config.enable_cross_thread_communication = false;
    config.enable_wallet_persistence_thread = false;
    config.enable_tx_persistence_thread = false;
    config.enable_chaining_thread = false;
    config.enable_virtual_balance = false;

    config.num_threads = 1;
    
    config.wallet_persistence_batch_min_size = 0;
    config.wallet_persistence_batch_max_size = 8;
    config.wallet_persistence_batch_time_us = 1000;
    
    config.chaining_batch_min_size = 0;
    config.chaining_batch_max_size = 8;
    config.chaining_batch_time_us = 1000;
    
    config.tx_persistence_batch_min_size = 0;
    config.tx_persistence_batch_max_size = 8;
    config.tx_persistence_batch_time_us = 1000;
}

void CascadeCBDC::set_config(DefaultCascadeContextType* typed_ctxt,const nlohmann::json& config){
    my_id = typed_ctxt->get_service_client_ref().get_my_id();
 
    if(config.count("enable_cross_thread_communication") > 0){
        this->config.enable_cross_thread_communication = std::string(config["enable_cross_thread_communication"]) != "0";
    }
    
    if(config.count("enable_wallet_persistence_thread") > 0){
        this->config.enable_wallet_persistence_thread = std::string(config["enable_wallet_persistence_thread"]) != "0";
    }
    
    if(config.count("enable_tx_persistence_thread") > 0){
        this->config.enable_tx_persistence_thread = std::string(config["enable_tx_persistence_thread"]) != "0";
    }
    
    if(config.count("enable_chaining_thread") > 0){
        this->config.enable_chaining_thread = std::string(config["enable_chaining_thread"]) != "0";
    }
    
    if(config.count("enable_virtual_balance") > 0){
        this->config.enable_virtual_balance = std::string(config["enable_virtual_balance"]) != "0";
    }
    
    if(config.count("num_threads") > 0){
        this->config.num_threads = std::stoull(std::string(config["num_threads"]));
    }
    
    if(config.count("wallet_persistence_batch_min_size") > 0){
        this->config.wallet_persistence_batch_min_size = std::stoull(std::string(config["wallet_persistence_batch_min_size"]));
    }
    
    if(config.count("wallet_persistence_batch_max_size") > 0){
        this->config.wallet_persistence_batch_max_size = std::stoull(std::string(config["wallet_persistence_batch_max_size"]));
    }
    
    if(config.count("wallet_persistence_batch_time_us") > 0){
        this->config.wallet_persistence_batch_time_us = std::stoull(std::string(config["wallet_persistence_batch_time_us"]));
    }
    
    if(config.count("chaining_batch_min_size") > 0){
        this->config.chaining_batch_min_size = std::stoull(std::string(config["chaining_batch_min_size"]));
    }
    
    if(config.count("chaining_batch_max_size") > 0){
        this->config.chaining_batch_max_size = std::stoull(std::string(config["chaining_batch_max_size"]));
    }
    
    if(config.count("chaining_batch_time_us") > 0){
        this->config.chaining_batch_time_us = std::stoull(std::string(config["chaining_batch_time_us"]));
    }
    
    if(config.count("tx_persistence_batch_min_size") > 0){
        this->config.tx_persistence_batch_min_size = std::stoull(std::string(config["tx_persistence_batch_min_size"]));
    }
    
    if(config.count("tx_persistence_batch_max_size") > 0){
        this->config.tx_persistence_batch_max_size = std::stoull(std::string(config["tx_persistence_batch_max_size"]));
    }
    
    if(config.count("tx_persistence_batch_time_us") > 0){
        this->config.tx_persistence_batch_time_us = std::stoull(std::string(config["tx_persistence_batch_time_us"]));
    }

    start_threads();
}

operation_type_t CascadeCBDC::operation_str_to_type(const std::string &operation_str){
    if(operation_str == "mint") return operation_type_t::MINT;
    else if(operation_str == "transfer") return operation_type_t::TRANSFER;
    else if(operation_str == "redeem") return operation_type_t::REDEEM;
    else if(operation_str == "forward") return operation_type_t::FORWARD;
    else if(operation_str == "commit") return operation_type_t::COMMIT;
    else if(operation_str == "abort") return operation_type_t::ABORT;
    return operation_type_t::NONE;
}

void CascadeCBDC::start_threads(){
    if(config.enable_tx_persistence_thread){
        tx_thread = new TXPersistenceThread(this);
        tx_thread->start();
    }

    if(config.enable_wallet_persistence_thread){
        wallet_thread = new WalletPersistenceThread(this);
        wallet_thread->start();
    }
    
    if(config.enable_chaining_thread){
        chain_thread = new ChainingThread(this);
        chain_thread->start();
    }

    for(uint64_t i=0;i<config.num_threads;i++){
        threads.emplace_back(i,this);
    }

    for(auto &t : threads){
        t.start();
    }
}

void CascadeCBDC::stop(){
    for(auto &t : threads){
        t.signal_stop();
    }
    
    for(auto &t : threads){
        t.join();
    }
    
    if(config.enable_chaining_thread){
        chain_thread->signal_stop();
        chain_thread->join();
    }
     
    if(config.enable_wallet_persistence_thread){
        wallet_thread->signal_stop();
        wallet_thread->join();
    }

    if(config.enable_tx_persistence_thread){
        tx_thread->signal_stop();
        tx_thread->join();
    }
}

void CascadeCBDC::ocdpo_handler(
        const node_id_t             sender,
        const std::string&          object_pool_pathname,
        const std::string&          key_string,
        const ObjectWithStringKey&  object,
        const emit_func_t&          emit,
        DefaultCascadeContextType*  typed_ctxt,
        uint32_t                    worker_id){

    if(key_string == "log"){ // flush timestamp log for measurements
        TimestampLogger::flush(reinterpret_cast<const char *>(object.blob.bytes));
        return;
    }
    
    if(key_string == "init"){ // write UDL config so clients can get it
        auto shard_index = typed_ctxt->get_service_client_ref().get_my_shard<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP);
        auto shard = typed_ctxt->get_service_client_ref().get_shard_members<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP,shard_index);
        std::sort(shard.begin(),shard.end());

        // only one node write the config
        if(my_id == shard[0]){
            ObjectWithStringKey obj;
            obj.key = CBDC_CONFIG_KEY;
            obj.blob = Blob([&](uint8_t* buffer,const std::size_t size){
                    return mutils::to_bytes(config, buffer);
                },mutils::bytes_size(config));

            typed_ctxt->get_service_client_ref().put_and_forget(obj);
        }
        return;
    }

    // {operation_str}/WID_{wallet_id}
    std::string operation_str = key_string.substr(0,key_string.find("/"));
    wallet_id_t wallet_id = std::stoull(key_string.substr(key_string.find("_")+1));
    operation_type_t operation = operation_str_to_type(operation_str);
    auto request = mutils::from_bytes<cbdc_request_t>(nullptr,object.blob.bytes).release();
    transaction_id_t txid = std::get<0>(*request);
    
    TimestampLogger::log(CBDC_TAG_UDL_HANDLER_START,my_id,txid,wallet_id);
    
    internal_transaction_t *tx = nullptr;
    switch(operation){
        case operation_type_t::MINT:
        case operation_type_t::TRANSFER:
        case operation_type_t::REDEEM:
        case operation_type_t::FORWARD: 
            {
                // check if already exists
                if(transaction_database.count(txid) > 0){
                    tx = transaction_database.at(txid);
                    delete request;
                    break;
                }

                // tx does not exist: create
                tx = new internal_transaction_t;
                tx->request = request;
                tx->status = transaction_status_t::PENDING;

                transaction_database.emplace(txid,tx);
            }
            break;

        case operation_type_t::COMMIT:
        case operation_type_t::ABORT:
            // ignore if transaction does not exist
            if(transaction_database.count(txid) == 0){
                break;
            }

            tx = transaction_database.at(txid);
            delete request;
            break;
        
        default:
            tx = nullptr;
            delete request;
    }

    if(tx != nullptr){
        // send to corresponding thread
        TimestampLogger::log(CBDC_TAG_UDL_HANDLER_QUEUING,my_id,txid,wallet_id);
        uint64_t to_thread = wallet_id % config.num_threads;
        queued_operation_t* queued_op = new queued_operation_t(operation,wallet_id,tx);
        threads[to_thread].push_operation(queued_op); 
    }
    
    TimestampLogger::log(CBDC_TAG_UDL_HANDLER_END,my_id,txid,wallet_id);
    //dbg_default_debug("[CBDC] operation {} invoked in node {} for wallet {} handled by thread {}",operation,my_id,wallet_id,to_thread);
}

// threads

CascadeCBDC::CBDCThread::CBDCThread(uint64_t my_thread_id,CascadeCBDC* udl){
    this->my_thread_id = my_thread_id;
    this->udl = udl;
    node_id = capi.get_my_id();
}

void CascadeCBDC::CBDCThread::push_operation(queued_operation_t* queued_op){
    std::unique_lock<std::mutex> lock(thread_mtx);
    operation_queue.push(queued_op);
    thread_signal.notify_all();
}

void CascadeCBDC::CBDCThread::signal_stop(){
    std::unique_lock<std::mutex> lock(thread_mtx);
    running = false;
    thread_signal.notify_all();
}

void CascadeCBDC::CBDCThread::main_loop(){
    if(!running) return;

    // thread main loop 
    while(true){
        std::unique_lock<std::mutex> lock(thread_mtx);
        if(operation_queue.empty()){
            thread_signal.wait(lock);
        }

        if(!running) break;

        auto queued_op = operation_queue.front();
        operation_queue.pop();
        lock.unlock();

        auto operation = std::get<0>(*queued_op);
        auto wallet_id = std::get<1>(*queued_op);
        auto tx = std::get<2>(*queued_op);
        auto request = tx->request;

        auto& txid = std::get<0>(*request);
        auto& sources = std::get<1>(*request);
        auto& destinations = std::get<2>(*request);
        auto& wallets = std::get<3>(*request);

        // check if this txid for this wallet_id was already received before: if yes, ignore
        if(already_handled[tx][wallet_id][operation]){
            delete queued_op;
            continue;
        }
        already_handled[tx][wallet_id][operation] = true;

        auto wallet_it = std::find(wallets.begin(),wallets.end(),wallet_id);
        if(wallet_it == wallets.end()) {
            delete queued_op;
            continue;
        }

        TimestampLogger::log(CBDC_TAG_UDL_OPERATION_START,node_id,txid,wallet_id);

        // check if wallet is in cache
        if(wallet_cache.count(wallet_id) == 0){
            fetch_wallet(wallet_id);
            committed_balance[wallet_id] = CBDC_COMPUTE_WALLET_BALANCE(wallet_cache[wallet_id]);
            virtual_balance[wallet_id] = committed_balance[wallet_id];
        }

        // perform operation
        switch(operation){
        case operation_type_t::MINT:
        case operation_type_t::TRANSFER:
        case operation_type_t::REDEEM:
        case operation_type_t::FORWARD:
            TimestampLogger::log(CBDC_TAG_UDL_NEW_START,node_id,txid,wallet_id);
            enqueue_transaction(tx,wallet_id);
            TimestampLogger::log(CBDC_TAG_UDL_ENQUEUE_END,node_id,txid,wallet_id);
            if(!has_conflict(tx,wallet_id)){
                TimestampLogger::log(CBDC_TAG_UDL_RUN_START,node_id,txid,wallet_id);
                tx_run_recursive(tx,wallet_id);
            }
            break;
        case operation_type_t::COMMIT:
            TimestampLogger::log(CBDC_TAG_UDL_COMMIT_START,node_id,txid,wallet_id);
            tx_committed_recursive(tx,wallet_id);
            break;
        case operation_type_t::ABORT:
            TimestampLogger::log(CBDC_TAG_UDL_ABORT_START,node_id,txid,wallet_id);
            tx_aborted_recursive(tx,wallet_id,true);
            break;
        }
    
        delete queued_op;
        TimestampLogger::log(CBDC_TAG_UDL_OPERATION_END,node_id,txid,wallet_id);
    }
    
}

void CascadeCBDC::CBDCThread::enqueue_transaction(internal_transaction_t* tx,wallet_id_t wallet_id){
    pending_wallets[tx].push_back(wallet_id);
    
    auto request = tx->request;
    auto& sources = std::get<1>(*request);
    auto& destinations = std::get<2>(*request);

    if(pending_transaction_it.count(tx) > 0){
        return;
    }
    
    pending_transactions.push_back(tx);
    pending_transaction_it[tx] = std::prev(pending_transactions.end());
    
    // if this operation only adds money, there is no conflict
    if(sources.empty()){
        return;
    }

    // first check if there is a conflict in general: this accelerates non-conflicting TXs, which should be the majority
    bool found = false;
    std::unordered_set<internal_transaction_t*> already_inserted;
    for(auto& src : sources){
        if(!pending_transactions_wallet_dependencies[src.first].empty()){
            // optimization: ignore conflict if the wallet is handled by this thread and there are enough virtual funds
            // this should speed up simple TXs with just one source wallet, which should be the majority of TXs
            if(udl->config.enable_virtual_balance && (virtual_balance.count(src.first) > 0) && (virtual_balance[src.first] >= src.second)){
                continue;
            }

            // add all previous txs that touch this wallet to the conflict tracking structures
            for(auto pending_tx : pending_transactions_wallet_dependencies[src.first]){
                if(already_inserted.count(pending_tx) == 0){
                    already_inserted.insert(pending_tx);
                    forward_conflicts[pending_tx].push_back(tx);
                    backward_conflicts[tx].push_back(pending_tx);
                }
            }

            found = true;
            break;
        }
    }

    if(!found) return;

    // update the map for general conflict checking
    for(auto& src : sources){
        pending_transactions_wallet_dependencies[src.first].insert(tx);
    } 
    for(auto& dest : destinations){
        pending_transactions_wallet_dependencies[dest.first].insert(tx);
    } 
}

bool CascadeCBDC::CBDCThread::dequeue_transaction(internal_transaction_t* tx,wallet_id_t wallet_id){
    pending_wallets[tx].erase(std::find(pending_wallets[tx].begin(),pending_wallets[tx].end(),wallet_id));
    if(pending_wallets[tx].empty()){
        pending_transactions.erase(pending_transaction_it[tx]);
        pending_transaction_it.erase(tx);
        pending_wallets.erase(tx);
    
        auto request = tx->request;
        auto& sources = std::get<1>(*request);
        auto& destinations = std::get<2>(*request);

        // update the map for general conflict checking
        for(auto& src : sources){
            pending_transactions_wallet_dependencies[src.first].erase(tx);
        } 
        for(auto& dest : destinations){
            pending_transactions_wallet_dependencies[dest.first].erase(tx);
        } 

        return true;
    }
    return false;
}

// check if an incoming TX (new_tx) coflicts with a TX already in the queue (old_tx)
bool CascadeCBDC::CBDCThread::conflicts(internal_transaction_t* new_tx,internal_transaction_t* old_tx,wallet_id_t wallet_id){
    auto new_request = new_tx->request;
    auto& new_sources = std::get<1>(*new_request);
    auto& new_destinations = std::get<2>(*new_request);
    
    auto old_request = old_tx->request;
    auto& old_sources = std::get<1>(*old_request);
    auto& old_destinations = std::get<2>(*old_request);

    for(auto& src : new_sources){
        if((old_sources.count(src.first) > 0) || (old_destinations.count(src.first) > 0)){
            return true;
        }
    }
    
    return false;
}

bool CascadeCBDC::CBDCThread::has_conflict(internal_transaction_t* tx,wallet_id_t wallet_id){
    return !((backward_conflicts.count(tx) == 0) || (backward_conflicts[tx].size() == 0));
}

bool CascadeCBDC::CBDCThread::is_valid(internal_transaction_t* tx,wallet_id_t wallet_id){
    auto request = tx->request;
    auto& sources = std::get<1>(*request);
   
    // a transaction only fails if there are not enough coins in a source wallet 
    if(sources.count(wallet_id) > 0){
        if(committed_balance[wallet_id] < sources[wallet_id]){
            return false;
        }
    }

    return true;
}

void CascadeCBDC::CBDCThread::tx_run_recursive(internal_transaction_t* tx,wallet_id_t wallet_id){
    auto request = tx->request;
    auto& sources = std::get<1>(*request);
    auto& wallets = std::get<3>(*request);

    tx->status = transaction_status_t::RUNNING;

    // first check if the TX is valid
    if(is_valid(tx,wallet_id)){
        if(sources.count(wallet_id) > 0){
            virtual_balance[wallet_id] -= sources[wallet_id];
        }

        // if this is the last wallet, commit
        if(wallet_id == wallets.back()){
            tx_committed_recursive(tx,wallet_id);
        } else {
            // this is not the last, send it forward
            send_tx_forward(tx,wallet_id);
        }
    } else {
        // abort
        tx_aborted_recursive(tx,wallet_id,false);
    }
}

void CascadeCBDC::CBDCThread::tx_committed_recursive(internal_transaction_t* tx,wallet_id_t wallet_id){
    auto request = tx->request;
    auto& wallets = std::get<3>(*request);
    
    tx->status = transaction_status_t::COMMIT;
    commit_transaction(tx,wallet_id);

    // send status backward if this is not the first wallet
    if(wallet_id != wallets.front()){
        send_status_backward(tx,wallet_id);
    } else {
        // persist the tx if this is the first
        persist_transaction(tx);
    }
    
    // only one wallet was committed, there may be others
    if(!dequeue_transaction(tx,wallet_id)){
        return;
    }

    // all wallets in the tx were committed, so we need to remove the tx from conflicts and check if other txs can run
    backward_conflicts.erase(tx);

    if(forward_conflicts.count(tx) > 0){
        // check transactions that are waiting this one
        for(internal_transaction_t* ahead_tx : forward_conflicts.at(tx)){
            // clear conflict
            auto& clist = backward_conflicts.at(ahead_tx);
            clist.erase(std::find(clist.begin(),clist.end(),tx));

            // check if ahead_tx can run
            if(clist.empty()){
                backward_conflicts.erase(ahead_tx);
                wallet_id_t start_wallet = pending_wallets[ahead_tx].front();
                tx_run_recursive(ahead_tx,start_wallet);
            }
        }

        forward_conflicts.erase(tx);
    }
}

void CascadeCBDC::CBDCThread::tx_aborted_recursive(internal_transaction_t* tx,wallet_id_t wallet_id,bool adjust_virtual){
    auto request = tx->request;
    auto& sources = std::get<1>(*request);
    auto& wallets = std::get<3>(*request);

    tx->status = transaction_status_t::ABORT;
    if(adjust_virtual && (sources.count(wallet_id) > 0)){
        virtual_balance[wallet_id] += sources[wallet_id];
    }
    
    // send backwards if this is not the first shard
    if(wallet_id != wallets.front()){
        send_status_backward(tx,wallet_id);
    } else {
        // persist the tx if this is the first
        persist_transaction(tx);
    }

    // only one wallet was aborted, there may be others
    if(!dequeue_transaction(tx,wallet_id)){
        return;
    }
    
    // tx was aborted, so we need to remove it from conflicts and check if other txs can run
    backward_conflicts.erase(tx);

    if(forward_conflicts.count(tx) > 0){
        // check transactions that are waiting this one
        for(internal_transaction_t* ahead_tx : forward_conflicts.at(tx)){
            // clear conflict
            auto& clist = backward_conflicts.at(ahead_tx);
            clist.erase(std::find(clist.begin(),clist.end(),tx));

            // check if ahead_op can run
            if(clist.empty()){
                backward_conflicts.erase(ahead_tx);
                wallet_id_t start_wallet = pending_wallets[ahead_tx].front();
                tx_run_recursive(ahead_tx,start_wallet);
            }
        }

        forward_conflicts.erase(tx);
    }
}

void CascadeCBDC::CBDCThread::commit_transaction(internal_transaction_t* tx,wallet_id_t wallet_id){
    auto request = tx->request;
    auto& sources = std::get<1>(*request);
    auto& destinations = std::get<2>(*request);
    auto& wallet = wallet_cache[wallet_id];

    // add coins
    if(destinations.count(wallet_id) > 0){
        auto value = destinations[wallet_id];
        add_to_wallet(wallet,value);
        committed_balance[wallet_id] += value;
        virtual_balance[wallet_id] += value;
    }

    // remove coins
    if(sources.count(wallet_id) > 0){
        auto value = sources[wallet_id];
        remove_from_wallet(wallet,value);
        committed_balance[wallet_id] -= value;
        // virtual_balance[wallet_id] was already updated in tx_run_recursive
    }

    // put new wallet
    persist_wallet(wallet_id,tx);
}

std::tuple<bool,bool,uint32_t> CascadeCBDC::CBDCThread::is_mine(internal_transaction_t* tx,wallet_id_t wallet_id,wallet_id_t next_wallet_id){
    auto shard_index = capi.get_my_shard<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP);
    auto shard = capi.get_shard_members<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP,shard_index);
    std::sort(shard.begin(),shard.end());
    
    bool chain = shard[wallet_id % shard.size()] == node_id;

    // check where the next wallet goes, but only of the associated optimization is enabled
    bool same_shard = false;
    uint32_t subgroup_type_index,subgroup_index,next_shard;
    std::string wallet_key = CBDC_BUILD_TRANSFER_KEY(next_wallet_id);
    std::tie(subgroup_type_index,subgroup_index,next_shard) = capi.key_to_shard(wallet_key);
    if(udl->config.enable_cross_thread_communication){
        same_shard = next_shard == shard_index;
    }

    return std::make_tuple(chain,same_shard,next_shard);
}

bool CascadeCBDC::CBDCThread::is_my_persistence(uint64_t factor){
    auto shard_index = capi.get_my_shard<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP);
    auto shard = capi.get_shard_members<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_SUBGROUP,shard_index);
    std::sort(shard.begin(),shard.end());
    return shard[factor % shard.size()] == node_id;
}

void CascadeCBDC::CBDCThread::send_tx_forward(internal_transaction_t* tx,wallet_id_t wallet_id){
    auto request = tx->request;
    auto& txid = std::get<0>(*request);
    auto& wallets = std::get<3>(*request);

    // next wallet
    auto it = std::find(wallets.begin(),wallets.end(),wallet_id);
    if(it == wallets.end()) return; // this should not happen
    auto next_wallet_id = *std::next(it);

    auto mine = is_mine(tx,wallet_id,next_wallet_id);

    if(std::get<1>(mine)){ // send directly to the correspoding thread
        TimestampLogger::log(CBDC_TAG_UDL_FORWARD_START,node_id,txid,wallet_id);

        // send to corresponding thread
        TimestampLogger::log(CBDC_TAG_UDL_HANDLER_QUEUING,node_id,txid,next_wallet_id);
        uint64_t to_thread = next_wallet_id % udl->config.num_threads;
        queued_operation_t* queued_op = new queued_operation_t(operation_type_t::FORWARD,next_wallet_id,tx);
        udl->threads[to_thread].push_operation(queued_op);
        
        TimestampLogger::log(CBDC_TAG_UDL_FORWARD_END,node_id,txid,wallet_id);
        return;
    }
     
    if(!std::get<0>(mine)){ // this is not the node responsible for forwarding
        return;
    }

    // this node is responsible for chaining the tx, proceed

    // if using the chaining thread
    if(udl->config.enable_chaining_thread){
        queued_chain_t queued_chain(operation_type_t::FORWARD,next_wallet_id,request);
        
        TimestampLogger::log(CBDC_TAG_UDL_FORWARD_START,node_id,txid,wallet_id);
        udl->chain_thread->push_chain(queued_chain,std::get<2>(mine));
        TimestampLogger::log(CBDC_TAG_UDL_FORWARD_END,node_id,txid,wallet_id);
        return;
    }

    // no chaining thread: we need to chain here

    // build the object
    ObjectWithStringKey obj;
    obj.key = CBDC_BUILD_FORWARD_KEY(next_wallet_id);
    obj.blob = Blob([&request](uint8_t* buffer,const std::size_t size){
            return mutils::to_bytes(*request, buffer);
        },mutils::bytes_size(*request));

    // put the object
    TimestampLogger::log(CBDC_TAG_UDL_FORWARD_START,node_id,txid,wallet_id);
    capi.put_and_forget(obj,true);
    TimestampLogger::log(CBDC_TAG_UDL_FORWARD_END,node_id,txid,wallet_id);
}

void CascadeCBDC::CBDCThread::send_status_backward(internal_transaction_t* tx,wallet_id_t wallet_id){
    // this node is responsible for chaining the tx, proceed
    auto request = tx->request;
    auto& txid = std::get<0>(*request);
    auto& wallets = std::get<3>(*request);

    // next wallet
    auto it = std::find(wallets.begin(),wallets.end(),wallet_id);
    if(it == wallets.end()) return; // this should not happen
    auto prev_wallet_id = *std::prev(it);

    auto mine = is_mine(tx,wallet_id,prev_wallet_id);
    
    if(std::get<1>(mine)){ // send directly to the correspoding thread
        TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_START,node_id,txid,wallet_id);
        
        // send to corresponding thread
        TimestampLogger::log(CBDC_TAG_UDL_HANDLER_QUEUING,node_id,txid,prev_wallet_id);
        uint64_t to_thread = prev_wallet_id % udl->config.num_threads;
        auto operation = tx->status == transaction_status_t::COMMIT ? operation_type_t::COMMIT : operation_type_t::ABORT;
        queued_operation_t* queued_op = new queued_operation_t(operation,prev_wallet_id,tx);
        udl->threads[to_thread].push_operation(queued_op);
        
        TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_END,node_id,txid,wallet_id);
        return;
    }
    
    if(!std::get<0>(mine)){ // this is not the node responsible for forwarding
        return;
    }
    
    // this node is responsible for chaining the tx, proceed

    // if using the chaining thread
    if(udl->config.enable_chaining_thread){
        auto operation = tx->status == transaction_status_t::COMMIT ? operation_type_t::COMMIT : operation_type_t::ABORT;
        queued_chain_t queued_chain(operation,prev_wallet_id,request);
        
        TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_START,node_id,txid,wallet_id);
        udl->chain_thread->push_chain(queued_chain,std::get<2>(mine));
        TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_END,node_id,txid,wallet_id);
        return;
    }

    // no chaining thread: we need to chain here

    // build the object
    ObjectWithStringKey obj;
    if(tx->status == transaction_status_t::COMMIT){
        obj.key = CBDC_BUILD_COMMIT_KEY(prev_wallet_id);
    } else if(tx->status == transaction_status_t::ABORT){
        obj.key = CBDC_BUILD_ABORT_KEY(prev_wallet_id);
    } else { 
        // this should not happen
        return;
    }

    cbdc_request_t dummy_request(txid,{},{},{});
    obj.blob = Blob([&dummy_request](uint8_t* buffer,const std::size_t size){
            return mutils::to_bytes(dummy_request, buffer);
        },mutils::bytes_size(dummy_request));

    // put the object
    TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_START,node_id,txid,wallet_id);
    capi.put_and_forget(obj,true);
    TimestampLogger::log(CBDC_TAG_UDL_BACKWARD_END,node_id,txid,wallet_id);
}

// wallet operations

void CascadeCBDC::CBDCThread::fetch_wallet(wallet_id_t wallet_id){
    ServiceClientAPI& capi = ServiceClientAPI::get_service_client();
    const std::string& key = CBDC_BUILD_WALLET_KEY(wallet_id);

    auto res = capi.get(key,CURRENT_VERSION,false);
    for (auto& reply_future : res.get()){
        auto& obj = reply_future.second.get();

        if(obj.version != INVALID_VERSION){
            wallet_cache[wallet_id] = std::move(*mutils::from_bytes<wallet_t>(nullptr,obj.blob.bytes));
            //wallet_cache[wallet_id] = *reinterpret_cast<const wallet_t *>(obj.blob.bytes);
        } else {
            // create a wallet with 0 coins
            wallet_cache[wallet_id] = 0;
        }
    }
}

coin_value_t CascadeCBDC::CBDCThread::add_to_wallet(wallet_t &wallet,coin_value_t value){
    wallet += value;
    return CBDC_COMPUTE_WALLET_BALANCE(wallet);
}

coin_value_t CascadeCBDC::CBDCThread::remove_from_wallet(wallet_t &wallet,coin_value_t value){
    if(value > wallet){
        // this should never happen!
        wallet = 0;
    } else {
        wallet -= value;
    }
    return CBDC_COMPUTE_WALLET_BALANCE(wallet);
}

void CascadeCBDC::CBDCThread::persist_wallet(wallet_id_t wallet_id,internal_transaction_t* tx){
    auto& wallet = wallet_cache[wallet_id];
    auto request = tx->request;
    auto& txid = std::get<0>(*request);

    // check if this node is responsible for this persistence
    if(!is_my_persistence(wallet_id)){
        return;
    }

    // if using the wallet persistence thread
    if(udl->config.enable_wallet_persistence_thread){
        queued_wallet_t queued_wallet(wallet_id,wallet,txid);
        
        TimestampLogger::log(CBDC_TAG_UDL_WALLET_PERSIST_START,node_id,txid,wallet_id);
        udl->wallet_thread->push_wallet(queued_wallet);
        TimestampLogger::log(CBDC_TAG_UDL_WALLET_PERSIST_END,node_id,txid,wallet_id);
        return;
    }

    // no wallet persistence thread: we need to put the wallet here

    // build the object
    ObjectWithStringKey obj;
    obj.key = CBDC_BUILD_WALLET_KEY(wallet_id);
    obj.message_id = txid;
    obj.blob = Blob([&wallet](uint8_t* buffer,const std::size_t size){
            return mutils::to_bytes(wallet, buffer);
        },mutils::bytes_size(wallet));

    // put the object
    TimestampLogger::log(CBDC_TAG_UDL_WALLET_PERSIST_START,node_id,txid,wallet_id);
    capi.put_and_forget(obj);
    TimestampLogger::log(CBDC_TAG_UDL_WALLET_PERSIST_END,node_id,txid,wallet_id);
}

// transaction persistence: happens after the first wallet commits or aborts
void CascadeCBDC::CBDCThread::persist_transaction(internal_transaction_t* tx){
    auto request = tx->request;
    auto& txid = std::get<0>(*request);
    
    // check if this node is responsible for this persistence
    if(!is_my_persistence(txid)){
        return;
    }
    
    std::string key = CBDC_BUILD_TRANSACTION_KEY(txid);
    uint32_t subgroup_type_index,subgroup_index,shard_index;
    std::tie(subgroup_type_index,subgroup_index,shard_index) = capi.key_to_shard(key);

    // if using the tx persistence thread
    if(udl->config.enable_tx_persistence_thread){
        TimestampLogger::log(CBDC_TAG_UDL_TX_PERSIST_START,node_id,txid,shard_index);
        udl->tx_thread->push_tx(tx,shard_index);
        TimestampLogger::log(CBDC_TAG_UDL_TX_PERSIST_END,node_id,txid,shard_index);
        return;
    }

    // no tx persistence thread: we need to put the tx here
    
    transaction_t persisted_tx(*request,tx->status);
    ObjectWithStringKey obj;
    obj.key = key;
    obj.message_id = txid;
    obj.blob = Blob([&persisted_tx](uint8_t* buffer,const std::size_t size){
            return mutils::to_bytes(persisted_tx, buffer);
        },mutils::bytes_size(persisted_tx));
 
    TimestampLogger::log(CBDC_TAG_UDL_TX_PERSIST_START,node_id,txid,shard_index);
    capi.put_and_forget<CBDC_OBJECT_POOL_TYPE>(obj,subgroup_index,shard_index);
    TimestampLogger::log(CBDC_TAG_UDL_TX_PERSIST_END,node_id,txid,shard_index);
}

// wallet persistence thread methods

CascadeCBDC::WalletPersistenceThread::WalletPersistenceThread(CascadeCBDC* udl){
    this->udl = udl;
    node_id = capi.get_my_id();
}

void CascadeCBDC::WalletPersistenceThread::push_wallet(queued_wallet_t &queued_wallet){
    std::unique_lock<std::mutex> lock(thread_mtx);
    wallet_queue.push(queued_wallet);
    thread_signal.notify_all();
}

void CascadeCBDC::WalletPersistenceThread::signal_stop(){
    std::unique_lock<std::mutex> lock(thread_mtx);
    running = false;
    thread_signal.notify_all();
}

void CascadeCBDC::WalletPersistenceThread::main_loop(){
    if(!running) return;
   
    // thread main loop 
    queued_wallet_t to_persist[udl->config.wallet_persistence_batch_max_size];
    auto wait_start = std::chrono::steady_clock::now();
    auto batch_time = std::chrono::microseconds(udl->config.wallet_persistence_batch_time_us);
    while(true){
        std::unique_lock<std::mutex> lock(thread_mtx);
        if(wallet_queue.empty()){
            thread_signal.wait_for(lock,batch_time);
        }

        if(!running) break;

        uint64_t persist_count = 0;
        uint64_t queued_count = wallet_queue.size();
        auto now = std::chrono::steady_clock::now();

        if((queued_count >= udl->config.wallet_persistence_batch_min_size) || ((now-wait_start) >= batch_time)){
            persist_count = std::min(queued_count,udl->config.wallet_persistence_batch_max_size);
            wait_start = now;
        
            // copy out wallets
            for(uint64_t i=0;i<persist_count;i++){
                to_persist[i] = wallet_queue.front();
                wallet_queue.pop();
            }
        }
        
        lock.unlock();

        // now we are outside the locked region (i.e the cbdc protocol can continue): build objects and call put_objects
        if(persist_count > 0){
            std::vector<ObjectWithStringKey> objects;
            objects.reserve(persist_count);

            for(uint64_t i=0;i<persist_count;i++){
                auto& queued_wallet = to_persist[i];
                auto& wallet_id = std::get<0>(queued_wallet);
                auto& wallet = std::get<1>(queued_wallet);
                auto& txid = std::get<2>(queued_wallet);

                std::size_t sz = mutils::bytes_size(wallet);
                uint8_t* buffer = new uint8_t[sz];
                mutils::to_bytes(wallet, buffer);

                objects.emplace_back(CBDC_BUILD_WALLET_KEY(wallet_id),Blob(buffer,sz));
                objects[i].message_id = txid;
            }
            
            TimestampLogger::log(CBDC_TAG_UDL_WALLET_BATCHING,node_id,objects.size(),0);
            capi.put_objects_and_forget(objects);
        }
    }
}

// chaining thread methods

CascadeCBDC::ChainingThread::ChainingThread(CascadeCBDC* udl){
    this->udl = udl;
    node_id = capi.get_my_id();
}

void CascadeCBDC::ChainingThread::push_chain(queued_chain_t &queued_chain,uint32_t next_shard){
    std::unique_lock<std::mutex> lock(thread_mtx);
    chain_queues[next_shard].push(queued_chain);
    thread_signal.notify_all();
}

void CascadeCBDC::ChainingThread::signal_stop(){
    std::unique_lock<std::mutex> lock(thread_mtx);
    running = false;
    thread_signal.notify_all();
}

void CascadeCBDC::ChainingThread::main_loop(){
    if(!running) return;
   
    // thread main loop 
    std::unordered_map<uint32_t,queued_chain_t*> to_persist;
    std::unordered_map<uint32_t,std::chrono::steady_clock::time_point> wait_time;
    auto batch_time = std::chrono::microseconds(udl->config.chaining_batch_time_us);
    while(true){
        std::unique_lock<std::mutex> lock(thread_mtx);
        bool empty = true;
        for(auto& item : chain_queues){
            empty = empty && item.second.empty();
        }

        if(empty){
            thread_signal.wait_for(lock,batch_time);
        }

        if(!running) break;

        std::unordered_map<uint32_t,uint64_t> persist_count;
        auto now = std::chrono::steady_clock::now();
        for(auto& item : chain_queues){
            auto& shard = item.first;
            auto& queue = item.second;

            if(to_persist.count(shard) == 0){
                to_persist[shard] = new queued_chain_t[udl->config.chaining_batch_max_size];
                wait_time[shard] = now;
            }
        
            uint64_t queued_count = queue.size();
            if((queued_count >= udl->config.chaining_batch_min_size) || ((now-wait_time[shard]) >= batch_time)){
                persist_count[shard] = std::min(queued_count,udl->config.chaining_batch_max_size);
                wait_time[shard] = now;
            
                // copy out wallets
                for(uint64_t i=0;i<persist_count[shard];i++){
                    to_persist[shard][i] = queue.front();
                    queue.pop();
                }
            }
        }
        
        lock.unlock();
        
        // now we are outside the locked region (i.e the cbdc protocol can continue): build objects and call put_objects
        for(auto& item : persist_count){
            auto& shard = item.first;
            auto count = item.second;

            if(count == 0){
                continue;
            }

            auto chains = to_persist[shard];

            std::vector<ObjectWithStringKey> objects;
            objects.reserve(count);

            for(uint64_t i=0;i<count;i++){
                auto& queued_chain = chains[i];
                auto& operation = std::get<0>(queued_chain);
                auto& wallet_id = std::get<1>(queued_chain);
                auto request = std::get<2>(queued_chain);
                auto& txid = std::get<0>(*request);

                if(operation == operation_type_t::FORWARD){ // forward
                    std::size_t sz = mutils::bytes_size(*request);
                    uint8_t* buffer = new uint8_t[sz];
                    mutils::to_bytes(*request, buffer);
                    objects.emplace_back(CBDC_BUILD_FORWARD_KEY(wallet_id),Blob(buffer,sz));
                } else {                    
                    cbdc_request_t dummy_request(txid,{},{},{});
                    std::size_t sz = mutils::bytes_size(dummy_request);
                    uint8_t* buffer = new uint8_t[sz];
                    mutils::to_bytes(dummy_request, buffer);
                    
                    if(operation == operation_type_t::COMMIT){ // commit
                        objects.emplace_back(CBDC_BUILD_COMMIT_KEY(wallet_id),Blob(buffer,sz));
                    } else { // abort
                        objects.emplace_back(CBDC_BUILD_ABORT_KEY(wallet_id),Blob(buffer,sz));
                    }
                }

                objects[i].message_id = txid;
            }

            TimestampLogger::log(CBDC_TAG_UDL_CHAIN_BATCHING,node_id,objects.size(),shard);
            capi.put_objects_and_forget<CBDC_OBJECT_POOL_TYPE>(objects,CBDC_OBJECT_POOL_SUBGROUP,shard,true);
        }
    }
}

// tx persistence thread methods
CascadeCBDC::TXPersistenceThread::TXPersistenceThread(CascadeCBDC* udl){
    this->udl = udl;
    node_id = capi.get_my_id();
}

void CascadeCBDC::TXPersistenceThread::push_tx(internal_transaction_t* queued_tx,uint32_t shard){
    std::unique_lock<std::mutex> lock(thread_mtx);
    tx_queues[shard].push(queued_tx);
    thread_signal.notify_all();
}

void CascadeCBDC::TXPersistenceThread::signal_stop(){
    std::unique_lock<std::mutex> lock(thread_mtx);
    running = false;
    thread_signal.notify_all();
}

void CascadeCBDC::TXPersistenceThread::main_loop(){
    if(!running) return;
   
    // thread main loop 
    std::unordered_map<uint32_t,internal_transaction_t**> to_persist;
    std::unordered_map<uint32_t,std::chrono::steady_clock::time_point> wait_time;
    auto batch_time = std::chrono::microseconds(udl->config.tx_persistence_batch_time_us);
    while(true){
        std::unique_lock<std::mutex> lock(thread_mtx);
        bool empty = true;
        for(auto& item : tx_queues){
            empty = empty && item.second.empty();
        }

        if(empty){
            thread_signal.wait_for(lock,batch_time);
        }

        if(!running) break;

        std::unordered_map<uint32_t,uint64_t> persist_count;
        auto now = std::chrono::steady_clock::now();
        for(auto& item : tx_queues){
            auto& shard = item.first;
            auto& queue = item.second;

            if(to_persist.count(shard) == 0){
                to_persist[shard] = new internal_transaction_t*[udl->config.tx_persistence_batch_max_size];
                wait_time[shard] = now;
            }
        
            uint64_t queued_count = queue.size();
            if((queued_count >= udl->config.tx_persistence_batch_min_size) || ((now-wait_time[shard]) >= batch_time)){
                persist_count[shard] = std::min(queued_count,udl->config.tx_persistence_batch_max_size);
                wait_time[shard] = now;
            
                // copy out wallets
                for(uint64_t i=0;i<persist_count[shard];i++){
                    to_persist[shard][i] = queue.front();
                    queue.pop();
                }
            }
        }
        
        lock.unlock();
        
        // now we are outside the locked region (i.e the cbdc protocol can continue): build objects and call put_objects
        for(auto& item : persist_count){
            auto& shard = item.first;
            auto count = item.second;

            if(count == 0){
                continue;
            }

            auto txs = to_persist[shard];

            std::vector<ObjectWithStringKey> objects;
            objects.reserve(count);

            for(uint64_t i=0;i<count;i++){
                auto queued_tx = txs[i];
                auto request = queued_tx->request;
                auto& txid = std::get<0>(*request);
                transaction_t persisted_tx(*request,queued_tx->status);
   
                std::size_t sz = mutils::bytes_size(persisted_tx);
                uint8_t* buffer = new uint8_t[sz];
                mutils::to_bytes(persisted_tx, buffer);
                objects.emplace_back(CBDC_BUILD_TRANSACTION_KEY(txid),Blob(buffer,sz));
                objects[i].message_id = txid;
            }

            TimestampLogger::log(CBDC_TAG_UDL_TX_BATCHING,node_id,objects.size(),shard);
            capi.put_objects_and_forget<CBDC_OBJECT_POOL_TYPE>(objects,CBDC_OBJECT_POOL_SUBGROUP,shard);
        }
    }
}

} // namespace cascade
} // namespace derecho

