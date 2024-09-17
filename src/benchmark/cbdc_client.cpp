
#include "cbdc_client.hpp"

CascadeCBDC::CascadeCBDC(){
}

CascadeCBDC::~CascadeCBDC(){
    client_thread->signal_stop();
    client_thread->join();
}

void CascadeCBDC::setup(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us){
    // create object pools
    // check if already exists
    auto opm = capi.find_object_pool(CBDC_OBJECT_POOL_PREFIX);
    if (!opm.is_valid() || opm.is_null()){
        auto res = capi.template create_object_pool<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_PREFIX,CBDC_OBJECT_POOL_SUBGROUP,HASH,{},CBDC_OBJECT_POOL_REGEX);
        for (auto& reply_future:res.get()) {
            auto reply = reply_future.second.get();
        }
    }

    // send init request
    ObjectWithStringKey init;
    init.key = CBDC_REQUEST_INIT_KEY;
    auto init_res = capi.put(init,true);
    for (auto& reply_future : init_res.get()){
        reply_future.second.get();
    }

    // retrieve service configuration
    bool retrieved = false;
    std::string config_key = CBDC_CONFIG_KEY;
    while(!retrieved){ // poll the configuration object
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        auto res = capi.get(config_key,CURRENT_VERSION,false);
        for (auto& reply_future : res.get()){
            auto& obj = reply_future.second.get();

            if(obj.version != INVALID_VERSION){
                config = *mutils::from_bytes<cascade_cbdc_config_t>(nullptr,obj.blob.bytes);
                retrieved = true;
            }
        }
    }

    // log deployment
    auto shards = capi.get_subgroup_members(CBDC_PREFIX);
    for(uint64_t i=0;i<shards.size();i++){
        for(auto node : shards[i]){
            TimestampLogger::log(CBDC_TAG_CLIENT_DEPLOYMENT_INFO,node,i,0);
        }
    }

    // start client thread
    client_thread = new ClientThread(batch_min_size,batch_max_size,batch_time_us);
    client_thread->start();
}

transaction_id_t CascadeCBDC::next_transaction_id(){
    std::unique_lock<std::mutex> lock(txid_mtx);
    transaction_id_t txid = (my_id << 48) | tx_count;
    tx_count++;
    return txid;
}

transaction_id_t CascadeCBDC::mint(wallet_id_t wallet_id,coin_value_t value){
    transaction_id_t txid = next_transaction_id();
    auto shard = std::get<2>(capi.key_to_shard(CBDC_BUILD_MINT_KEY(wallet_id)));
    cbdc_request_t *request = new cbdc_request_t(txid,{},{{wallet_id,value}},{wallet_id});
    queued_request_t queued_request(thread_request_t::MINT,request);
    client_thread->push_request(queued_request,shard);
    
    return txid;
}

transaction_id_t CascadeCBDC::transfer(const std::unordered_map<wallet_id_t,coin_value_t>& senders,const std::unordered_map<wallet_id_t,coin_value_t>& receivers){
    transaction_id_t txid = next_transaction_id();
    TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_START,my_id,txid,0);
   
    std::vector<wallet_id_t> sorted_wallets; 
    coin_value_t value_in = 0;
    coin_value_t value_out = 0;
    
    for(auto& item : senders){
        sorted_wallets.push_back(item.first);
        value_in += item.second;
    }

    if(!config.enable_source_only_conflicts){
        // add destinations before ordering if this optimization is disabled
        for(auto& item : receivers){
            sorted_wallets.push_back(item.first);
            value_out += item.second;
        }
    }

    std::sort(sorted_wallets.begin(),sorted_wallets.end(),[&](const wallet_id_t &a, const wallet_id_t &b){
                uint32_t subgroup_type_index,subgroup_index,shard_index;

                std::string a_key = CBDC_BUILD_TRANSFER_KEY(a);
                std::tie(subgroup_type_index,subgroup_index,shard_index) = capi.key_to_shard(a_key);
                uint64_t a_index = shard_index * config.num_threads + (a % config.num_threads);
                
                std::string b_key = CBDC_BUILD_TRANSFER_KEY(b);
                std::tie(subgroup_type_index,subgroup_index,shard_index) = capi.key_to_shard(b_key);
                uint64_t b_index = shard_index * config.num_threads + (b % config.num_threads);

                return a_index > b_index;
            });

    if(config.enable_source_only_conflicts){
        // add destinations after ordering if this optimization is enabled
        for(auto& item : receivers){
            sorted_wallets.push_back(item.first);
            value_out += item.second;
        }
    }

    // validate if value_in == value_out
    if(value_in != value_out){
        std::cout << "ERROR: value_in(" << value_in << ") != value_out(" << value_out << ") in TX " << txid << std::endl;
        return txid;
    }

    if(sorted_wallets.empty()){
        std::cout << "ERROR: empty transfer for TX " << txid << std::endl;
        return txid;
    }
    
    wallet_id_t first_wallet = sorted_wallets[0];
    auto first_shard = std::get<2>(capi.key_to_shard(CBDC_BUILD_TRANSFER_KEY(first_wallet)));
    cbdc_request_t *request = new cbdc_request_t(txid,senders,receivers,sorted_wallets);
    queued_request_t queued_request(thread_request_t::TRANSFER,request);
    
    TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_QUEUE,my_id,txid,first_wallet);
    client_thread->push_request(queued_request,first_shard);
    
    return txid;
}

transaction_id_t CascadeCBDC::redeem(wallet_id_t wallet_id,coin_value_t value){
    transaction_id_t txid = next_transaction_id();
    auto shard = std::get<2>(capi.key_to_shard(CBDC_BUILD_REDEEM_KEY(wallet_id)));
    cbdc_request_t *request = new cbdc_request_t(txid,{{wallet_id,value}},{},{wallet_id});
    queued_request_t queued_request(thread_request_t::REDEEM,request);
    client_thread->push_request(queued_request,shard);
    
    return txid;
}

std::string CascadeCBDC::status_to_string(const transaction_status_t status){
    switch(status){
        case transaction_status_t::PENDING:
            return "pending";
            break;
        case transaction_status_t::RUNNING:
            return "running";
            break;
        case transaction_status_t::COMMIT:
            return "commit";
            break;
        case transaction_status_t::ABORT:
            return "abort";
            break;
        default:
            break;
    }
    return "unknown";
}

wallet_t CascadeCBDC::get_wallet(wallet_id_t wallet_id){
    const std::string& key = CBDC_BUILD_WALLET_KEY(wallet_id);
    auto res = capi.get(key,CURRENT_VERSION,false);
    for (auto& reply_future : res.get()){
        auto& obj = reply_future.second.get();

        if(obj.version != INVALID_VERSION){
            return *mutils::from_bytes<wallet_t>(nullptr,obj.blob.bytes);
        }
    }
  
    return 0;
}

transaction_status_t CascadeCBDC::get_status(const transaction_id_t& txid){
    const std::string& key = CBDC_BUILD_TRANSACTION_KEY(txid);
    auto res = capi.get(key,CURRENT_VERSION,false);
    for (auto& reply_future : res.get()){
        auto& obj = reply_future.second.get();

        if(obj.version != INVALID_VERSION){
            TimestampLogger::log(CBDC_TAG_CLIENT_STATUS,my_id,txid,obj.version);
            return std::get<1>(*mutils::from_bytes<transaction_t>(nullptr,obj.blob.bytes));
        }
    }
  
    return transaction_status_t::UNKNOWN;
}

void CascadeCBDC::reset(){
    ObjectWithStringKey obj;
    obj.key = CBDC_REQUEST_RESET_KEY;
    
    std::vector<std::vector<uint32_t>> shards = capi.get_subgroup_members(CBDC_PREFIX);
    for(uint32_t shard_index = 0; shard_index < shards.size(); shard_index++){
        capi.put_and_forget<CBDC_OBJECT_POOL_TYPE>(obj,CBDC_OBJECT_POOL_SUBGROUP,shard_index,true);
    }
}

void CascadeCBDC::write_logs(const std::string local_log,const std::string remote_logs){
    TimestampLogger::flush(local_log);

    if(remote_logs == "-"){
        return;
    }

    ObjectWithStringKey obj;
    obj.key = CBDC_REQUEST_LOG_KEY;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(remote_logs.c_str()),remote_logs.length()+1);
            
    std::vector<std::vector<uint32_t>> shards = capi.get_subgroup_members(CBDC_PREFIX);
    for(uint32_t shard_index = 0; shard_index < shards.size(); shard_index++){
        capi.put_and_forget<CBDC_OBJECT_POOL_TYPE>(obj,CBDC_OBJECT_POOL_SUBGROUP,shard_index,true);
    }
}

// client thread methods

CascadeCBDC::ClientThread::ClientThread(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us){
    this->batch_min_size = batch_min_size;
    this->batch_max_size = batch_max_size;
    this->batch_time_us = batch_time_us;
}

void CascadeCBDC::ClientThread::push_request(queued_request_t &queued_request,uint32_t shard){
    std::unique_lock<std::mutex> lock(thread_mtx);
    request_queues[shard].push(queued_request);
    thread_signal.notify_all();
}

void CascadeCBDC::ClientThread::signal_stop(){
    std::unique_lock<std::mutex> lock(thread_mtx);
    running = false;
    thread_signal.notify_all();
}

void CascadeCBDC::ClientThread::main_loop(){
    if(!running) return;
   
    // thread main loop 
    std::unordered_map<uint32_t,queued_request_t*> to_persist;
    std::unordered_map<uint32_t,std::chrono::steady_clock::time_point> wait_time;
    auto batch_time = std::chrono::microseconds(batch_time_us);
    while(true){
        std::unique_lock<std::mutex> lock(thread_mtx);
        bool empty = true;
        for(auto& item : request_queues){
            empty = empty && item.second.empty();
        }

        if(empty){
            thread_signal.wait_for(lock,batch_time);
        }

        if(!running) break;

        std::unordered_map<uint32_t,uint64_t> persist_count;
        auto now = std::chrono::steady_clock::now();
        for(auto& item : request_queues){
            auto& shard = item.first;
            auto& queue = item.second;

            if(to_persist.count(shard) == 0){
                to_persist[shard] = new queued_request_t[batch_max_size];
                wait_time[shard] = now;
            }
        
            uint64_t queued_count = queue.size();
            if((queued_count >= batch_min_size) || ((now-wait_time[shard]) >= batch_time)){
                persist_count[shard] = std::min(queued_count,batch_max_size);
                wait_time[shard] = now;
            
                // copy out wallets
                for(uint64_t i=0;i<persist_count[shard];i++){
                    to_persist[shard][i] = queue.front();
                    queue.pop();
                }
            }
        }
        
        lock.unlock();
        
        // now we are outside the locked region (i.e the client can continue adding requests to the queues): build objects and call put_objects
        for(auto& item : persist_count){
            auto& shard = item.first;
            auto count = item.second;

            if(count == 0){
                continue;
            }

            auto requests = to_persist[shard];

            std::vector<ObjectWithStringKey> objects;
            objects.reserve(count);

            for(uint64_t i=0;i<count;i++){
                auto& queued_request = requests[i];
                auto& operation = queued_request.first;
                auto request = queued_request.second;
                auto& txid = std::get<0>(*request);
                auto& sorted_wallets = std::get<3>(*request);
                auto first_wallet = sorted_wallets[0];

                std::size_t sz = mutils::bytes_size(*request);
                uint8_t* buffer = new uint8_t[sz];
                mutils::to_bytes(*request, buffer);

                std::string key;
                switch(operation){
                    case thread_request_t::MINT:
                        key = CBDC_BUILD_MINT_KEY(first_wallet);
                        break;
                    case thread_request_t::TRANSFER:
                        key = CBDC_BUILD_TRANSFER_KEY(first_wallet);
                        break;
                    case thread_request_t::REDEEM:
                        key = CBDC_BUILD_REDEEM_KEY(first_wallet);
                        break;
                }
                    
                objects.emplace_back(key,Blob(buffer,sz));
                objects[i].message_id = txid;
                delete request;
            }
    
            for(auto& obj : objects){
                TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_SENDING,node_id,obj.message_id,0);
            }

            TimestampLogger::log(CBDC_TAG_CLIENT_BATCHING,node_id,objects.size(),shard);
            capi.put_objects_and_forget<CBDC_OBJECT_POOL_TYPE>(objects,CBDC_OBJECT_POOL_SUBGROUP,shard,true);
            
            for(auto& obj : objects){
                TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_SENT,node_id,obj.message_id,0);
            }
        }
    }
}

