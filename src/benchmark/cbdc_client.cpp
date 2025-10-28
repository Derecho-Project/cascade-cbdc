
#include "cbdc_client.hpp"

CascadeCBDC::CascadeCBDC(){
}

CascadeCBDC::~CascadeCBDC(){
    if (client_thread) {
        client_thread->signal_stop();
        client_thread->join();
        delete client_thread;
        client_thread = nullptr;
    }
}

void CascadeCBDC::setup(uint64_t batch_min_size,uint64_t batch_max_size,uint64_t batch_time_us){
    // create object pools
    // check if already exists
    load_service_key("service_public_key.pem");
    auto opm = capi.find_object_pool(CBDC_OBJECT_POOL_PREFIX);
    if (!opm.is_valid() || opm.is_null()){
        auto res = capi.template create_object_pool<CBDC_OBJECT_POOL_TYPE>(CBDC_OBJECT_POOL_PREFIX,CBDC_OBJECT_POOL_SUBGROUP,HASH,{},CBDC_OBJECT_POOL_REGEX);
        for (auto& reply_future:res.get()) {
            auto reply = reply_future.second.get();
        }
    }

    auto lpm = capi.find_object_pool(CBDC_LOG_POOL_PREFIX);
    if (!lpm.is_valid() || lpm.is_null()){
        auto res = capi.template create_object_pool<CBDC_LOG_POOL_TYPE>(CBDC_LOG_POOL_PREFIX,CBDC_LOG_POOL_SUBGROUP,HASH,{},CBDC_LOG_POOL_REGEX);
        for (auto& reply_future:res.get()) {
            auto reply = reply_future.second.get();
        }
    }
    // capi.register_signature_notification_handler([&](const Blob& message) { signature_notification_handler(message); },
    //                                                        CBDC_LOG_POOL_PREFIX);
    std::string sig_pool = std::string(CBDC_LOG_POOL_PREFIX);
    std::cout << sig_pool << "\n";
    // if (sig_pool.empty() || sig_pool.back() != '/') sig_pool.push_back('/');

    std::cout << "[setup] Binding handler on pool: " << sig_pool << "\n";

    // Bind handler to the signature pool
    capi.register_signature_notification_handler(
        [this](const Blob& msg){ this->signature_notification_handler(msg); },
        sig_pool
    );

    // One pool-wide subscription; no per-key subscriptions:
    // auto sub_all = capi.subscribe_signature_notifications(sig_pool);
    // sub_all.get();
    // std::cout << "[subscribe-ok] pool-wide " << sig_pool << "\n";

    // send init request
    ObjectWithStringKey init;
    init.key = CBDC_REQUEST_INIT_KEY;
    auto init_res = capi.put(init,true);
    for (auto& reply_future : init_res.get()){
        reply_future.second.get();
    }

    {
        bool stable = false;
        persistent::version_t version = CURRENT_VERSION;
        std::string pool_prefix = CBDC_OBJECT_POOL_PREFIX;

        std::cout << "[DEBUG] Listing keys in pool: " << pool_prefix << std::endl;

        auto res_futures = capi.list_keys(version, stable, pool_prefix);

        for (size_t shard = 0; shard < res_futures.size(); ++shard) {
            const auto& query_result_ptr = res_futures[shard];
            if (!query_result_ptr) {
                std::cerr << "[WARN] Null QueryResult for shard " << shard << std::endl;
                continue;
            }

            try {
                 auto&& reply_map = query_result_ptr->get();  // non-copyable: must use as reference or rvalue

                for (auto& node_result_pair : reply_map) {
                    auto node_id = node_result_pair.first;
                    std::future<std::vector<std::string>>& future_vec = node_result_pair.second;

                    std::vector<std::string> keys = future_vec.get();  // unwrap future
                    std::cout << "[DEBUG] Shard " << shard << " Node " << node_id << " Keys:" << std::endl;
                    for (const auto& key : keys) {
                        std::cout << "  - " << key << std::endl;
                    }
                }            } catch (const std::exception& e) {
                std::cerr << "[ERROR] Failed to get keys for shard " << shard
                        << ": " << e.what() << std::endl;
            }
        }
    }
    // retrieve service configuration
    bool retrieved = false;
    std::string config_key = CBDC_CONFIG_KEY;
    while(!retrieved){ // poll the configuration object
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        auto res = capi.get(config_key,CURRENT_VERSION,false);
        for (auto& reply_future : res.get()){
            auto& obj = reply_future.second.get();

            if(obj.version != persistent::INVALID_VERSION){
                config = *mutils::from_bytes<cascade_cbdc_config_t>(nullptr,obj.blob.bytes);
                retrieved = true;
            }
        }
    }

    // log deployment
    auto shards = capi.get_subgroup_members(CBDC_OBJECT_POOL_PREFIX);
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
    // auto shard = std::get<2>(capi.key_to_shard(CBDC_BUILD_MINT_KEY(wallet_id)));
    cbdc_request_t::body request_body(txid,{},{{wallet_id,value}},{wallet_id});
    // cbdc_request_t *request = new cbdc_request_t(std::move(request_body), cbdc_request_t::hash_body(request_body));
    cbdc_request_t request(
        std::move(request_body), 
        cbdc_request_t::hash_body(request_body)
    );
    // queued_request_t queued_request(thread_request_t::MINT,request);
    // client_thread->push_request(queued_request,shard);
    bool success = put_with_signature(thread_request_t::MINT, request);

    if (success) {
        return txid;
    }
    else 
        return -1;
}

transaction_id_t CascadeCBDC::transfer(const std::unordered_map<wallet_id_t,coin_value_t>& senders,const std::unordered_map<wallet_id_t,coin_value_t>& receivers){
    std::cout << "Fail check 1\n";
    transaction_id_t txid = next_transaction_id();
    TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_START,my_id,txid,0);
   
    std::cout << "Fails check 2\n";
    std::vector<wallet_id_t> sorted_wallets; 
    coin_value_t value_in = 0;
    coin_value_t value_out = 0;
    
    std::cout << "Fails check 3\n";
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
    std::cout << "Fails check 4\n";

    std::sort(sorted_wallets.begin(),sorted_wallets.end(),[&](const wallet_id_t &a, const wallet_id_t &b){
                uint32_t subgroup_type_index,subgroup_index,shard_index;

                std::string a_key = std::string(CBDC_OBJECT_POOL_PREFIX) +  CBDC_BUILD_TRANSFER_KEY(a);
                std::tie(subgroup_type_index,subgroup_index,shard_index) = capi.key_to_shard(a_key);
                uint64_t a_index = shard_index * config.num_threads + (a % config.num_threads);
                
                std::string b_key = std::string(CBDC_OBJECT_POOL_PREFIX) + CBDC_BUILD_TRANSFER_KEY(b);
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
    std::cout << "check \n";

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
    auto first_shard = std::get<2>(capi.key_to_shard(CBDC_OBJECT_POOL_PREFIX + CBDC_BUILD_TRANSFER_KEY(first_wallet)));
    cbdc_request_t::body request_body(txid,senders,receivers,sorted_wallets);
    cbdc_request_t request(std::move(request_body), cbdc_request_t::hash_body(request_body));
    bool success = put_with_signature(thread_request_t::TRANSFER, request);
    queued_request_t queued_request(thread_request_t::TRANSFER, &request);
    
    if (success){
        TimestampLogger::log(CBDC_TAG_CLIENT_TRANSFER_QUEUE,my_id,txid,first_wallet);
        return txid;
    } else 
        return -1;
}

transaction_id_t CascadeCBDC::redeem(wallet_id_t wallet_id,coin_value_t value){
    transaction_id_t txid = next_transaction_id();
    auto shard = std::get<2>(capi.key_to_shard(CBDC_BUILD_REDEEM_KEY(wallet_id)));
    cbdc_request_t::body request_body(txid,{{wallet_id,value}},{},{wallet_id});
    cbdc_request_t request(std::move(request_body), cbdc_request_t::hash_body(request_body));
    queued_request_t queued_request(thread_request_t::REDEEM, &request);
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

        if(obj.version != persistent::INVALID_VERSION){
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

        if(obj.version != persistent::INVALID_VERSION){
            TimestampLogger::log(CBDC_TAG_CLIENT_STATUS,my_id,txid,obj.version);
            return std::get<1>(*mutils::from_bytes<transaction_t>(nullptr,obj.blob.bytes));
        }
    }
  
    return transaction_status_t::UNKNOWN;
}

void CascadeCBDC::reset(){
    ObjectWithStringKey obj;
    obj.key = CBDC_REQUEST_RESET_KEY;
    
    std::vector<std::vector<uint32_t>> shards = capi.get_subgroup_members(CBDC_OBJECT_POOL_PREFIX);
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
    // std::unordered_map<uint32_t,queued_request_t*> to_persist;
    std::unordered_map<uint32_t,std::vector<queued_request_t>> to_persist;
    std::unordered_map<uint32_t,std::chrono::steady_clock::time_point> wait_time;
    auto batch_time = std::chrono::microseconds(batch_time_us);
    // size_t curr_batch_size = 0;
    while(running){
        std::unique_lock<std::mutex> lock(thread_mtx);
        bool empty = true;
        for(auto& item : request_queues) {
            empty = empty && item.second.empty();
        }
        
        if(empty) {
            thread_signal.wait_for(lock, batch_time);
            if(!running) break;
        }

        std::unordered_map<uint32_t,uint64_t> persist_count;
        auto now = std::chrono::steady_clock::now();

        // Process one request per shard per iteration
        for(auto& item : request_queues) {
            auto& shard = item.first;
            auto& queue = item.second;

            if(to_persist.count(shard) == 0) {
                to_persist[shard] = std::vector<queued_request_t>();
                to_persist[shard].reserve(batch_max_size);
                wait_time[shard] = now;
            }
        
            // Process if we hit minimum batch size or timeout
            if(!queue.empty() && 
               (queue.size() >= batch_min_size || 
                (now - wait_time[shard]) >= batch_time)) {
                
                // Only take one request per iteration for fairness
                to_persist[shard].push_back(queue.front());
                persist_count[shard] = to_persist[shard].size();
                queue.pop();

                // Reset wait time if we've hit batch size
                if(persist_count[shard] >= batch_max_size) {
                    wait_time[shard] = now;
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

            auto& requests = to_persist[shard];

            std::vector<ObjectWithStringKey> objects;
            objects.reserve(count);

            for(uint64_t i=0;i<count;i++){
                auto& queued_request = requests[i];
                auto& operation = queued_request.first;
                auto request = queued_request.second;
                auto& txid = request->Body.txid;
                auto& sorted_wallets = request->Body.sorted_wallets;
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

            // Clear processed requests
            requests.clear();
        }
        // Reset for next batch
        persist_count.clear();
    }
}

bool CascadeCBDC::put_with_signature(thread_request_t op, cbdc_request_t& request) {
    wallet_id_t first_wallet = request.Body.sorted_wallets[0];
    std::string bare_key;
    switch(op) {
        case thread_request_t::MINT:     bare_key = CBDC_BUILD_MINT_KEY(first_wallet);    break;
        case thread_request_t::TRANSFER: bare_key = CBDC_BUILD_TRANSFER_KEY(first_wallet); break;
        case thread_request_t::REDEEM:   bare_key = CBDC_BUILD_REDEEM_KEY(first_wallet);   break;
    }

    const std::string data_key = std::string(CBDC_OBJECT_POOL_PREFIX) + bare_key;
    const std::string sig_key  = std::string(CBDC_LOG_POOL_PREFIX)    + bare_key;

    auto [sti, sgi, shard_index] = capi.key_to_shard(data_key);

    std::vector<uint8_t> buf(mutils::bytes_size(request));
    mutils::to_bytes(request, buf.data());

    ObjectWithStringKey obj;
    obj.key  = data_key;
    obj.blob = Blob(buf.data(), buf.size());

    if (subscribed_notification_keys.insert(sig_key).second) {
        if (op == thread_request_t::TRANSFER) {
            std::cout << sig_key<<"\n";
        }
        auto sub = capi.subscribe_signature_notifications(sig_key);
        // sub.get();
        std::cout << "[subscribe-ok] " << sig_key << "\n";
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));

    std::mutex cb_mx;
    std::condition_variable cb_cv;
    bool fired = false;
    persistent::version_t hash_object_version{};
    std::vector<uint8_t> server_signature, prev_signature;


    // auto put_res   = capi.put(obj);
    auto put_res   = capi.put<CBDC_OBJECT_POOL_TYPE>(obj, CBDC_OBJECT_POOL_SUBGROUP, shard_index, true); // <<< TYPED
    auto put_reply = put_res.get().begin()->second.get();

    obj.version                 = std::get<0>(put_reply);
    obj.timestamp_us            = std::get<1>(put_reply);
    obj.previous_version        = std::get<2>(put_reply);
    obj.previous_version_by_key = std::get<3>(put_reply);

    signature_notification_handler.register_callback(obj.version, 
                [&](persistent::version_t data_ver, persistent::version_t hash_ver,
                      const std::vector<uint8_t>& sig, persistent::version_t /*prev_signed_ver*/,
                      const std::vector<uint8_t>& prev_sig) {
        std::lock_guard<std::mutex> lk(cb_mx);
        hash_object_version = hash_ver;
        server_signature    = sig;
        prev_signature      = prev_sig;
        fired = true;
        cb_cv.notify_all();
    });

    {
        std::unique_lock<std::mutex> lk(cb_mx);
        if (!cb_cv.wait_for(lk, std::chrono::seconds(5), [&]{ return fired; })) {

            std::cerr << "[timeout] No signature notification for data ver "
                      << std::hex << obj.version << std::dec
                      << " key=" << sig_key << "\n";
            return false;
        }
    }

    auto hash_get_result = capi.get(sig_key, hash_object_version /* exact version */, /*stable=*/false);
    auto hashObject = hash_get_result.get().begin()->second.get();

    auto local_hash = compute_hash(obj);
    if (hashObject.blob.size != local_hash.size() ||
        memcmp(hashObject.blob.bytes, local_hash.data(), local_hash.size()) != 0) {
        std::cout << "Server hash != local hash\n";
        return false;
    }
    if (!verify_object_signature(hashObject, server_signature, prev_signature)) {
        std::cout << "Invalid server signature\n";
        return false;
    }
    std::cout << "Success! Signed receipt verified.\n";
    return true;
}

std::vector<uint8_t> CascadeCBDC::compute_hash(const ObjectWithStringKey& data_obj) {
    openssl::Hasher object_hasher(openssl::DigestAlgorithm::SHA256);
    object_hasher.init();
    // Note: get_hash_size() only works after init()
    std::vector<uint8_t> hash(object_hasher.get_hash_size());
    object_hasher.add_bytes(&data_obj.version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.timestamp_us, sizeof(uint64_t));
    object_hasher.add_bytes(&data_obj.previous_version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.previous_version_by_key, sizeof(persistent::version_t));
    object_hasher.add_bytes(data_obj.key.data(), data_obj.key.size());
    object_hasher.add_bytes(data_obj.blob.bytes, data_obj.blob.size);
    object_hasher.finalize(hash.data());
    return hash;
}

bool CascadeCBDC::verify_object_signature(const ObjectWithStringKey& hash,
                                          const std::vector<uint8_t>& signature,
                                          const std::vector<uint8_t>& previous_signature) {
    if(!service_verifier) {
        std::cout << "Service's public key has not been loaded. Cannot verify.\n";
        return false;
    }
    service_verifier->init();

    SignatureCascadeStoreWithStringKey::LogEntry hash_log_entry;
    hash_log_entry.objects.emplace(hash.get_key_ref(), hash);
    const std::size_t log_entry_size = mutils::bytes_size(hash_log_entry);

    std::vector<uint8_t> bytes_of_log_entry(log_entry_size);
    mutils::to_bytes(hash_log_entry, bytes_of_log_entry.data());
    service_verifier->add_bytes(bytes_of_log_entry.data(), bytes_of_log_entry.size());

    service_verifier->add_bytes(previous_signature.data(), previous_signature.size());
    return service_verifier->finalize(signature);
}

bool CascadeCBDC::load_service_key(const std::string& pem_path) {
    service_verifier = std::make_unique<openssl::Verifier>(openssl::EnvelopeKey::from_pem_public(pem_path),
                                                           openssl::DigestAlgorithm::SHA256);
    return true;
}
