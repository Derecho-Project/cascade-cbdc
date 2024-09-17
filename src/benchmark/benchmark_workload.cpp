
#include "benchmark_workload.hpp"

// helper function to shuffle a list: usually this is not recommended, but using a vector in the generate() method below incurs a much bigger overhead due to the frequent erase() calls. Thus we need a list, but we also need to shuffle it for the randomness.
static void shuffle_list(std::list<wallet_id_t>& list, std::mt19937& rng){
    // copy list to a vector
    std::vector<wallet_id_t> temp(list.begin(), list.end());

    // shuffle the vector
    std::shuffle(temp.begin(), temp.end(), rng);

    // copy shuffled vector back into the list
    std::copy(temp.begin(), temp.end(), list.begin());
}

void CBDCBenchmarkWorkload::generate(){
    if(generated){
        return;
    }

    // reset
    wallets.clear();
    transfers.clear();
    expected_balance.clear();
    expected_status.clear();

    // create wallets with initial balance
    for(wallet_id_t wid=wallet_start_id;wid<(wallet_start_id+num_wallets);wid++){
        wallets[wid] = wallet_initial_balance;
        expected_balance[wid] = wallet_initial_balance;
    }

    // generate list of all wallets that are going to be used (with repetitions), and shuffle it. 
    std::list<wallet_id_t> wallet_list;
    std::mt19937 rng(random_seed);
    
    for(wallet_id_t wid=wallet_start_id;wid<(wallet_start_id+num_wallets);wid++){
        for(wallet_id_t i=0;i<transfers_per_wallet;i++){
            wallet_list.push_back(wid);
        }
    }
    
    shuffle_list(wallet_list,rng);

    // generate random transfers
    uint64_t wallets_per_transfer = senders_per_transfer + receivers_per_transfer;
    auto next_wallet_it = wallet_list.begin();
    uint64_t transfer_idx = 0;
    while(wallet_list.size() >= wallets_per_transfer){
        benchmark_transfer_t transfer;
        std::unordered_map<wallet_id_t,bool> picked;
        bool finish = false;
        bool success = true;
        
        // pick senders
        auto started = next_wallet_it;
        coin_value_t value_in = 0;
        while(transfer.senders.size() < senders_per_transfer){
            // find the next valid wallet 
            wallet_id_t sender_id = *next_wallet_it;
            while(picked.count(sender_id) > 0){
                // circular increment
                next_wallet_it++;
                if(next_wallet_it == wallet_list.end()){
                    next_wallet_it = wallet_list.begin();
                }

                if(next_wallet_it == started){
                    finish = true;
                    break;
                }

                sender_id = *next_wallet_it;
            }

            if(finish){
                // impossible to form a new transfer
                break;
            }

            picked[sender_id] = true;
            transfer.senders[sender_id] = transfer_value;
            value_in += transfer_value;
            success = success && (expected_balance[sender_id] >= transfer_value);

            next_wallet_it = wallet_list.erase(next_wallet_it);
            if(next_wallet_it == wallet_list.end()){
                next_wallet_it = wallet_list.begin();
            }
        }

        if(finish){
            break;
        }

        // pick receivers
        started = next_wallet_it;
        coin_value_t value_out = 0;
        wallet_id_t last_receiver_id = 0;
        while(transfer.receivers.size() < receivers_per_transfer){
            // find the next valid wallet 
            wallet_id_t receiver_id = *next_wallet_it;
            while(picked.count(receiver_id) > 0){
                // circular increment
                next_wallet_it++;
                if(next_wallet_it == wallet_list.end()){
                    next_wallet_it = wallet_list.begin();
                }

                if(next_wallet_it == started){
                    finish = true;
                    break;
                }

                receiver_id = *next_wallet_it;
            }

            if(finish){
                break;
            }

            picked[receiver_id] = true;
            transfer.receivers[receiver_id] = value_in / receivers_per_transfer;
            value_out += transfer.receivers[receiver_id];
            last_receiver_id = receiver_id;
            
            next_wallet_it = wallet_list.erase(next_wallet_it);
            if(next_wallet_it == wallet_list.end()){
                next_wallet_it = wallet_list.begin();
            }
        }

        // fix value_out > value_in (due to int division)
        if(value_out > value_in){
            coin_value_t diff = value_out - value_in;
            transfer.receivers[last_receiver_id] += diff;
        }

        if(!finish){
            // update expected balance, status
            expected_status[transfer_idx] = success;
            if(success){
                for(auto& sender : transfer.senders){
                    expected_balance[sender.first] -= sender.second;
                }
                
                for(auto& receiver : transfer.receivers){
                    expected_balance[receiver.first] += receiver.second;
                }
            }

            transfers.push_back(transfer);
            transfer_idx++;
        }
    }

    generated = true;
}

void CBDCBenchmarkWorkload::to_file(const std::string fname){
    if(!generated){
        generate();
    }

    ogzstream fout(fname.c_str());

    // parameters
    fout << num_wallets << " " << wallet_start_id << " " << transfers_per_wallet << " " << senders_per_transfer << " " << receivers_per_transfer << " " << wallet_initial_balance << " " << transfer_value << " " << random_seed << " " << transfers.size() << " " << expected_balance.size() << " " << expected_status.size() << std::endl; 

    // wallets and initial balances
    for(wallet_id_t wid=wallet_start_id;wid<(wallet_start_id+num_wallets);wid++){
        fout << wid << " " << wallets[wid] << std::endl;
    }

    // transfers
    for(std::size_t i=0;i<transfers.size();i++){
        auto& transfer = transfers[i];

        // senders
        for(auto& sender : transfer.senders){
            fout << sender.first << " " << sender.second << " ";
        }
        
        // receivers
        for(auto& receiver : transfer.receivers){
            fout << receiver.first << " " << receiver.second << " ";
        }

        fout << std::endl;
    }

    // expected_balance
    for(wallet_id_t wid=wallet_start_id;wid<(wallet_start_id+num_wallets);wid++){
        fout << wid << " " << expected_balance[wid] << std::endl;
    }
    
    // expected_status
    for(uint64_t i=0;i<transfers.size();i++){
        fout << i << " " << expected_status[i] << std::endl;
    }

    fout.close();
}

CBDCBenchmarkWorkload& CBDCBenchmarkWorkload::from_file(const std::string fname){
    igzstream fin(fname.c_str());

    wallet_id_t num_wallets;
    wallet_id_t wallet_start_id;
    uint64_t transfers_per_wallet;
    uint64_t senders_per_transfer;
    uint64_t receivers_per_transfer;
    coin_value_t wallet_initial_balance;
    coin_value_t transfer_value;
    uint64_t random_seed;
    uint64_t transfer_count;
    uint64_t expected_count;
    uint64_t expected_status_count;
    
    // read parameters
    fin >> num_wallets >> wallet_start_id >> transfers_per_wallet >> senders_per_transfer >> receivers_per_transfer >> wallet_initial_balance >> transfer_value >> random_seed >> transfer_count >> expected_count >> expected_status_count;

    CBDCBenchmarkWorkload* benchmark = new CBDCBenchmarkWorkload(num_wallets,wallet_start_id,transfers_per_wallet,senders_per_transfer,receivers_per_transfer,wallet_initial_balance,transfer_value,random_seed);

    // fill benchmark->wallets
    for(wallet_id_t i=0;i<num_wallets;i++){
        wallet_id_t wid;
        coin_value_t balance;
        fin >> wid >> balance;
        benchmark->wallets[wid] = balance;
    }

    // fill benchmark->transfers
    for(uint64_t i=0;i<transfer_count;i++){
        benchmark_transfer_t transfer;

        // senders
        for(uint64_t j=0;j<senders_per_transfer;j++){
            wallet_id_t sender_id;
            coin_value_t value;
            fin >> sender_id >> value;
            transfer.senders[sender_id] = value;
        }
        
        // receivers
        for(uint64_t j=0;j<receivers_per_transfer;j++){
            wallet_id_t receiver_id;
            coin_value_t value;
            fin >> receiver_id >> value;
            transfer.receivers[receiver_id] = value;
        }

        benchmark->transfers.push_back(transfer);
    }
    
    // fill benchmark->expected_balance
    for(uint64_t i=0;i<expected_count;i++){
        wallet_id_t wid;
        coin_value_t value;

        fin >> wid >> value;
        benchmark->expected_balance[wid] = value;
    }
    
    // fill benchmark->expected_status
    for(uint64_t i=0;i<expected_status_count;i++){
        uint64_t transfer_idx;
        bool status;

        fin >> transfer_idx >> status;
        benchmark->expected_status[transfer_idx] = status;
    }

    benchmark->generated = true;
    return *benchmark;
}

