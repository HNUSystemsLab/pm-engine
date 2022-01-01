#include <vector>
#include <map>
#include <thread>
#include <iostream>
#include <string>
#include <algorithm>

class Latency{
    public:
        std::map<std::thread::id, std::vector<uint64_t>> latency_select, latency_insert, latency_delete, latency_update, latency_stock;
        std::vector<uint64_t> total_select, total_insert, total_delete, total_update, total_stock;
        void init_func(std::thread::id tid){
            latency_insert[tid].push_back(0); //IND //delivery //clflush
            latency_delete[tid].push_back(0); //LOG //new_order //sfence
            latency_update[tid].push_back(0); //MOD //order status
            latency_select[tid].push_back(0); //TXN 
            latency_stock[tid].push_back(0);
        }
        void output_latency(std::vector<size_t> total_vec, std::map<std::thread::id, std::vector<size_t>> single_map, std::string op_name){
            if(single_map.size() == 0)
                return;
            std::cout<<"map size is"<<single_map.size()<<std::endl;
            auto iter = single_map.begin();
            int count = 0;
            while (iter != single_map.end()){
                total_vec.insert(total_vec.end(), iter->second.begin(), iter->second.end());
                iter++;
                count++;
            }
            sort(total_vec.begin(), total_vec.end());
            std::cout<< op_name << " latency: "<<std::endl;
            //std::cout<< op_name <<" %10: "<<total_vec[total_vec.size()*0.1]<< std::endl;
            //std::cout<< op_name <<" %50: "<<total_vec[total_vec.size()*0.5]<<std::endl;
            std::cout<< op_name <<" %90: "<<total_vec[total_vec.size()*0.9]<<std::endl;
            std::cout<< op_name <<" %99: "<<total_vec[total_vec.size()*0.99]<<std::endl;
            std::cout<< op_name <<" %99.9: "<<total_vec[total_vec.size()*0.999]<<std::endl;
            std::cout<< op_name <<" %99.99: "<<total_vec[total_vec.size()*0.9999] <<std::endl;
            std::cout<< op_name <<" %99.999: "<<total_vec[total_vec.size()*0.99999] <<std::endl;
        }
        void output_all(){
            output_latency(total_insert, latency_insert, "INSERT");
            output_latency(total_delete, latency_delete, "DELETE");
            output_latency(total_update, latency_update, "UPDATE");
            output_latency(total_select, latency_select, "SELECT");
#ifdef TCOUT
            output_latency(total_stock, latency_stock, "STOCK");
#endif
        }
        void output_all_func(){
            output_func(latency_insert, "index");
            output_func(latency_delete, "log");
            output_func(latency_update, "modify");
            output_func(latency_select, "transaction");        
        }
        void output_func(std::map<std::thread::id, std::vector<size_t>> single_map, std::string op_name){
            uint64_t func_array=0;
            if(single_map.size() == 0)
                return;
            std::cout<<"map size is"<<single_map.size()<<std::endl;
            auto iter = single_map.begin();
            int count = 0;
            while (iter != single_map.end()){
                if(iter->second.size()!=0){
                    func_array += iter->second.at(0);
                }
                iter++;
                count++;
            }
#ifndef FLUSH_C
            std::cout << "total "<<op_name <<": "<< func_array / (1000 * 1000) << " ms "<<std::endl;
#else
            std::cout << "total "<<op_name << ": "<< func_array << std::endl;
#endif           
            //std::cout<<"total index: " << func_array[OP_IND % 5] / (1000*1000) <<" ms "<<std::endl;
            //std::cout<<"total log: " << func_array[OP_LOG % 5] / (1000*1000) <<" ms "<<std::endl;
            //std::cout<<"total modify: " << func_array[OP_MOD % 5] / (1000*1000) <<" ms "<<std::endl;
            //std::cout<<"total transaction: " << func_array[OP_TXN % 5] / (1000*1000) <<" ms "<<std::endl;
        }
};