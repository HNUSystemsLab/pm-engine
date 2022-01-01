export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU NVMMALLOC PMDK # NVMMALLOC PMDK NVMMALLOC # MAKALU PMDK NVMMALLOC # RALLOC
do
    cd ../../pm_allocator/benchmark
    sudo make clean
    sudo -E make alloc=-D$allocator all
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR latency=-DLATENCY no_pm=-DPRECISE band=-DTCOUT
    if [[ $allocator == "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    for thread in 4
    do
        for numa in 1 # 2 
        do
            for benchmark in m 
            do
                lat_in_array=()
                sub_in_index=0
                lat_de_array=()
                sub_de_index=0
                lat_up_array=()
                sub_up_index=0
                lat_sl_array=()
                sub_sl_index=0
                lat_sto_array=()
                sub_sto_index=0
                sudo rm -rf /mnt/pmem/*
                sudo rm -rf /tmp/tpcc
                echo "run ycsb in $thread thread and $read read and $benchmark engine"
                if [[ $numa == 1 ]]; then
                    sudo -E numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 2 ]]; then
                    echo "numa!"
                    sudo numactl --cpunodebind=0 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                while read line; do
                    if [[ $line == *"INSERT %"* ]]; then
                        lat_in_array[$sub_in_index]=$(echo $line | awk '{print $3}')
                        let sub_in_index++
                    fi
                    if [[ $line == *"DELETE %"* ]]; then
                        lat_de_array[$sub_de_index]=$(echo $line | awk '{print $3}')
                        let sub_de_index++
                    fi
                    if [[ $line == *"UPDATE %"* ]]; then
                        lat_up_array[$sub_up_index]=$(echo $line | awk '{print $3}')
                        let sub_up_index++
                    fi
                    if [[ $line == *"SELECT %"* ]]; then
                        lat_sl_array[$sub_sl_index]=$(echo $line | awk '{print $3}')
                        let sub_sl_index++
                    fi
                    if [[ $line == *"STOCK %"* ]]; then
                        lat_sto_array[$sub_sto_index]=$(echo $line | awk '{print $3}')
                        let sub_sto_index++
                    fi
                done < /tmp/tpcc
                echo "${lat_in_array[@]}">>result_t_l_$allocator"_"$benchmark.csv # delivery 
                echo "${lat_de_array[@]}">>result_t_l_$allocator"_"$benchmark.csv # new_order
                echo "${lat_up_array[@]}">>result_t_l_$allocator"_"$benchmark.csv # order status
                echo "${lat_sl_array[@]}">>result_t_l_$allocator"_"$benchmark.csv # payment
                echo "${lat_sto_array[@]}">>result_t_l_$allocator"_"$benchmark.csv # stock_level 
            done
        done
    done
done
# sudo numactl --cpunodebind=0 ./nstore -x8000000 -$benchmark -t -e$thread -q0.2 -n0 > /tmp/tpcc