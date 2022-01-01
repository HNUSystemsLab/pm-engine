export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU NVMMALLOC PMDK
do
    cd ../../pm_allocator/benchmark
    make clean
    make alloc=-D$allocator all
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR no_pm=-DPRECISE latency=-DLATENCY
    if [[ $allocator == "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    for insert in 0 # thread in 16 24# 0 0.1 0.5 0.9
    do  
        remove=0
        update=1
        for thread in 16 # 8 16 24 
        do
            for skew in 0.5 # 0.5 0.9
            do
                for numa in 2
                do
                    th_array=()
                    index=0
                    for benchmark in w m c
                    do
                        lat_in_array=()
                        sub_in_index=0
                        lat_de_array=()
                        sub_de_index=0
                        lat_up_array=()
                        sub_up_index=0
                        lat_sl_array=()
                        sub_sl_index=0
                        sudo rm -rf /mnt/pmem/*
                        echo "run ycsb in $thread thread and $read read and $benchmark engine"
                        if [[ $numa == 0 ]]; then
                            sudo ./nstore -x8000000 -k2000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 1 ]]; then
                            echo "numa1"
                            sudo numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x2000000 -k2000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 2 ]]; then
                            echo "numa2"
                            sudo numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -g200 -q$skew -n0 > /tmp/tpcc
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
                        done < /tmp/tpcc
                        # echo "${lat_in_array[@]}">>result_lat_$allocator"_"$benchmark_"insert".csv
                        # echo "${lat_de_array[@]}">>result_lat_$allocator"_"$benchmark_"delete".csv
                        echo "${lat_up_array[@]}">>result_$allocator"_"$benchmark_"update".csv
                        # echo "${lat_sl_array[@]}">>result_$allocator"_"$benchmark_"read".csv
                    done
                done
            done
        done
    done
done