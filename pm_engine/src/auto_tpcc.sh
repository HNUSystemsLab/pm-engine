export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in NVMMALLOC # RALLOC MAKALU PMDK NVMMALLOC
do
    #if [[ $allocator == "PMDK" ]]; then
        #export PMEM_NO_FLUSH=1
    # fi
    cd ../../pm_allocator/benchmark
    sudo make clean
    sudo -E make alloc=-D$allocator all
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR no_pm=-DPRECISE # noflush=-DNOFLUSH
    if [[ $allocator == "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so . # noflush.so .
    fi
    for thread in 32
    do
        for numa in 1 2 3 # 1 2 # 1 2 # 1 2 # 0 1 2
        do
            th_array=()
            index=0
            for benchmark in m
            do
                sudo rm -rf /mnt/pmem/*
                sudo rm -rf /tmp/tpcc
                echo "run ycsb in $thread thread and $read read and $benchmark engine"
                if [[ $numa == 0 ]]; then # no limit
                    sudo -E ./nstore -x1000000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 1 ]]; then # remote
                    echo "numa1!"
                    sudo -E numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 2 ]]; then # local
                    echo "numa2!"
                    sudo -E numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 3 ]]; then # half
                    sudo -E numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31  ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                while read line; do
                    if [[ $line == *"Throughput"* ]]; then
                        th_array[$index]=$(echo $line | awk '{print $3}')
                    fi
                done < /tmp/tpcc
                let index++
            done
            echo "${th_array[@]}">>result_t_lsm_confirm_$allocator.csv
        done
    done
done
# sudo numactl --cpunodebind=0 ./nstore -x8000000 -$benchmark -t -e$thread -q0.2 -n0 > /tmp/tpcc