export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU # RALLOC PMDK NVMMALLOC MAKALU # RALLOC PMDK NVMMALLOC
do
    cd ../../pm_allocator/benchmark
    make clean
    make alloc=-D$allocator
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR no_pm=-DPRECISE band=-DFUNCT latency=-DFUNC noflush=-DFUNC2
    if [[ $allocator ==  "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    # for update in 0.1 0.5 0.9
    # do
        # insert=0
        # remove=0
    for thread in 16 # 8 16 24 
    do
                for numa in 2 # 1 2 # 1 2 #  0 1 2 # 1 2
                do
                    for benchmark in w c m
                    do
                        t_total=0
                        t_ind=0 # r_band=0
                        t_log=0 # w_band=0
                        t_mod=0 # r_total=0
                        t_txn=0 # w_total=0
                        sudo rm -rf /mnt/pmem/*
                        sudo rm -rf /tmp/tpcc
                        echo "run ycsb in $thread thread and $read read and $benchmark engine"
                        if [[ $numa == 0 ]]; then
                            sudo ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 1 ]]; then
                            echo "numa1"
                            sudo numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 2 ]]; then
                            echo "numa2"
                            sudo numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                        fi
                        while read line; do 
                            if [[ $line == *"total dur"* ]]; then # scan
                                t_total=$(echo $line | awk '{print $4}')
                            fi
                            if [[ $line == *"total index"* ]]; then
                                t_ind=$(echo $line | awk '{print $3}')
                            fi
                            if [[ $line == *"total log"* ]]; then # crash consistent
                                t_log=$(echo $line | awk '{print $3}')
                            fi
                            if [[ $line == *"total modify"* ]]; then # execution of modify
                                t_mod=$(echo $line | awk '{print $3}')
                            fi
                            if [[ $line == *"total transaction"* ]]; then # memory management
                                t_txn=$(echo $line | awk '{print $3}')
                            fi
                        done < /tmp/tpcc
                        echo "$t_total $t_ind $t_log $t_mod $t_txn">>result_t_func"_tpcc_"$allocator.csv
                    done
                done
        # done
    done
done