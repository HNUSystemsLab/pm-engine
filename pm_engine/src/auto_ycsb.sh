export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU NVMMALLOC PMDK
do
    # if [[ $allocator == "PMDK" ]]; then
        # export PMEM_NO_FLUSH=1
    # fi
    cd ../../pm_allocator/benchmark
    make clean
    make alloc=-D$allocator all
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR no_pm=-DPRECISE # noflush=-DNOFLUSH
    if [[ $allocator == "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    for insert in 0 0.1 0.5 0.9
    do
        remove=0
        update=0
        for thread in 8 16 20 24 28 32
        do
            for skew in 0.5 
            do
                for numa in 2
                do
                    for ysize in 200
                    do
                    th_array=()
                    index=0
                    for benchmark in w m c
                    do
                        
                        sudo rm -rf /mnt/pmem/*
                        sudo rm -rf /tmp/tpcc
                        echo "run ycsb in $thread thread and $read read and $benchmark engine"
                        if [[ $numa == 0 ]]; then #no limit
                            sudo ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -g$ysize -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 1 ]]; then # remote
                            echo "numa1"
                            sudo numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -g$ysize -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 2 ]]; then # local
                            echo "numa2"
                            sudo -E numactl --cpunodebind=0 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -g$ysize -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 3 ]]; then # half
                            echo "numa3"
                            sudo numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -g$ysize -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 4 ]]; then
                            sudo numactl --physcpubind=0,32,2,34,4,36,6,38,8,40,10,42,12,44,14,46,1,31,3,35,5,37,7,39,9,41,11,43,13,45,15,47 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -n0 > /tmp/tpcc
                        fi
                        while read line; do
                            if [[ $line == *"Throughput"* ]]; then
                                th_array[$index]=$(echo $line | awk '{print $3}')
                            fi
                        done < /tmp/tpcc
                        let index++
                    done
                    echo "${th_array[@]}">>result_y"_insert_$insert"$allocator.csv
                    done
                done  
            done
        done
    done
done