export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU PMDK NVMMALLOC
do
    cd ../../pm_allocator/benchmark
    make clean
    make alloc=-D$allocator
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR no_pm=-DPRECISE
    if [[ $allocator ==  "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    
    for txn_nums in 100000 200000 300000
    do
        index=0
        rec_array=()
        for benchmark in w l m
        do
            sudo rm -rf /mnt/pmem/*
            sudo rm -rf /tmp/tpcc
            sudo ./nstore -x$txn_nums -k2000000 -$benchmark -y -r -e1 -n0 > /tmp/tpcc
            
            while read line; do
                if [[ $line == *"Recovery"* ]]; then
                    rec_array[$index]=$(echo $line | awk '{print $7}')
                fi
            done < /tmp/tpcc
            let index++
        done
        echo "${rec_array[@]}">>result_r_$allocator.csv
    done
done