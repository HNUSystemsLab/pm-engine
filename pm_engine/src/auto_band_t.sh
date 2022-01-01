export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU PMDK NVMMALLOC # PMDK NVMMALLOC # RALLOC MAKALU PMDK NVMMALLOC
do
    cd ../../pm_allocator/benchmark
    sudo make clean
    sudo -E make alloc=-D$allocator all
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR band=-DPCMTOOL no_pm=-DPRECISE # noflush=-DNOFLUSH
    if [[ $allocator == "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    for thread in 16 
    do
        for numa in 2 
        do
            for benchmark in w
            do
                sudo rm -rf /mnt/pmem/*
                sudo rm -rf /tmp/tpcc
                echo "run ycsb in $thread thread and $read read and $benchmark engine"
                if [[ $numa == 0 ]]; then
                    sudo -E ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 1 ]]; then
                    echo "numa1!"
                    sudo -E numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                if [[ $numa == 2 ]]; then
                    echo "numa2!"
                    sudo -E numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x800000 -$benchmark -t -e$thread -n0 > /tmp/tpcc
                fi
                read_band=0
                read_total=0
                write_band=0
                write_total=0
                while read line; do
                    if [[ $line == *"cache hits"* ]]; then
                        read_band=$(echo $line | awk '{print $3}')
                    fi
                    if [[ $line == *"cache miss"* ]]; then
                        write_band=$(echo $line | awk '{print $3}')
                    fi
                    if [[ $line == *"Throughput"* ]]; then
                        read_total=$(echo $line | awk '{print $4}')
                    fi
                    if [[ $line == *"write result"* ]]; then
                        write_total=$(echo $line | awk '{print $4}')
                    fi
                done < /tmp/tpcc
            echo "$read_band , $write_band , $read_total , $write_total">>result_t_band"_"$benchmark"_"$allocator.csv
            done
        done
    done
done