export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig
for allocator in MAKALU PMDK NVMMALLOC # RALLOC PMDK NVMMALLOC MAKALU # RALLOC PMDK NVMMALLOC
do
    # if [[ $allocator == "PMDK" ]]; then
        # export PMEM_NO_FLUSH=1
    # fi
    cd ../../pm_allocator/benchmark
    make clean
    make alloc=-D$allocator
    cd ../../nstore-lsm/src
    make clean
    make type=-DALLOCATOR band=-DPCMTOOL no_pm=-DPRECISE # noflush=-DNOFLUSH
    if [[ $allocator ==  "NVMMALLOC" ]]; then
        cp ../../pm_allocator/benchmark/libnvmmalloc.so .
    fi
    for remove in 0
    do
            update=1
            insert=0
            # remove=0
    for thread in 16
    do
        for skew in 0.5 # 0.9
        do
                for numa in 2 # 1 2 # 1 2 #  0 1 2 # 1 2
                do
                    for benchmark in w c m
                    do
                        r_band=0
                        w_band=0
                        r_total=0
                        w_total=0
                        sudo rm -rf /mnt/pmem/*
                        sudo rm -rf /tmp/tpcc
                        echo "run ycsb in $thread thread and $read read and $benchmark engine"
                        if [[ $numa == 0 ]]; then
                            sudo ./nstore -x1000000 -k2000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 1 ]]; then
                            echo "numa1"
                            sudo numactl --physcpubind=1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63 ./nstore -x8000000 -k2000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -n0 > /tmp/tpcc
                        fi
                        if [[ $numa == 2 ]]; then
                            echo "numa2"
                            sudo numactl --physcpubind=0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58,60,62 ./nstore -x10000000 -k10000000 -$benchmark -y -p$update -f$insert -v$remove -e$thread -q$skew -g200 -n0 > /tmp/tpcc
                        fi
                        while read line; do
                            if [[ $line == *"read result"* ]]; then
                                r_total=$(echo $line | awk '{print $4}')
                            fi
                            if [[ $line == *"write result"* ]]; then
                                w_total=$(echo $line | awk '{print $4}')
                            fi
                            if [[ $line == *"read band"* ]]; then
                                r_band=$(echo $line | awk '{print $4}')
                            fi
                            if [[ $line == *"write band"* ]]; then
                                w_band=$(echo $line | awk '{print $4}')
                            fi
                            # if [[ $line == *"cache hits"* ]]; then
                               # r_band=$(echo $line | awk '{print $3}')
                            # fi
                            # if [[ $line == *"cache miss"* ]]; then
                                # w_band=$(echo $line | awk '{print $3}')
                            # fi
                        done < /tmp/tpcc
                        echo "$r_band , $w_band , $r_total , $w_total">>result_yband_cache"_"$allocator.csv
                    done
                done
            done
        done
    done
done