sudo perf script -i perf.data > perf.unfold
sudo ./../../FlameGraph/stackcollapse-perf.pl perf.unfold > perf.fold
sudo ./../../FlameGraph/flamegraph.pl perf.fold > perf.svg
