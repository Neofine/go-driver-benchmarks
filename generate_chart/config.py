# Title displayed at the top of the generated chart
benchmark_title = "RAM: 2GB, Memory Swap: 4GB, CPUs: 2, GOGC=500 \n 1 000 000 inserts, concurrency = 1024"

# Benchmark results in ms
benchmark_results = {
    'scylla-rust-driver': 8389,
    'scylla-go-driver': 13052,
    'gocql': 17067
}
