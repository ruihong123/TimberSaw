# How to run
*git clone --recurse-submodules the-code-link
*mkdir -p build && cd build
*cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
*remote memory side: make Server && ./Server
*compute node side: make db_bench
* Write benchmar: ./db_bench --benchmarks=fillrandom --threads=1 --value_size=400 --block_size=8192 --num=10000000
