#!/bin/bash
home_dir="/users/Ruihong/TimberSaw"
nmemory="1"
ncompute="8"
nmachines="9"
nshard="8"
port=$((10000+RANDOM%1000))
function run_bench() {
	memory_port=()
	memory_server=()
	compute_port=()
	compute_server=()
#	machines=()
	i=0
  n=0
  while [ $n -lt nmemory ]
  do
    # if [[ $i == "2" ]]; then
    # 	i=$((i+1))
    # 	continue
    # fi
    memory_server+=("node-$i")
    i=$((i+1))
    n=$((n+1))
  done
  n=0
  i=$((nmachines-1))
  while [ $n -lt $ncompute ]
  do
    # if [[ $i == "2" ]]; then
    # 	i=$((i-1))
    # 	continue
    # fi
    compute_server+=("node-$i")
    i=$((i-1))
    n=$((n+1))
  done
  echo "here are the sets up"
  echo $?
  echo ${compute_server[@]}
  echo ${memory_server[@]}
#  echo ${machines[@]}
  nova_servers=""
  	nova_all_servers=""
  	i="0"
  	for s in ${TimberSawservers[@]}
  	do
  		TimberSawport="$port"
  		TimberSawservers="$TimberSawservers,$s:$TimberSawport"
  		i=$((i+1))
#  		if [[ $i -le number_of_ltcs ]]; then
#  			nova_all_servers="$nova_all_servers,$s:$nova_port"
#  		fi
  		TimberSawport=$((TimberSawport+1))
  	done
  	nova_servers="${nova_servers:1}"
  	nova_all_servers="${nova_all_servers:1}"
	}
	run_bench