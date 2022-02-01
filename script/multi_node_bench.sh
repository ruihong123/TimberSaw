#!/bin/bash
home_dir="/users/Ruihong/TimberSaw"
nmemory="1"
ncompute="8"
nmachines="9"
nshard="8"
port=$((10000+RANDOM%1000))
github_repo="https://github.com/ruihong123/TimberSaw"
gitbranch="Tuned_for_multi_setup"
function run_bench() {
  communication_port=()
#	memory_port=()
	memory_server=()
  memory_shard=()

#	compute_port=()
	compute_server=()
	compute_shard=()
#	machines=()
	i=0
  n=0
  while [ $n -lt $nmemory ]
  do
    memory_server+=("node-$i")
    i=$((i+1))
    n=$((n+1))
  done
  n=0
  i=$((nmachines-1))
  while [ $n -lt $ncompute ]
  do

    compute_server+=("node-$i")
    i=$((i-1))
    n=$((n+1))
  done
  echo "here are the sets up"
  echo $?
  echo compute servers are ${compute_server[@]}
  echo memoryserver is ${memory_server[@]}
#  echo ${machines[@]}
  n=0
  while [ $n -lt $nshard ]
  do
    communication_port+=("$((port+n))")
    n=$((n+1))
  done
  n=0
  while [ $n -lt $nshard ]
  do
    # if [[ $i == "2" ]]; then
    # 	i=$((i-1))
    # 	continue
    # fi
    compute_shard+=(${compute_server[$n%$ncompute]})
    memory_shard+=(${memory_server[$n%$nmemory]})
    n=$((n+1))
  done
  echo compute shards are ${compute_shard[@]}
  echo memory shards are ${memory_shard[@]}
  echo communication ports are ${communication_port[@]}
  #test for dowload and compile the codes
  n=0
    while [ $n -lt $nshard ]
  	do
  		echo "Set up the ${compute_shard[n]}"
  		ssh -o StrictHostKeyChecking=no -i cloudlab.pem ${compute_shard[n]} "git clone --recurse-submodules $github_repo && cd TimberSaw/ && mkdir build && cd build && sudo apt-get install -y libnuma-dev && cmake -DCMAKE_BUILD_TYPE=Release .. && make db_bench -j 32 make Server -j 32"
  		sleep 1
  	done
#  n=0
#  while [ $n -lt $nshard ]
#	do
#		echo "creating memory server on ${memory_shards[n]}"
#		cmd="stdbuf --output=0 --error=0 ./nova_server_main --fail_stoc_id=$fail_stoc_id --exp_seconds_to_fail_stoc=$exp_seconds_to_fail_stoc --num_sstable_replicas=$num_sstable_replicas --level=$level --l0_start_compaction_mb=$l0_start_compaction_mb --enable_subrange_reorg=$enable_subrange_reorg --enable_detailed_db_stats=$enable_detailed_db_stats --major_compaction_type=$major_compaction_type --major_compaction_max_parallism=$major_compaction_max_parallism --major_compaction_max_tables_in_a_set=$major_compaction_max_tables_in_a_set --enable_flush_multiple_memtables=$enable_flush_multiple_memtables --recover_dbs=$recover_dbs --num_recovery_threads=$num_recovery_threads  --sampling_ratio=1 --zipfian_dist_ref_counts=$zipfian_dist_file_path --client_access_pattern=$dist  --memtable_type=static_partition --enable_subrange=$enable_subrange --num_log_replicas=$num_log_replicas --log_record_mode=$log_record_mode --scatter_policy=$scatter_policy --number_of_ltcs=$number_of_ltcs --enable_lookup_index=$enable_lookup_index --l0_stop_write_mb=$l0_stop_write_mb --num_memtable_partitions=$num_memtable_partitions --num_memtables=$num_memtables --num_rdma_bg_workers=$num_rdma_bg_workers --db_path=$db_path --num_storage_workers=$num_storage_workers --stoc_files_path=$cc_stoc_files_path --max_stoc_file_size_mb=$max_stoc_file_size_mb --sstable_size_mb=$sstable_size_mb --ltc_num_stocs_scatter_data_blocks=$ltc_num_stocs_scatter_data_blocks --all_servers=$nova_servers --server_id=$server_id --mem_pool_size_gb=$mem_pool_size_gb --use_fixed_value_size=$value_size --ltc_config_path=$ltc_config_path --ltc_num_client_workers=$cc_nconn_workers --num_rdma_fg_workers=$num_rdma_fg_workers --num_compaction_workers=$num_compaction_workers --block_cache_mb=$block_cache_mb --row_cache_mb=$row_cache_mb --memtable_size_mb=$memtable_size_mb --cc_log_buf_size=$cc_log_buf_size --rdma_port=$rdma_port --rdma_max_msg_size=$rdma_max_msg_size --rdma_max_num_sends=$rdma_max_num_sends --rdma_doorbell_batch_size=$rdma_doorbell_batch_size --enable_rdma=$enable_rdma --enable_load_data=$enable_load_data --use_local_disk=$use_local_disk"
#		echo "$cmd"
#		ssh -oStrictHostKeyChecking=no $s "rm -rf $cc_stoc_files_path && mkdir -p $cc_stoc_files_path && rm -rf $db_path && mkdir -p $db_path && cd $cache_bin_dir && $cmd >& $results/server-$s-out &" &
#		server_id=$((server_id+1))
#		nova_rdma_port=$((nova_rdma_port+1))
#		sleep 30
#	done
	}
	run_bench