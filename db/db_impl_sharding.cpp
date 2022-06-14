//
// Created by ruihong on 6/12/22.
//

#include "db_impl_sharding.h"

namespace TimberSaw {

DBImpl_Sharding::DBImpl_Sharding(const Options& options, const std::string& dbname) {
    assert(options.ShardInfo->size() != 0);
    for (auto iter : *options.ShardInfo) {
      Shard_Info.emplace_back(iter.first.ToString(), iter.second.ToString());
    }
    for(const auto& iter : Shard_Info) {
      //We can not set the target node id in DBImpl because we don't know what should be
      // the node id corresponding with this shard. (Is that true?) Probably not.
      // Now the shards are assigned to target memory nodes in a strictly round robin manner
      // according to the upper bound of shard.
      auto sharded_db = new DBImpl(options, dbname);
      shards_pool.insert({iter.second, sharded_db});
    }
    int i = 0;
    for(auto & iter : shards_pool){
      iter.second->Setup_target_id_create_handling_thread(2*i);
      i++;
    }
    for(auto & iter : shards_pool){
      iter.second->Wait_for_client_message_hanlding_setup();
    }
}
DBImpl_Sharding::~DBImpl_Sharding() {
  for(auto iter : shards_pool){
//    delete[] iter.first.data();
    delete iter.second;
  }
}
Status DBImpl_Sharding::Put(const WriteOptions& options, const Slice& key,
                            const Slice& value) {
  DBImpl* db;
  if(Get_Target_Shard(db, key)){
    WriteBatch batch;
    batch.Put(key, value);
    return db->Write(options, &batch);
  }else{
    // forward to other shards
    assert(false);
    return Status();
  }

}
Status DBImpl_Sharding::Delete(const WriteOptions& options, const Slice& key) {
  DBImpl* db;
  if(Get_Target_Shard(db, key)){
    WriteBatch batch;
    batch.Delete(key);
    return db->Write(options, &batch);
  }else{
    // forward to other shards
    assert(false);
    return Status();
  }

}
Status DBImpl_Sharding::Write(const WriteOptions& options,
                              WriteBatch* updates) {
  DBImpl* db;
  assert(false);
  //TODO: cross shard batch should be atomic.
//  Get_Target_Shard(db, key);
  return Status();
}
Status DBImpl_Sharding::Get(const ReadOptions& options, const Slice& key,
                            std::string* value) {
  DBImpl* db;
  if(Get_Target_Shard(db, key)){
    return db->Get(options, key, value);
  }else{
    assert(false);
    return Status();
  }

}
Iterator* DBImpl_Sharding::NewIterator(const ReadOptions& options) {
  //TODO: support cross shard iterator.
  DBImpl* db = shards_pool.begin()->second;

  return db->NewIterator(options);
}
#ifdef BYTEADDRESSABLE
Iterator* DBImpl_Sharding::NewSEQIterator(const ReadOptions& options) {
  DBImpl* db = shards_pool.begin()->second;

  return db->NewSEQIterator(options);
}
#endif
const Snapshot* DBImpl_Sharding::GetSnapshot() {
  //TODO: This is vital for the cross shard transaction.
  return nullptr;
}
void DBImpl_Sharding::ReleaseSnapshot(const Snapshot* snapshot) {}
bool DBImpl_Sharding::GetProperty(const Slice& property, std::string* value) {
  //Not implemented.
  return false;
}
void DBImpl_Sharding::GetApproximateSizes(const Range* range, int n,
                                          uint64_t* sizes) {
  //Not implemented
}
void DBImpl_Sharding::CompactRange(const Slice* begin, const Slice* end) {
  //Not implemented
  assert(false);
}
void DBImpl_Sharding::WaitforAllbgtasks(bool clear_mem) {
  for(auto iter : shards_pool){
    iter.second->WaitforAllbgtasks(clear_mem);
  }
}
}