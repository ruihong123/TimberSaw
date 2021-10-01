//
// Created by ruihong on 9/30/21.
//
#include <stdlib.h>
#include <string>
#include <iostream>
#include "include/TimberSaw/db.h"
#include "include/TimberSaw/filter_policy.h"
int main()
{
  TimberSaw::DB* db;
  TimberSaw::Options options;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  //TODO: implement the FIlter policy before initial the database
  auto b_policy = TimberSaw::NewBloomFilterPolicy(options.bloom_bits);
  options.filter_policy = b_policy;
  TimberSaw::Status s = TimberSaw::DB::Open(options, "mem_leak", &db);
  std::string value;
  std::string key;
  auto option_wr = TimberSaw::WriteOptions();
  for (int i = 0; i<10000000; i++){
    key = std::to_string(i);
    key.insert(0, 20 - key.length(), '0');
    value = std::to_string(std::rand() % ( 4500001 ));
    value.insert(0, 400 - value.length(), '0');
    s = db->Put(option_wr, key, value);
    if (!s.ok()){
      std::cerr << s.ToString() << std::endl;
    }

    //     std::cout << "iteration number " << i << std::endl;
  }
  delete db;
}