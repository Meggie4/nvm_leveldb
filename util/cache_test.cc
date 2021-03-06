// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <vector>
#include "util/coding.h"
#include "util/testharness.h"
#include <errno.h>

#include <errno.h>
#include <memkind.h>
#include <memkind/internal/memkind_pmem.h>
#include "util/debug.h"

#define PMEM_CACHE_SIZE MEMKIND_PMEM_MIN_SIZE * 1
namespace leveldb {

struct student{
  std::string name;
  std::string birthday;
};

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}
static int DecodeKey(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class CacheTest {
 public:
  static CacheTest* current_;//静态变量

  static void Deleter(const Slice& key, void* v) {//静态函数清除工作，
    current_->deleted_keys_.push_back(DecodeKey(key));
    current_->deleted_values_.push_back(DecodeValue(v));
  }

  static const int kCacheSize = 1000;//静态变量缓存大小，1000B
  std::vector<int> deleted_keys_;//回收key
  std::vector<int> deleted_values_;//回收value
  Cache* cache_;

  CacheTest(){//构造函数
    struct memkind *pmem_kind;
    size_t block_size = 4 * 1024;
    std::string dirname = "/mnt/pmemdir";
    cache_ = NewNVMLRUCache(dirname, pmem_kind, PMEM_CACHE_SIZE, block_size);
    current_ = this;
  }

  ~CacheTest() {
    delete cache_;
  }

  int Lookup(int key) {//查找
    Cache::Handle* handle = cache_->Lookup(EncodeKey(key));//查找LRU cache
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache_->Value(handle));
    if (handle != nullptr) {
      cache_->Release(handle);//表示不用了
    }
    return r;
  }

  void Insert(int key, int value, int charge = 1) {//插入
    Cache::Handle* h = cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                                   &CacheTest::Deleter);//插入之后就不使用了
    if(h)
      cache_->Release(h);//插入之后就不使用了
    /*else
      DEBUG_T("h is null\n");*/
  }

  Cache::Handle* InsertAndReturnHandle(int key, int value, int charge = 1) {//插入后，使用
    return cache_->Insert(EncodeKey(key), EncodeValue(value), charge,
                          &CacheTest::Deleter);
  }

  void Erase(int key) {//擦除key
    cache_->Erase(EncodeKey(key));
  }
};
CacheTest* CacheTest::current_;

TEST(CacheTest, HitAndMiss) {//命中和缺失
  DEBUG_T("before_lookup\n");
  ASSERT_EQ(-1, Lookup(100));
  DEBUG_T("finish_lookup\n");

  Insert(100, 101);
  DEBUG_T("finish_insert\n");
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));

  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);
  DEBUG_T("finish_hitand_miss\n");
}



TEST(CacheTest, Erase) {//擦除
  Erase(200);
  ASSERT_EQ(0, deleted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, deleted_keys_.size());
}

TEST(CacheTest, EntriesArePinned) {
  Insert(100, 101);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0, deleted_keys_.size());

  cache_->Release(h1);
  ASSERT_EQ(1, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[0]);
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, deleted_keys_.size());

  cache_->Release(h2);
  ASSERT_EQ(2, deleted_keys_.size());
  ASSERT_EQ(100, deleted_keys_[1]);
  ASSERT_EQ(102, deleted_values_[1]);
}


TEST(CacheTest, EvictionPolicy) {//替换策略
  Insert(100, 101);//插入(100,101)
  Insert(200, 201);//插入(200,201)
  Insert(300, 301);//插入(300,301)
  Cache::Handle* h = cache_->Lookup(EncodeKey(300));//查找，在使用中，那肯定不能替换出去

  // Frequently used entry must be kept around,
  // as must things that are still in use.
  for (int i = 0; i < kCacheSize + 100; i++) {
    Insert(1000+i, 2000+i);//插入(1000, 2000)
    ASSERT_EQ(2000+i, Lookup(1000+i));//查找后又释放了
    ASSERT_EQ(101, Lookup(100));//一直在查找，即使最后在lru_list中了，但是因为一直再使用，也不会被淘汰
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));//在二级cache中找到
  ASSERT_EQ(301, Lookup(300));
  cache_->Release(h);
}

TEST(CacheTest, UseExceedsCacheSize) {//超过内存容量
  // Overfill the cache, keeping handles on all inserted entries.
  std::vector<Cache::Handle*> h;
  int added = 0;
  int index = 0;
  DEBUG_T("before insert\n");
  while(added < PMEM_CACHE_SIZE + 8 * 1024){
    //DEBUG_T("i:%lu, PMEM_MAX_SIZE:%lu\n", i, kCacheSize + PMEM_MAX_SIZE);
    h.push_back(InsertAndReturnHandle(index, index + 1, 4 * 1024));
    if(index < 3)
       cache_->Release(h[index]);
    index++;
    added += 4*1024;
        
  }
  DEBUG_T("after insert\n");

  // Check that all the entries can be found in the cache.
  for (int i = 3; i < h.size(); i++) {
    ASSERT_EQ(1+i, Lookup(i));
  }

  for (int i = 3; i < h.size(); i++) {
    cache_->Release(h[i]);
  }
}
/*
TEST(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  size_t TwoLevelCache_SIZE = kCacheSize + PMEM_MAX_SIZE;
  DEBUG_T("before add\n");
  while (added < 2*TwoLevelCache_SIZE ){
    if(added >= TwoLevelCache_SIZE)
        DEBUG_T("the L2_cache if full\n");
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000+index, weight);//wight表示键值对中value占用的内存空间
    added += weight;//added表示所有值占用的内存空间
    index++;//key的值
    DEBUG_T("added:%lu, kCacheSize:%lu, TwoLevelCache_SIZE:%lu\n", added, kCacheSize, TwoLevelCache_SIZE);
  }
  DEBUG_T("finish add\n");
  int cached_weight = 0;
  for (int i = 0; i < index; i++) {//遍历所有插入过的键值对
    const int weight = (i & 1 ? kLight : kHeavy);//获取其权重
    int r = Lookup(i);//查找
    if (r >= 0) {//查找成功的
      cached_weight += weight;
      ASSERT_EQ(1000+i, r);
    }
  }
  ASSERT_LE(cached_weight, TwoLevelCache_SIZE + TwoLevelCache_SIZE/10);
}

TEST(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}

TEST(CacheTest, Prune) {
  Insert(1, 100);
  Insert(2, 200);

  Cache::Handle* handle = cache_->Lookup(EncodeKey(1));
  ASSERT_TRUE(handle);
  cache_->Prune();
  cache_->Release(handle);

  ASSERT_EQ(100, Lookup(1));
  ASSERT_EQ(-1, Lookup(2));
}

TEST(CacheTest, ZeroSizeCache) {//
  delete cache_;
  cache_ = NewLRUCache(0);

  Insert(1, 100);
  ASSERT_EQ(-1, Lookup(1));
}
*/


}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
