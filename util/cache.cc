// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/param.h>
#include <sys/mman.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <memkind.h>
#include <memkind/internal/memkind_pmem.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.


// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};
// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable(struct memkind *pmem_kind, bool is_nvm) : 
            length_(0), elems_(0), list_(nullptr), 
            table_kind(pmem_kind), nvm_table(is_nvm){ Resize(); }
  ~HandleTable() { 
    //////////////meggie
    if(nvm_table && table_kind){
      memkind_free(table_kind, list_);
    }
    else
      delete[] list_;//释放内存空间
    //////////////////meggie
  }
  ///////////////meggie
  void SetKind(struct memkind *pmem_kind) {table_kind = pmem_kind; }
  void SetNvmcache(bool is_nvm) { nvm_table = is_nvm; }
  //////////////meggie

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {//插入到hash table中去
    LRUHandle** ptr = FindPointer(h->key(), h->hash);//找到对应handle指针的地址
    LRUHandle* old = *ptr;//保存
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);//如果存在，那就通过二级指针进行更新，否则插入到末尾
    *ptr = h;
    if (old == nullptr) {//如果不存在节点
      ++elems_;//增加元素的数目
      if (elems_ > length_) {//保证每个hash桶链表中的平均entry数目<=1,可能有的hash桶数目为1
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();//扩容
      }
    }
    return old;//返回旧节点
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {//移除handle
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {//查找成功
      *ptr = result->next_hash;//通过二级指针删除
      --elems_;//元素减去1,hash表中元素数目
    }
    return result;//返回查找到的handle
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;//hash表中桶的数目
  uint32_t elems_;//hash表中entry的数目
  LRUHandle** list_;// hash bucket数组，二维的链表，每个bucket链表对应的hash值相同
  ////////////meggie
  bool nvm_table;
  struct memkind *table_kind;
  //////////megggie

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {//获取二级指针
    LRUHandle** ptr = &list_[hash & (length_ - 1)];//找到对应的桶，取第一个entry
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {//根据key值获取找到对应的cache handle
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {//扩容hash表
    uint32_t new_length = 4;
    while (new_length < elems_) {//获取新的桶数目，2的幂次方，能够通过hash & (length -1）
      new_length *= 2;
    }
    ////////////////////meggie
    LRUHandle** new_list;
    if(nvm_table && table_kind){
        new_list = (LRUHandle**)memkind_malloc(table_kind, sizeof(LRUHandle*[new_length]));
    }
    else{
      new_list = new LRUHandle*[new_length];//重新分配hash桶头节点指针
    }
    ////////////////////meggie
    memset(new_list, 0, sizeof(new_list[0]) * new_length);//初始化
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {//遍历所有桶，为桶搬家
      LRUHandle* h = list_[i];//每个桶的第一个entry
      while (h != nullptr) {//遍历桶中所有entry
        LRUHandle* next = h->next_hash;//暂时保存桶中下一个entry
        uint32_t hash = h->hash;//hash值
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];//计算在新表中的桶下标，获取新桶中的第一个entry指针的地址
        h->next_hash = *ptr;//头插法，通过二级指针，插入的entry的next_hash应该指向旧的头指针
        *ptr = h;//改变头指针
        h = next;//得到下一个需要搬家的entry指针
        count++;
      }
    }
    assert(elems_ == count);
    //////////////meggie
    if(nvm_table && table_kind){
      memkind_free(table_kind, list_);
    }
    else{
      delete[] list_;//释放内存空间
    }
    //////////////////meggie
    list_ = new_list;//更新list和length,新的hash桶链表，以及桶数目
    length_ = new_length;
  }
};

// A single shard of sharded cache.
class LRUCache {//实质就是通过handleTable和LRUHandle实现的
 public:
  //LRUCache();
  LRUCache(bool L1_cache, size_t Capacity, struct memkind *pmem_kind, bool is_nvm): 
                  capacity_(Capacity),
                  cache_kind(pmem_kind), 
                  nvm_cache(is_nvm), 
                  table_(pmem_kind, is_nvm),
                  L1_cache_(L1_cache),
                  upper_cache_(NULL),
                  lower_cache_(NULL),
                  usage_(0) {//环形双向链表
    lru_.next = &lru_;
    lru_.prev = &lru_;
    in_use_.next = &in_use_;
    in_use_.prev = &in_use_;
  }
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }//设置容量
  /////////////meggie
  void SetKind(struct memkind *pmem_kind) 
  {
    cache_kind = pmem_kind; 
    table_.SetKind(pmem_kind);
  }
  void SetNvmcache(bool is_nvm) 
  { 
    nvm_cache = is_nvm; 
    table_.SetNvmcache(is_nvm);
  }
  void setLowerCache(Cache *lowerCache){
    lower_cache_ = lowerCache;
  }
  void setUpperCache(Cache *upperCache){
    upper_cache_ = upperCache;
  }

  bool addHandleToLowerCache(Cache::Handle *handle);
  bool addHandleToUpperCache(Cache::Handle *handle);
  /////////////meggie

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));//插入key和value，获取LRUHandle
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);//通过hash值判断在哪个LRUCache中搜索
  void Release(Cache::Handle* handle);//客户端不再使用是，显示地调用LRU handle,
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {//总共占用的内存开销
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);//加引用
  void Unref(LRUHandle* e);//解引用
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);//擦除

  // Initialized before use.
  size_t capacity_;//LRU的总容量
  ///////////////meggie
  bool nvm_cache;
  struct memkind *cache_kind;
  Cache *upper_cache_;
  Cache *lower_cache_;
  bool L1_cache_;
  //////////////meggie

  // mutex_ protects the following state.
  mutable port::Mutex mutex_;//互斥锁
  size_t usage_ GUARDED_BY(mutex_);//目前LRU cache所占据的内存空间

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_ GUARDED_BY(mutex_);//傀儡头节点

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_ GUARDED_BY(mutex_);//现在正在被客户端使用的handle双向链表,即引用数>=2,且in_cache = true

  HandleTable table_ GUARDED_BY(mutex_);//LRUcache中都含有一个table cache,用来加快搜索速度
};


LRUCache::~LRUCache() {//析构函数，
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {//只是对引用数减1,真正对handle释放内存空间，在unref()函数
    LRUHandle* next = e->next;//先保存下一个handle的指针
    assert(e->in_cache);
    e->in_cache = false;//表示不在缓存中了，此时引用数应该为1
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);//引用数为0,调用函数进行善后工作，并释放内存
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.如果已经在lru list中了，那就将其移到in_use list中
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;//如果不在cache中，那只是引用数+1
}

void LRUCache::Unref(LRUHandle* e) {//解引用
  assert(e->refs > 0);
  e->refs--;//引用数-1
  if (e->refs == 0) {  // Deallocate.引用数为0了
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    ///////////meggie
    if(nvm_cache && cache_kind){
      memkind_free(cache_kind, e);
    }
    //////////meggie
    else
      free(e);//释放handle所占的内存空间
  } else if (e->in_cache && e->refs == 1) {//如果引用数为1,表示此时没有被客户端使用了
    // No longer in use; move to lru_ list.
    LRU_Remove(e);//将LRU从 in_use list中移除
    LRU_Append(&lru_, e);//将LRU加入到 lru_list中，lru_list第一个节点就是最早未使用的节点
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {//从相应的链表中移除
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {//加入到相应的链表list中
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {//查找
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);//从hash_table中查找
  if (e != nullptr) {//查找成功，加引用
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {//表示不使用了，解引用
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {//将key和数据封装成handle插入到LRUcache中
  MutexLock l(&mutex_);
  ////////////meggie
  LRUHandle* e;
  if(nvm_cache && cache_kind){
    e = reinterpret_cast<LRUHandle*>(
        memkind_malloc(cache_kind, (sizeof(LRUHandle)-1 + key.size())));//分配内存空间
  }
  ///////////meggie
  else
    e = reinterpret_cast<LRUHandle*>(
        malloc(sizeof(LRUHandle)-1 + key.size()));//分配内存空间
  memset(e, 0, sizeof(e));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;//值的字节数
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;//不在cache中，插入后才在
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());//malloc不会调用构造函数，需要自己初始化

  if (capacity_ > 0) {//capacity = 0表示关闭cache
    e->refs++;  // for the cache's reference.引用计数+1
    e->in_cache = true;//表示在缓存中
    LRU_Append(&in_use_, e);//将其插入到in_use list中，当没有被客户使用了，即ref == 1就加入到lru_list中
    usage_ += charge;//占用的内存空间
    FinishErase(table_.Insert(e));//将handle插入到hash table中，将旧节点从LRU cache中删除
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  while (usage_ > capacity_ && lru_.next != &lru_) {//如果内存占用量超过最大容量，并且lru list不为空时
    LRUHandle* old = lru_.next;//获取最久未被使用的handle
    /////////meggie
    Slice old_key = Slice(old->key_data, old->key_length);
    void *old_value = old->value;
    size_t old_charge = old->charge;
    void (*old_deleter)(const Slice& key, void* value) = old->deleter;
    //////////meggie
    assert(old->refs == 1);//lru list中的refs都为1
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
    ///////////meggie
    if(L1_cache_ && lower_cache_){
        Cache::Handle *handle = lower_cache_->Insert(old_key, old_value, old_charge, old_deleter);
        lower_cache_->Release(handle);
    }
    ///////////meggie
  }

  return reinterpret_cast<Cache::Handle*>(e);//返回handle
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {//旧节点存在
    assert(e->in_cache);
    LRU_Remove(e);//旧节点从lru list中移除，之前已经从hash table中移除了
    e->in_cache = false;//不在cache中了
    usage_ -= e->charge;//内存占用量减去固定值
    Unref(e);//引用数减去1
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {//擦除指定handle
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));//首先从hash table中移除，然后从lru cache 中移除
}

void LRUCache::Prune() {//删除所有现在未被使用的entry
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {//遍历lru_中的所有entries,将其全部删掉,从头开始删除
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}
////////////meggie
bool LRUCache::addHandleToLowerCache(Cache::Handle *handle){
    //加入到2级cache的handle必须是lru_list中的值
    MutexLock l(&mutex_);
    if(!L1_cache_ || !lower_cache_)
        return false;
    LRUHandle* old = reinterpret_cast<LRUHandle *>(handle);//获取最久未被使用的handle
    Slice old_key = Slice(old->key_data, old->key_length);
    void *old_value = old->value;
    size_t old_charge = old->charge;
    void (*old_deleter)(const Slice& key, void* value) = old->deleter;
    assert(old->refs == 1);//lru list中的refs都为1
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
    Cache::Handle *new_handle = lower_cache_->Insert(old_key, old_value, old_charge, old_deleter);
    lower_cache_->Release(new_handle);
    return true;
} 
bool LRUCache::addHandleToUpperCache(Cache::Handle *handle){
    //加入到2级cache的handle必须是lru_list中的值
    MutexLock l(&mutex_);
    if(L1_cache_ || !upper_cache_)
        return false;
    LRUHandle* old = reinterpret_cast<LRUHandle *>(handle);//获取最久未被使用的handle
    Slice old_key = Slice(old->key_data, old->key_length);
    void *old_value = old->value;
    size_t old_charge = old->charge;
    void (*old_deleter)(const Slice& key, void* value) = old->deleter;
    assert(old->refs == 1);//lru list中的refs都为1
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
    Cache::Handle *new_handle = upper_cache_->Insert(old_key, old_value, old_charge, old_deleter);
    upper_cache_->Release(new_handle);
    return true;
} 
////////////meggie

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;//16个LRU cache

class ShardedLRUCache : public Cache {//继承于类Cache
 private:
  LRUCache* shard_[kNumShards];//私有成员，LRU cache数组
  port::Mutex id_mutex_;//互斥锁
  uint64_t last_id_;//上一个id
  ////////meggie
  Cache *upper_cache_;
  Cache *lower_cache_;
  /////////////meggie
  static inline uint32_t HashSlice(const Slice& s) {//通过key获取相应的hash值
    return Hash(s.data(), s.size(), 0);//在hash.cc文件中，通过key的值获取hash值
  }

  static uint32_t Shard(uint32_t hash) {//获取相应的LRU cache
    return hash >> (32 - kNumShardBits);
  }

 public:
  bool L1_cache;
  explicit ShardedLRUCache(size_t capacity)//构造函数
      : last_id_(0), 
      L1_cache(true),
      upper_cache_(NULL),
      lower_cache_(NULL){
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;//每个LRU cache平均分到的容量
    for (int s = 0; s < kNumShards; s++) {//设置每个LRU cache的capacity
        shard_[s] = new LRUCache(L1_cache, per_shard, NULL, NULL);
    }
  }
  virtual ~ShardedLRUCache() { 
  }//析构函数
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)]->Insert(key, hash, value, charge, deleter);//插入到指定的LRU cache中
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)]->Lookup(key, hash);//在指定的LRU cache中获取
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)]->Release(handle);//在指定的LRU cache中释放相应的handle
  }
  virtual void Erase(const Slice& key) {//在指定LRU cache中删除指定handle
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)]->Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {//获取handle的value
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {//删除所有cache中的现在未使用的handle
    for (int s = 0; s < kNumShards; s++) {
      shard_[s]->Prune();
    }
  }
  virtual size_t TotalCharge() const {//占据的总内存量
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s]->TotalCharge();
    }
    return total;
  }
  ///////////meggie
  virtual void setUpperCache(Cache *upperCache){
      upper_cache_ = upperCache;
      for (int s = 0; s < kNumShards; s++) {//设置每个LRU cache的capacity
         shard_[s]->setUpperCache(upperCache);
      }
  }
  virtual void setLowerCache(Cache *lowerCache){
      lower_cache_ = lowerCache;
      for (int s = 0; s < kNumShards; s++) {//设置每个LRU cache的capacity
         shard_[s]->setLowerCache(lowerCache);
      }
  }
  //////////meggie
};
//////////////////////////meggie

static const int kNumNVMShardBits = 4;
static const int kNumNVMShards = 1 << kNumNVMShardBits;//16个LRU cache

class ShardedNVMLRUCache : public Cache{
  private:
    LRUCache* shard_[kNumNVMShards];//私有成员，LRU cache数组
    port::Mutex id_mutex_;//互斥锁
    uint64_t last_id_;//上一个id
    struct memkind *pmem_kind;
    ////////meggie
    Cache *upper_cache_;
    Cache *lower_cache_;   
    ////////////meggie

    static inline uint32_t HashSlice(const Slice& s) {//通过key获取相应的hash值
      return Hash(s.data(), s.size(), 0);//在hash.cc文件中，通过key的值获取hash值
    }

    static uint32_t Shard(uint32_t hash) {//获取相应的LRU cache
      return hash >> (32 - kNumShardBits);
    }
  public:
    bool L1_cache;
    ShardedNVMLRUCache(struct memkind *pmem_kind, size_t capacity): 
        last_id_(0), 
        pmem_kind(pmem_kind), 
        L1_cache(false),
        upper_cache_(NULL),
        lower_cache_(NULL)
    {
      const size_t per_shard = (capacity + (kNumNVMShards - 1)) / kNumNVMShards;//每个LRU cache平均分到的容量
      for (int s = 0; s < kNumNVMShards; s++) {//设置每个LRU cache的capacity
         shard_[s] = new LRUCache(L1_cache, per_shard, pmem_kind, capacity);
      }
    }
    virtual ~ShardedNVMLRUCache() { 
      //memkind_destroy_kind(pmem_kind);
      for (int s = 0; s < kNumNVMShards; s++) {//设置每个LRU cache的capacity
         delete shard_[s];
      }
    }//析构函数
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                           void (*deleter)(const Slice& key, void* value)) {
      const uint32_t hash = HashSlice(key);
      return shard_[Shard(hash)]->Insert(key, hash, value, charge, deleter);//插入到指定的LRU cache中
    }
    virtual Handle* Lookup(const Slice& key) {
      const uint32_t hash = HashSlice(key);
      return shard_[Shard(hash)]->Lookup(key, hash);//在指定的LRU cache中获取
    }
    virtual void Release(Handle* handle) {
      LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
      shard_[Shard(h->hash)]->Release(handle);//在指定的LRU cache中释放相应的handle
    }
    virtual void Erase(const Slice& key) {//在指定LRU cache中删除指定handle
      const uint32_t hash = HashSlice(key);
      shard_[Shard(hash)]->Erase(key, hash);
    }
    virtual void* Value(Handle* handle) {//获取handle的value
      return reinterpret_cast<LRUHandle*>(handle)->value;
    }
    virtual uint64_t NewId() {
      MutexLock l(&id_mutex_);
      return ++(last_id_);
    }
    virtual void Prune() {//删除所有cache中的现在未使用的handle
      for (int s = 0; s < kNumNVMShards; s++) {
        shard_[s]->Prune();
      }
    }
    virtual size_t TotalCharge() const {//占据的总内存量
      size_t total = 0;
      for (int s = 0; s < kNumNVMShards; s++) {
        total += shard_[s]->TotalCharge();
      }
      return total;
    }
    ///////////meggie
    virtual void setUpperCache(Cache *upperCache){
        upper_cache_ = upperCache;
        for (int s = 0; s < kNumNVMShards; s++) {//设置每个LRU cache的capacity
           shard_[s]->setUpperCache(upperCache);
        }
    }
    virtual void setLowerCache(Cache *lowerCache){
        lower_cache_ = lowerCache;
        for (int s = 0; s < kNumNVMShards; s++) {//设置每个LRU cache的capacity
           shard_[s]->setLowerCache(lowerCache);
        } 
    }
    //////////meggie  
};
/////////////////////////meggie
}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);//实质是创建了SharedLRUCache对象
}
///////////////////meggie
Cache* NewNVMLRUCache(struct memkind *pmem_kind, size_t capacity) {
  return new ShardedNVMLRUCache(pmem_kind, capacity);//实质是创建了SharedLRUCache对象
}
/////////////////meggie
}  // namespace leveldb
