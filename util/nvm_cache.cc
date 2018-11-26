
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
#include "util/lru_handle.h"

namespace leveldb{

namespace {

class LRUCache {//实质就是通过handleTable和LRUHandle实现的
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }//设置容量
  void SetKind(struct memkind* pmem_kind) { cache_kind = pmem_kind; }//设置容量

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
  struct memkind* cache_kind;

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

LRUCache::LRUCache()
    : usage_(0) {//环形双向链表
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

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

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));//分配内存空间
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
    assert(old->refs == 1);//lru list中的refs都为1
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
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


static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;//16个LRU cache

class ShardedNVMLRUCache : public Cache {//继承于类Cache
 private:
  NVMLRUCache shard_[kNumShards];//私有成员，LRU cache数组
  port::Mutex id_mutex_;//互斥锁
  uint64_t last_id_;//上一个id

  static inline uint32_t HashSlice(const Slice& s) {//通过key获取相应的hash值
    return Hash(s.data(), s.size(), 0);//在hash.cc文件中，通过key的值获取hash值
  }

  static uint32_t Shard(uint32_t hash) {//获取相应的LRU cache
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedNVMLRUCache(struct memkind *pmem_kind, size_t capacity)//构造函数
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;//每个LRU cache平均分到的容量
    for (int s = 0; s < kNumShards; s++) {//设置每个LRU cache的capacity
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedNVMLRUCache() { }//析构函数
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);//插入到指定的LRU cache中
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);//在指定的LRU cache中获取
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);//在指定的LRU cache中释放相应的handle
  }
  virtual void Erase(const Slice& key) {//在指定LRU cache中删除指定handle
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
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
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {//占据的总内存量
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}

Cache* NewNVMLRUCache(std::string *filename, size_t capacity) {
  return new ShardedNVMLRUCache(filename, capacity);//实质是创建了SharedLRUCache对象
}
}
