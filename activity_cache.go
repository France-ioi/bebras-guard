package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "sync"
)

type KeySet map[string]struct{}
type TagSet map[string]struct{}

type ActivityCacheConfig struct {

  // Size of the local LRU cache.
  MaxEntries int

  // Activity threshold.
  Threshold int64

  // Print debugging messages?
  Debug bool

}

/* Returns default Config. */
func NewActivityCacheConfig() (ActivityCacheConfig) {
  return ActivityCacheConfig{
    MaxEntries: 65536,
    Threshold: 20,
    Debug: false,
  }
}

type ActivityCache struct {
  m sync.Mutex
  cc *CounterCache
  threshold int64
  debug bool
  lru *lru.Cache
  rc *redis.Client
  markedKeys KeySet
  markedTags TagSet
}

type ActivityCacheEntry struct {
  counter int64
  keys KeySet
}

/* Associate a key with each of the tags. */
func (this *ActivityCache) Assoc(key string, tags []string) {
  this.m.Lock()
  defer this.m.Unlock()
  for _, tag := range tags {
    var entry *ActivityCacheEntry
    if val, hit := this.lru.Get(tag); hit {
      entry = val.(*ActivityCacheEntry)
    } else {
      entry = this.clear(tag)
    }
    entry.keys[key] = struct{}{}
  }
  return
}

/* Increment the counter associated with the tag.  If the counter goes over the
   threshold, mark the tag and all its associated keys. */
func (this *ActivityCache) Bump(tag string) {
  this.m.Lock()
  defer this.m.Unlock()
  var entry *ActivityCacheEntry
  if val, hit := this.lru.Get(tag); hit {
    entry = val.(*ActivityCacheEntry)
    entry.counter += 1
    if entry.counter >= this.threshold {
      this.markedTags[tag] = struct{}{}
      if this.debug {
        fmt.Printf("marked tag %s\n", tag)
      }
      for key, _ := range entry.keys {
        this.markedKeys[key] = struct{}{}
        if this.debug {
          fmt.Printf("marked key %s\n", key)
        }
      }
      this.clear(tag)
    }
  }
  return
}

func (this *ActivityCache) clear(key string) (*ActivityCacheEntry) {
  entry := ActivityCacheEntry{counter: 0, keys: make(KeySet)}
  this.lru.Add(key, &entry)
  return &entry
}

func (this *ActivityCache) Flush() {
  this.m.Lock()
  defer this.m.Unlock()
  if len(this.markedKeys) != 0 {
    for key, _ := range this.markedKeys {
      this.cc.Push(key)
      if this.debug {
        fmt.Printf("pushed key %s\n", key)
      }
    }
    this.markedKeys = make(KeySet)
  }
  if len(this.markedTags) != 0 {
    for tag, _ := range this.markedTags {
      if _, err := this.rc.LPush("activity_queue", tag).Result(); err != nil {
        fmt.Printf("failed to push activity for %s\n", tag)
      }
      if this.debug {
        fmt.Printf("pushed tag %s\n", tag)
      }
    }
    this.markedTags = make(TagSet)
  }
  return
}

/* Updates the cache's configuration. */
func (this *ActivityCache) Configure(config ActivityCacheConfig) {
  this.m.Lock()
  defer this.m.Unlock()
  this.threshold = config.Threshold
  this.debug = config.Debug
  if config.MaxEntries > 0 {
    // Trim the cache as needed.
    for this.lru.Len() > config.MaxEntries {
      this.lru.RemoveOldest()
    }
  }
  this.lru.MaxEntries = config.MaxEntries
}

func NewActivityCache(redisClient *redis.Client, counterCache *CounterCache) (*ActivityCache) {
  return &ActivityCache{
    cc: counterCache,
    rc: redisClient,
    lru: &lru.Cache{},
    markedKeys: make(KeySet),
    markedTags: make(TagSet),
  }
}
