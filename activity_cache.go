package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "sync"
)

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
  threshold int64
  debug bool
  lru *lru.Cache
  rc *redis.Client
}

/* Increment the counter associated with the given key.
   If the threshold is exceeded, the key is added to the activity queue. */
func (this *ActivityCache) Bump(key string) (result bool) {
  if this.debug {
    fmt.Printf("bump %s\n", key)
  }
  this.m.Lock()
  if val, hit := this.lru.Get(key); hit {
    var ref = val.(*int64)
    if this.debug {
      fmt.Printf("hit %s %d\n", key, *ref)
    }
    *ref += 1
    if *ref >= this.threshold {
      *ref = 0
      this.push(key)
      if this.debug {
        fmt.Printf("cleared %s\n", key)
      }
    }
  } else {
    var counter int64 = 1
    this.lru.Add(key, &counter)
    if this.debug {
      fmt.Printf("new %s\n", key)
    }
  }
  this.m.Unlock()
  return
}

func (this *ActivityCache) push(key string) {
  if _, err := this.rc.LPush("activity_queue", key).Result(); err != nil {
    fmt.Printf("failed to push activity for %s\n", key)
  }
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

func NewActivityCache(redisClient *redis.Client) (*ActivityCache) {
  return &ActivityCache{
    rc: redisClient,
    lru: &lru.Cache{},
  }
}
