package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "sync"
  "time"
  "sync/atomic"
  "math"
  _ "log"
)

type Counter struct {
  Local int64
  Shared int64
  SharedExpiry int64
}

func (c Counter) Value() (result int64) {
  return c.Local + c.Shared;
}

type Store struct {
  CounterTtl time.Duration
  LocalCounterThreshold int64
  rc  *redis.Client
  rw  sync.RWMutex
  lru *lru.Cache
}

/* Returns the counter associated with the given key from the local store,
   or nil if there is no local counter. */
func (c *Store) FastGet(key string) (result *Counter) {
  c.rw.RLock();
  if val, hit := c.lru.Get(key); hit {
    result = val.(*Counter)
  } else {
    result = nil
  }
  c.rw.RUnlock()
  return
}

/* Returns the counter associated with the given key.
   The remote store is queried to update the shared part of the counter. */
func (c *Store) Get(key string) (result *Counter) {
  /* Add the new counter to the cache if missing */
  var pull bool
  c.rw.Lock()
  if val, hit := c.lru.Get(key); hit {
    result = val.(*Counter)
    pull = result.SharedExpiry != 0 && result.SharedExpiry <= time.Now().Unix()
  } else {
    result = &Counter{Local: 0, Shared: 0, SharedExpiry: 0}
    c.lru.Add(key, result)
    pull = true
  }
  c.rw.Unlock()
  if pull {
    c.Pull(key, result)
  }
  return
}

/* Atomically increment the local part of a counter. */
func (c *Store) Incr(key string) {
  var counter *Counter
  c.rw.Lock()
  if val, hit := c.lru.Get(key); hit {
    counter = val.(*Counter)
    local := atomic.AddInt64(&counter.Local, 1)
    if local > c.LocalCounterThreshold {
      c.Push(key, counter)
    }
  } else {
    counter = &Counter{Local: 1, Shared: 0, SharedExpiry: 0}
    c.lru.Add(key, counter)
  }
  c.rw.Unlock()
  return
}

/* Updates the shared part of the given counter from the remote store. */
func (c *Store) Pull(key string, counter *Counter) {
  var value int64
  var err error
  if value, err = c.rc.Get(key).Int64(); err == nil {
    counter.Shared = value
    counter.SharedExpiry = time.Now().Unix() + 60
  }
}

/* Pushes the local part of the counter to the remote store.
   The local part is also added to the shared part, and cleared. */
func (c *Store) Push(key string, counter *Counter) {
  if (counter.Local != 0) {
    var err error
    if err = c.rc.IncrBy(key, counter.Local).Err(); err != nil {
      fmt.Printf("IncrBy %d failed on %s\n", counter.Local, key)
      return
    }
    if err = c.rc.Expire(key, c.CounterTtl).Err(); err != nil {
      fmt.Printf("Expire failed on %s\n", key)
      return
    }
    counter.Shared += counter.Local
    counter.Local = 0
  }
}

func (c *Store) Trim(ratio float64) {
  var toFlush int = int(math.Floor(float64(c.lru.MaxEntries) * ratio))
  var targetLen int = c.lru.Len() - toFlush
  if (targetLen < 0) {
    targetLen = 0
  }
  c.rw.Lock()
  defer c.rw.Unlock()
  for c.lru.Len() > targetLen {
    c.lru.RemoveOldest()
  }
  return
}

func NewCounterStore(maxEntries int, redisClient *redis.Client, counterTtl time.Duration, localCounterThreshold int64) (store *Store) {
  lru := &lru.Cache{
    MaxEntries: maxEntries,
    OnEvicted: func (key lru.Key, value interface{}) {
      var strKey = key.(string)
      var counter = value.(*Counter)
      store.Push(strKey, counter);
      return
    },
  }
  store = &Store{
    lru: lru,
    rc: redisClient,
    CounterTtl: counterTtl,
    LocalCounterThreshold: localCounterThreshold,
  }
  return
}
