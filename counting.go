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
  // SharedTtl: redis TTL on shared part of counter
  sharedTtl time.Duration
  // LocalMaximum: maximum local part of counter before push
  localMaximum int64
  // ReloadInterval: maximum duration before reloading the shared part from redis
  reloadInterval int64
  // flushInterval: interval in seconds between automatic partial flush of the cache
  flushInterval int64
  // flushRatio: fraction of the cache flushed every flushInterval seconds
  flushRatio float64
  rc  *redis.Client
  rw  sync.RWMutex
  lru *lru.Cache
  ft  *time.Ticker
  ftStop chan bool
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
   The remote store may be queried to update the shared part of the counter. */
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
    if local > c.localMaximum {
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
    if err = c.rc.Expire(key, c.sharedTtl).Err(); err != nil {
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
  c.trim(targetLen);
  return
}

/* Trim LRU cache to given size. Must be called with the mutex locked. */
func (c *Store) trim(targetLen int) {
  for c.lru.Len() > targetLen {
    c.lru.RemoveOldest()
  }
}

func (c *Store) resize(maxEntries int) {
  /* Flush the old cache. */
  if c.lru != nil {
    c.trim(0);
  }
  /* Allocate the new cache. */
  c.lru = &lru.Cache{
    MaxEntries: maxEntries,
    OnEvicted: func (key lru.Key, value interface{}) {
      var strKey = key.(string)
      var counter = value.(*Counter)
      c.Push(strKey, counter);
      return
    },
  }
}

func (c *Store) Configure() {
  var err error
  var tempInt int64
  var tempFloat float64
  /* Load settings from redis. */
  var localCacheSize int = 65536
  var counterTtl int = 3600
  var localMaximum int64 = 100
  var reloadInterval int64 = 60
  var flushInterval int64 = 60
  var flushRatio float64 = 0.1
  if tempInt, err = c.rc.Get("config.counters.local_cache_size").Int64(); err == nil {
    localCacheSize = int(tempInt)
  }
  if tempInt, err = c.rc.Get("config.counters.ttl").Int64(); err == nil {
    counterTtl = int(tempInt)
  }
  if tempInt, err = c.rc.Get("config.counters.local_maximum").Int64(); err == nil {
    localMaximum = tempInt
  }
  if tempInt, err = c.rc.Get("config.counters.reload_interval").Int64(); err == nil {
    reloadInterval = tempInt
  }
  if tempInt, err = c.rc.Get("config.counters.flush_interval").Int64(); err == nil {
    flushInterval = tempInt
  }
  if tempFloat, err = c.rc.Get("config.counters.flush_ratio").Float64(); err == nil {
    flushRatio = tempFloat
  }
  /* Update settings */
  c.rw.Lock()
  defer c.rw.Unlock()
  c.sharedTtl = time.Duration(counterTtl) * time.Second
  c.localMaximum = localMaximum
  c.reloadInterval = reloadInterval
  c.flushRatio = flushRatio
  /* Resize the cache. */
  if c.lru == nil || c.lru.MaxEntries != localCacheSize {
    c.resize(localCacheSize)
  }
  /* Reset the ticker interval. */
  if flushInterval != c.flushInterval {
    c.flushInterval = flushInterval
    if c.ft != nil {
      /* Signal the old goroutine to stop. */
      c.ft.Stop()
      c.ft = nil
      c.ftStop <- true
    }
    if flushInterval > 0 {
      /* Start a ticker and flush goroutine. */
      c.ft = time.NewTicker(time.Duration(flushInterval) * time.Second)
      go func(ticker *time.Ticker) {
        for {
          select {
          case <-ticker.C:
            c.Trim(c.flushRatio)
          case <-c.ftStop:
            return
          }
        }
      }(c.ft)
    }
  }
}

func NewCounterStore(redisClient *redis.Client) (*Store) {
  return &Store{
    rc: redisClient,
    ftStop: make(chan bool, 1),
  }
}
