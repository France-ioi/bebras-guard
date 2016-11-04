package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "sync"
  "time"
  "sync/atomic"
  "math"
)

type StoreConfig struct {

  // Size of the local LRU cache.
  LocalCacheSize int

  // redis TTL on shared part of counter.
  CounterTtl int

  // Maximum local part of counter before a push occurs.
  LocalMaximum int64

  // Maximum duration before reloading the shared part from redis.
  ReloadInterval int64

  // Interval in seconds between automatic partial flush of the cache.
  FlushInterval int64

  // Fraction of the cache flushed every flushInterval seconds.
  FlushRatio float64

  // Print debugging messages?
  Debug bool

  // Hide all messages? (such as redis errors)
  Quiet bool

}

/* Returns default Config. */
func NewStoreConfig() (StoreConfig) {
  return StoreConfig{
    LocalCacheSize: 65536,
    CounterTtl: 3600,
    LocalMaximum: 100,
    ReloadInterval: 60,
    FlushInterval: 60,
    FlushRatio: 0.1,
    Debug: false,
    Quiet: false,
  }
}

type Store struct {
  counterTtl time.Duration
  localMaximum int64
  reloadInterval int64
  flushInterval int64
  flushRatio float64
  debug bool
  quiet bool
  rc  *redis.Client
  rw  sync.RWMutex
  lru *lru.Cache
  ft  *time.Ticker
  ftStop chan bool
}

type Counter struct {
  Local int64
  Shared int64
  ReloadTime int64
}

func (c Counter) Value() (result int64) {
  return c.Local + c.Shared;
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
  if c.debug {
    fmt.Printf("get %s\n", key)
  }
  c.rw.Lock()
  if val, hit := c.lru.Get(key); hit {
    result = val.(*Counter)
    pull = result.ReloadTime <= time.Now().Unix()
    if c.debug {
      fmt.Printf("got %s (%d+%d) %s\n", key, result.Shared, result.Local, pull)
    }
  } else {
    result = &Counter{Local: 0, Shared: 0, ReloadTime: time.Now().Unix()}
    c.lru.Add(key, result)
    if c.debug {
      fmt.Printf("new %s\n", key)
    }
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
    counter = &Counter{Local: 1, Shared: 0, ReloadTime: time.Now().Unix() + c.reloadInterval}
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
    counter.ReloadTime = time.Now().Unix() + c.reloadInterval
    if c.debug {
      fmt.Printf("pulled %s (%d+%d)\n", key, counter.Shared, counter.Local)
    }
  }
}

/* Pushes the local part of the counter to the remote store.
   The local part is also added to the shared part, and cleared. */
func (c *Store) Push(key string, counter *Counter) {
  if (counter.Local != 0) {
    var err error
    if err = c.rc.IncrBy(key, counter.Local).Err(); err != nil {
      if !c.quiet {
        fmt.Printf("IncrBy %d failed on %s\n", counter.Local, key)
      }
      return
    }
    if err = c.rc.Expire(key, c.counterTtl).Err(); err != nil {
      if !c.quiet {
        fmt.Printf("Expire failed on %s\n", key)
      }
      return
    }
    if c.debug {
      fmt.Printf("pushed %s (%d+%d)\n", key, counter.Shared, counter.Local)
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

/* Updates the store's configuration. */
func (c *Store) Configure(config StoreConfig) {
  c.rw.Lock()
  defer c.rw.Unlock()
  c.counterTtl = time.Duration(config.CounterTtl) * time.Second
  c.localMaximum = config.LocalMaximum
  c.reloadInterval = config.ReloadInterval
  c.flushRatio = config.FlushRatio
  c.debug = config.Debug
  c.quiet = config.Quiet
  /* Resize the cache. */
  if c.lru == nil || c.lru.MaxEntries != config.LocalCacheSize {
    c.resize(config.LocalCacheSize)
  }
  /* Reset the ticker interval. */
  if config.FlushInterval != c.flushInterval {
    c.flushInterval = config.FlushInterval
    if c.ft != nil {
      /* Signal the old goroutine to stop. */
      c.ft.Stop()
      c.ft = nil
      c.ftStop <- true
    }
    if config.FlushInterval > 0 {
      /* Start a ticker and flush goroutine. */
      c.ft = time.NewTicker(time.Duration(config.FlushInterval) * time.Second)
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
