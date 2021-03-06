package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "strconv"
  "sync"
  "time"
  "sync/atomic"
  "math"
)

type CounterCacheConfig struct {

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
func NewCounterCacheConfig() (CounterCacheConfig) {
  return CounterCacheConfig{
    LocalCacheSize: 65536,
    CounterTtl: 3600,
    LocalMaximum: 10,
    ReloadInterval: 60,
    FlushInterval: 8,
    FlushRatio: 1,
    Debug: false,
    Quiet: false,
  }
}

type CounterCache struct {
  counterTtl time.Duration
  localMaximum int64
  reloadInterval int64
  flushInterval int64
  flushRatio float64
  debug bool
  quiet bool
  rc  *redis.Client
  m   sync.Mutex
  lru *lru.Cache
  ft  *time.Ticker
  ftStop chan bool
}

type Counter struct {
  Local int64
  Shared int64
  ReloadTime int64
}

func (this Counter) Value() (result int64) {
  return this.Local + this.Shared;
}

/* Returns the counter associated with the given key.
   The remote store may be queried to update the shared part of the counter. */
func (this *CounterCache) Get(key string) (result *Counter) {
  /* Add the new counter to the cache if missing */
  var stale bool
  if this.debug {
    fmt.Printf("get %s\n", key)
  }
  this.m.Lock()
  if val, hit := this.lru.Get(key); hit {
    result = val.(*Counter)
    stale = result.ReloadTime <= time.Now().Unix()
    if this.debug {
      fmt.Printf("got %s (%d+%d) %s\n", key, result.Shared, result.Local, staleMsg[stale])
    }
  } else {
    result = &Counter{Local: 0, Shared: 0, ReloadTime: time.Now().Unix()}
    this.lru.Add(key, result)
    if this.debug {
      fmt.Printf("new %s\n", key)
    }
    stale = true
  }
  this.m.Unlock()
  if stale {
    this.pull(key, result)
  }
  return
}

/* Atomically increment the local part of a counter. */
func (this *CounterCache) Incr(key string) {
  var counter *Counter
  this.m.Lock()
  if val, hit := this.lru.Get(key); hit {
    counter = val.(*Counter)
    local := atomic.AddInt64(&counter.Local, 1)
    if local > this.localMaximum {
      this.push(key, counter)
    }
  } else {
    counter = &Counter{Local: 1, Shared: 0, ReloadTime: time.Now().Unix() + this.reloadInterval}
    this.lru.Add(key, counter)
    if 1 > this.localMaximum {
      this.push(key, counter)
    }
  }
  this.m.Unlock()
  return
}

/* Updates the shared part of the given counter from the remote store. */
func (this *CounterCache) pull(key string, counter *Counter) {
  var value int64
  var err error
  if value, err = this.rc.Get(key).Int64(); err == nil {
    counter.Shared = value
    counter.ReloadTime = time.Now().Unix() + this.reloadInterval
    if this.debug {
      fmt.Printf("pulled %s (%d+%d)\n", key, counter.Shared, counter.Local)
    }
  }
}

/* Pushes the local part of the counter to the remote store, if non-zero. */
func (this *CounterCache) Push(key string) {
  var counter *Counter
  this.m.Lock()
  if val, hit := this.lru.Get(key); hit {
    counter = val.(*Counter)
    this.push(key, counter)
  }
  this.m.Unlock()
  return
}

/* Pushes the local part of the counter to the remote store.
   The local part is also added to the shared part, and cleared. */
func (this *CounterCache) push(key string, counter *Counter) {
  if (counter.Local != 0) {
    var err error

    // Update normal counter
    if err = this.rc.IncrBy(key, counter.Local).Err(); err != nil {
      if !this.quiet {
        fmt.Printf("IncrBy %d failed on %s\n", counter.Local, key)
      }
      return
    }
    if err = this.rc.Expire(key, this.counterTtl).Err(); err != nil {
      if !this.quiet {
        fmt.Printf("Expire failed on %s\n", key)
      }
      return
    }
    if this.debug {
      fmt.Printf("pushed %s (%d+%d)\n", key, counter.Shared, counter.Local)
    }

    // Update 10-seconds counter
    var tkey string = "d(" + strconv.FormatInt(time.Now().Unix() / 10, 10) + ")." + key;
    if err = this.rc.IncrBy(tkey, counter.Local).Err(); err != nil {
      if !this.quiet {
        fmt.Printf("IncrBy %d failed on %s\n", counter.Local, tkey)
      }
      return
    }
    if err = this.rc.Expire(tkey, time.Duration(60) * time.Second).Err(); err != nil {
      if !this.quiet {
        fmt.Printf("Expire failed on %s\n", tkey)
      }
      return
    }
    if this.debug {
      fmt.Printf("pushed %s (%d)\n", tkey, counter.Local)
    }

    counter.Shared += counter.Local
    counter.Local = 0
  }
}

func (this *CounterCache) Trim(ratio float64) {
  var toFlush int = int(math.Floor(float64(this.lru.MaxEntries) * ratio))
  var targetLen int = this.lru.Len() - toFlush
  if (targetLen < 0) {
    targetLen = 0
  }
  this.m.Lock()
  defer this.m.Unlock()
  this.trim(targetLen);
  return
}

/* Trim LRU cache to given size. Must be called with the mutex locked. */
func (this *CounterCache) trim(targetLen int) {
  for this.lru.Len() > targetLen {
    this.lru.RemoveOldest()
  }
}

func (this *CounterCache) resize(maxEntries int) {
  /* Flush the old cache. */
  if this.lru != nil {
    this.trim(0);
  }
  /* Allocate the new cache. */
  this.lru = &lru.Cache{
    MaxEntries: maxEntries,
    OnEvicted: func (key lru.Key, value interface{}) {
      var strKey = key.(string)
      var counter = value.(*Counter)
      this.push(strKey, counter);
      return
    },
  }
}

/* Updates the cache's configuration. */
func (this *CounterCache) Configure(config CounterCacheConfig) {
  this.m.Lock()
  defer this.m.Unlock()
  this.counterTtl = time.Duration(config.CounterTtl) * time.Second
  this.localMaximum = config.LocalMaximum
  this.reloadInterval = config.ReloadInterval
  this.flushRatio = config.FlushRatio
  this.debug = config.Debug
  this.quiet = config.Quiet
  /* Resize the cache. */
  if this.lru == nil || this.lru.MaxEntries != config.LocalCacheSize {
    this.resize(config.LocalCacheSize)
  }
  /* Reset the ticker interval. */
  if config.FlushInterval != this.flushInterval {
    this.flushInterval = config.FlushInterval
    if this.ft != nil {
      /* Signal the old goroutine to stop. */
      this.ft.Stop()
      this.ft = nil
      this.ftStop <- true
    }
    if config.FlushInterval > 0 {
      /* Start a ticker and flush goroutine. */
      this.ft = time.NewTicker(time.Duration(config.FlushInterval) * time.Second)
      go func(ticker *time.Ticker) {
        for {
          select {
          case <-ticker.C:
            this.Trim(this.flushRatio)
          case <-this.ftStop:
            return
          }
        }
      }(this.ft)
    }
  }
}

func NewCounterCache(redisClient *redis.Client) (*CounterCache) {
  return &CounterCache{
    rc: redisClient,
    ftStop: make(chan bool, 1),
  }
}
