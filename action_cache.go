package bebras_guard

import (
  "fmt"
  "github.com/golang/groupcache/lru"
  "gopkg.in/redis.v5"
  "sync"
  "time"
)

type ActionCacheConfig struct {

  // Maximum number of entries in the LRU cache.
  MaxEntries int

  // Maximum duration before reloading entries from redis.
  ReloadInterval int64

  // Print debugging messages?
  Debug bool

}

/* Returns default Config. */
func NewActionCacheConfig() (ActionCacheConfig) {
  return ActionCacheConfig{
    MaxEntries: 65536,
    ReloadInterval: 10,
    Debug: false,
  }
}

type Action struct {
  Block bool  // block requests
  RateLimit bool // rate-limit requests
  Quick bool  // skip maintaining counters
  ReloadTime int64
}

type PullRequest struct {
  key string
  action *Action
}

type ActionCache struct {
  reloadInterval int64
  debug bool
  rc *redis.Client
  m sync.Mutex
  lru *lru.Cache
  pullChannel chan PullRequest
}

/* Returns the action associated with the given key. */
func (this *ActionCache) Get(key string) (result *Action) {
  /* Add the new action to the cache if missing */
  var stale bool
  if this.debug {
    fmt.Printf("get action %s\n", key)
  }
  this.m.Lock()
  if val, hit := this.lru.Get(key); hit {
    result = val.(*Action)
    stale = result.ReloadTime <= time.Now().Unix()
    if this.debug {
      fmt.Printf("got action %s %s %s %s %s\n", key,
        blockMsg[result.Block], rateMsg[result.RateLimit], quickMsg[result.Quick], staleMsg[stale])
    }
  } else {
    result = &Action{Block: false, RateLimit: false, Quick: false, ReloadTime: time.Now().Unix() + this.reloadInterval}
    this.lru.Add(key, result)
    if this.debug {
      fmt.Printf("new %s\n", key)
    }
    stale = true
  }
  this.m.Unlock()
  if stale {
    // Update action from redis
    // Note : we update ReloadTime so other threads don't try to update it too
    result.ReloadTime = time.Now().Unix() + this.reloadInterval
    this.pullChannel <- PullRequest{key, result}
  }
  return
}

/* Load the action from redis. */
func (this *ActionCache) pull(key string, action *Action) {
  var value string
  var err error
  if value, err = this.rc.Get(key).Result(); err != nil {
    /* Redis failed to respond, assume empty value. */
    value = ""
  }
  switch value {
  case "b": // blacklist
    action.Block = true
    action.RateLimit = false
    action.Quick = false
  case "r": // ratelimit
    action.Block = false
    action.RateLimit = true
    action.Quick = false
  case "W": // whitelist + quick
    action.Block = false
    action.RateLimit = false
    action.Quick = true
  default: // no data, whitelist, ...
    action.Block = false
    action.RateLimit = false
    action.Quick = false
  }
  action.ReloadTime = time.Now().Unix() + this.reloadInterval
  if this.debug {
    fmt.Printf("pulled action %s [%s] %s %s\n", key, value,
      blockMsg[action.Block], quickMsg[action.Quick])
  }
}

/* Updates the cache's configuration. */
func (this *ActionCache) Configure(config ActionCacheConfig) {
  this.m.Lock()
  defer this.m.Unlock()
  this.reloadInterval = config.ReloadInterval
  this.debug = config.Debug
  if config.MaxEntries > 0 {
    // Trim the cache as needed.
    for this.lru.Len() > config.MaxEntries {
      this.lru.RemoveOldest()
    }
  }
  this.lru.MaxEntries = config.MaxEntries
}

func NewActionCache(redisClient *redis.Client) (*ActionCache) {
  var pullChannelSize int = 128
  pullChannel := make(chan PullRequest, pullChannelSize)
  var ac *ActionCache = &ActionCache{
    rc: redisClient,
    lru: &lru.Cache{},
    pullChannel: pullChannel,
  }
  go func() {
    for {
      pull := <-pullChannel
      ac.pull(pull.key, pull.action)
    }
  }()
  return ac
}
