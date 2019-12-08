package bebras_guard

import (
  "sync"
  "time"
)

// Implements a very simple and approximate leaky bucket algorithm.

// To keep it simple, each request will wait for a duration depending on the
// number of other requests waiting ; and the bucket will leak at regular
// intervals, independently of when the requests arrived.
// That means that the delay between two requests can be anywhere between
// epsilon (if the bucket leaked right between the two requests) and
// 2 * delay - epsilon (if the bucket leaked right before the first request,
// and then right after the second).
// The average request rate will still be the expected one, making it a good
// approximation.


type LeakyBucket struct {
  maxBurst int
  maxWaiting int
  delay time.Duration
  m sync.Mutex
  active bool
  burst map[string]int
  waiting map[string]int
}

type LeakyBucketConfig struct {
  MaxBurst int
  MaxWaiting int
  Delay time.Duration
}

func (this *LeakyBucket) GetSlot(key string) (result bool) {
  // Gets a slot in the bucket, waiting if a slot is not available yet
  this.m.Lock()
  this.active = true
  if this.burst[key] < this.maxBurst {
    this.burst[key]++
    this.m.Unlock()
    result = true
  } else if this.waiting[key] < this.maxWaiting {
    this.waiting[key]++
    this.m.Unlock()
    time.Sleep(time.Duration(this.waiting[key]) * this.delay)
    result = true
  } else {
    this.m.Unlock()
    result = false
  }
  return
}

func (this *LeakyBucket) Leak() {
  // Leak buckets, allowing one more slot to be taken

  // Skip everything if no bucket needs to leak
  if(!this.active) { return; }

  var stillActive bool = false
  for k := range this.burst {
    // Leak for each key
    this.m.Lock()
    if(this.waiting[k] > 0) {
      stillActive = true
      this.waiting[k]--
    } else if(this.burst[k] > 0) {
      delete(this.waiting, k)
      stillActive = true
      this.burst[k]--
    } else {
      delete(this.burst, k)
    }
    this.m.Unlock()
  }
  this.active = stillActive
}

func (this *LeakyBucket) Configure(config LeakyBucketConfig) {
  this.maxBurst = config.MaxBurst
  this.maxWaiting = config.MaxWaiting
  this.delay = config.Delay
}

func (this *LeakyBucket) Run() {
  leakTicker := time.NewTicker(this.delay)
  go func() {
    for {
      select {
      case <-leakTicker.C:
        this.Leak()
      }
    }
  }()
}

func NewLeakyBucket() (*LeakyBucket) {
  // TODO :: proper configuration
  var lc *LeakyBucket = &LeakyBucket{
    maxBurst: 5,
    maxWaiting: 30,
    delay: 100 * time.Millisecond,
    burst: make(map[string]int),
    waiting: make(map[string]int),
  }
  return lc
}
