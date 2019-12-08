package bebras_guard

import (
  "sync"
  "fmt"
)

// Implements a queue with simple QoS slot attribution

// When a request arrives :
// * if we have less than maxActive requests active, just let it through
// * otherwise, the request gets a queue number depending on the client past
// number of requests
// With that queue number, the queue will let through requests so that each
// client gets one request each, then another request each, etc.
// When we fall under maxActive, everything is reset to free up resources.


type QosQueue struct {
  maxActive int
  maxClients int64
  maxRequestsBeforeNextStep int

  fastMode bool
  active int

  clientIds map[string]int64
  nextId int64
  clientSteps map[string]int64
  curStep int64
  waiting map[int64]chan struct{}

  m sync.Mutex

  releaseChan chan struct{}
}

func (this *QosQueue) ResetSlow() {
  // Reset internal state
  this.clientIds = make(map[string]int64)
  this.clientSteps = make(map[string]int64)
  this.waiting = make(map[int64]chan struct{})
  this.nextId = 0
  this.curStep = 0
}

func (this *QosQueue) GetSlot(key string) {
  // Gets a slot in the queue, waiting if required by QoS
  this.m.Lock()
  if this.active < this.maxActive {
    // Fast mode : we're within maxActive
    this.active++
    this.m.Unlock()
    return
  }

  // Slow mode : we need to wait
  this.fastMode = false
  var cid int64 = this.clientIds[key]
  if cid == 0 {
    // Get a new clientId
    this.nextId++
    cid = this.nextId
    this.clientIds[key] = cid
  }
  var cs int64 = this.clientSteps[key]
  if cs < this.curStep {
    cs = this.curStep
  }
  this.clientSteps[key] = cs + 1

  // Create a waiting channel
  waitChan := make(chan struct{}, 1)
  // Take a spot in the waiting map, with a priority number ; this priority
  // number
  this.waiting[cs * this.maxClients + cid] = waitChan
  this.m.Unlock()

  // Now wait for the supervisor to assign us a slot
  <-waitChan
}

func (this *QosQueue) ReleaseSlot() {
  // Signals that we finished getting a response from the backend
  this.releaseChan <- struct{}{}
}

func (this *QosQueue) releaseSlotLoop() {
  var nbRequestsSinceLastStep int
  for {
    <-this.releaseChan
    this.m.Lock()
    if this.fastMode {
      // We're in fast mode, no request waiting
      this.active--
      this.m.Unlock()
      continue
    }

    // We're in slow mode, select request to wake up
    var minId int64 = 0
    for id := range this.waiting {
      if id < minId || minId == 0 {
        minId = id
      }
    }
    if minId == 0 {
      // There were no more requests waiting
      this.ResetSlow()

      this.fastMode = true
      this.active--
      this.m.Unlock()
      continue
    }

    // Tell the request it can proceed
    waitChan := this.waiting[minId]
    waitChan <- struct{}{}
    delete(this.waiting, minId)

    // Move steps
    minIdStep := minId / this.maxClients
    if minIdStep > this.curStep {
      this.curStep = minIdStep
      nbRequestsSinceLastStep = 0
    } else {
      nbRequestsSinceLastStep++
      if nbRequestsSinceLastStep > this.maxRequestsBeforeNextStep {
        this.curStep++
        nbRequestsSinceLastStep = 0
      }
    }

    // We're done for now
    this.m.Unlock()
  }
}

func NewQosQueue() (*QosQueue) {
  // TODO :: proper configuration
  releaseChan := make(chan struct{}, 256)
  var qq *QosQueue = &QosQueue{
    maxActive: 255,
    maxClients: 1 << 31,
    maxRequestsBeforeNextStep: 100,
    releaseChan: releaseChan,
  }
  qq.ResetSlow()
  go qq.releaseSlotLoop()
  return qq
}
