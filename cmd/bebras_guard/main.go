package main

import (
//  "container/heap"
  "gopkg.in/redis.v5"
  "log"
  "net/http"
  "net/http/httputil"
  "os"
  "time"
  "os/signal"
  "syscall"
  "strings"
  _ "strconv"
  bg "github.com/France-ioi/bebras-guard"
//  "net/url"
)

type BackendResponse struct {
  clientIp string
  hints string
}

var store *bg.Store

/* Goroutine for handling hints. */

func handleHint(response BackendResponse, hint string) {
  /* Split the hint as path:name */
  parts := strings.Split(hint, `:`)
  if len(parts) > 2 {
    log.Printf("bad hint %s\n", hint)
    return
  }
  var path []string = strings.Split(parts[0], `.`)
  /* Perform tag replacement */
  for tagIndex, tag := range path {
    if tag == "ClientIp" {
      path[tagIndex] = response.clientIp
    }
  }
  /* Build the key and increment the counter */
  parts[0] = strings.Join(path, ".")
  key := strings.Join(parts, ":")
  store.Incr(key)
  /* If a counter name is given, also increment the "total" counter. */
  if len(parts) == 2 {
    parts[1] = "total"
    key = strings.Join(parts, ":")
    store.Incr(key)
  }
  log.Printf("%s: %s\n", response.clientIp, hint)
}
func handleHints(ch chan BackendResponse) {
  for {
    response := <-ch
    hints := strings.Fields(response.hints)
    for _, hint := range hints {
      /* Unquote */
      hint = strings.Trim(hint, `"`)
      handleHint(response, hint)
    }
  }
}

// Reverse proxy

type ProxyTransport struct {
  hintsChannel chan BackendResponse
  backendTransport http.RoundTripper
}

func (t ProxyTransport) RoundTrip(req *http.Request) (res *http.Response, err error) {
  res, err = t.backendTransport.RoundTrip(req)
  if err != nil {
    return
  }
  hints := res.Header.Get("X-Backend-Hints")
  if hints != "" {
    realIp := req.Header.Get("X-Real-IP")
    t.hintsChannel <- BackendResponse{realIp, hints}
  }
  res.Header.Set("X-Guarded", "true")
  return
}

func proxyDirector(req *http.Request) {
  // req.URL.Scheme = "http"
  // req.URL.Host = "127.0.0.1:8001"
  // req.Host = "concours.castor-informatique.fr"
}

//
// Main
//

func main() {
  var err error

  log.Printf("bebras-guard is starting\n")

  redisClient := redis.NewClient(&redis.Options{
      Addr:     os.Getenv("REDIS_SERVER"),
      Password: "",
      DB:       0,
  })

  /* Build the counter store.
     config.max_counters sets the size of the local counters LRU cache (default 64k).
     config.counter_ttl sets the redis TTL on shared counters, in seconds (default 1h).
  */
  var tempVal int64
  var maxCounters int = 65536
  if tempVal, err = redisClient.Get("config.max_counters").Int64(); err == nil {
    maxCounters = int(tempVal)
  }
  var counterTtl time.Duration = 1 * time.Hour
  if tempVal, err = redisClient.Get("config.counter_ttl").Int64(); err == nil {
    counterTtl = time.Duration(tempVal) * time.Second
  }
  var localCounterThreshold int64 = 100
  if tempVal, err = redisClient.Get("config.local_counter_threshold").Int64(); err == nil {
    localCounterThreshold = tempVal
  }
  store = bg.NewCounterStore(maxCounters, redisClient, counterTtl * time.Second, localCounterThreshold)
  log.Printf("store{MaxCounters: %d, CounterTtl: %d, LocalCounterThreshold: %d}\n",
    maxCounters, int(counterTtl / time.Second), localCounterThreshold)

  // Catch SIGINT, save store to redis
  quitChannel := make(chan os.Signal, 1)
  // SIGINT is sent when by Ctrl-C.
  // SIGHUP is sent by runit for graceful shutdown.
  signal.Notify(quitChannel, syscall.SIGINT) // Ctrl-C
  signal.Notify(quitChannel, syscall.SIGHUP)
  go func(){
    <-quitChannel
    log.Printf("flushing store...")
    store.Trim(0)
    log.Printf(" done\n")
    os.Exit(0)
  }()

  /* Periodically trim 10% of the counters. */
  flushTicker := time.NewTicker(60 * time.Second)
  go func() {
    for {
      <-flushTicker.C
      store.Trim(0.1)
    }
  }()

  /* Buffer a number of responses without blocking. */
  var responseQueueSize int = 128
  if tempVal, err = redisClient.Get("config.response_queue_size").Int64(); err != nil {
    responseQueueSize = int(tempVal)
  }
  hintsChannel := make(chan BackendResponse, responseQueueSize)
  go handleHints(hintsChannel)

  /* Select the backend transport. */
  var backendTransport http.RoundTripper
  if backendSocket := os.Getenv("BACKEND_SOCKET"); backendSocket != "" {
    backendTransport = bg.FixedUnixTransport(backendSocket)
  } else {
    backendTransport = http.DefaultTransport
  }

  /* Start the reverse proxy. */
  proxyTransport := &ProxyTransport{
    hintsChannel: hintsChannel,
    backendTransport: backendTransport,
  }
  proxy := &httputil.ReverseProxy{
    Director: proxyDirector,
    Transport: proxyTransport,
  }
  var listen string = os.Getenv("LISTEN")
  if listen == "" {
    listen = ":80"
  }
  log.Printf("starting proxy on %s\n", listen)
  err = http.ListenAndServe(listen, proxy)
  if err != nil {
    log.Printf("fatal: %s\n", err)
  }
}
