package main

import (
//  "container/heap"
  "gopkg.in/redis.v5"
  "log"
  "net"
  "net/http"
  "net/http/httputil"
  "os"
  "time"
  "os/signal"
  "syscall"
  "strings"
  _ "strconv"
  "encoding/hex"
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
      ip := net.ParseIP(response.clientIp)
      path[tagIndex] = hex.EncodeToString(ip)
    }
  }
  /* Build the key and increment the counter */
  parts[0] = strings.Join(path, ".")
  key := "c." + strings.Join(parts, ".")
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

//
// Main
//

func main() {
  var err error

  log.Printf("bebras-guard is starting\n")

  redisAddr := os.Getenv("REDIS_SERVER")
  if redisAddr == "" {
    redisAddr = "127.0.0.1:6379"
  }
  redisClient := redis.NewClient(&redis.Options{
      Addr:     redisAddr,
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
  var counterTtl int = 3600
  if tempVal, err = redisClient.Get("config.counter_ttl").Int64(); err == nil {
    counterTtl = int(tempVal)
  }
  var localCounterThreshold int64 = 100
  if tempVal, err = redisClient.Get("config.local_counter_threshold").Int64(); err == nil {
    localCounterThreshold = tempVal
  }
  store = bg.NewCounterStore(maxCounters, redisClient, time.Duration(counterTtl) * time.Second, localCounterThreshold)
  log.Printf("store{MaxCounters: %d, CounterTtl: %d, LocalCounterThreshold: %d}\n",
    maxCounters, counterTtl, localCounterThreshold)

  /* Add signal handlers to flush the store on exit. */
  quitChannel := make(chan os.Signal, 1)
  signal.Notify(quitChannel, syscall.SIGINT)  // Ctrl-C.
  signal.Notify(quitChannel, syscall.SIGHUP)  // runit graceful shutdown
  go func(){
    <-quitChannel
    log.Printf("flushing store...")
    store.Trim(1)
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

  /* Select the backend transport and director. */
  var proxyDirector func (req *http.Request)
  var backendTransport http.RoundTripper
  forceHttpHost := os.Getenv("FOCRCE_HTTP_HOST")
  if backendSocket := os.Getenv("BACKEND_SOCKET"); backendSocket != "" {
    backendTransport = bg.FixedUnixTransport(backendSocket)
    proxyDirector = func (req *http.Request) {
      if forceHttpHost != "" {
        req.Host = forceHttpHost
      }
    }
  } else {
    backendTransport = http.DefaultTransport
    backendHost := os.Getenv("BACKEND_HOST")
    proxyDirector = func (req *http.Request) {
      req.URL.Scheme = "http"
      req.URL.Host = backendHost
      if forceHttpHost != "" {
        req.Host = forceHttpHost
      }
      req.Host = "concours.castor-informatique.fr"
    }
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
