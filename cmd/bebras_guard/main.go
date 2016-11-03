package main

import (
//  "container/heap"
  "gopkg.in/redis.v5"
  "log"
  "net"
  "net/http"
  "net/http/httputil"
  "os"
  "os/signal"
  "syscall"
  "strings"
  "encoding/hex"
  bg "github.com/France-ioi/bebras-guard"
)

type BackendResponse struct {
  clientIp string
  hints string
}

/* Goroutine for handling hints. */

func handleHint(store *bg.Store, response BackendResponse, hint string) {
  /* Split the hint as path:name */
  parts := strings.Split(hint, `:`)
  if len(parts) > 2 {
    log.Printf("bad hint %s\n", hint)
    return
  }
  var path []string = strings.Split(parts[0], `.`)
  /* Perform tag replacement */
  for tagIndex, tag := range path {
    if tag == "ClientIp" || tag == "ClientIP" {
      ip := net.ParseIP(response.clientIp)
      /* ParseIP always returns an IPv6 address, try to convert to IPv4 */
      if ip4 := ip.To4(); ip4 != nil {
        ip = ip4
      }
      path[tagIndex] = "IP(" + hex.EncodeToString(ip) + ")"
    }
  }
  /* Build the key and increment the counter */
  parts[0] = strings.Join(path, ".")
  key := "c." + strings.Join(parts, ".")
  store.Get(key)
  store.Incr(key)
  /* If a counter name is given, also increment the "total" counter. */
  if len(parts) == 2 {
    parts[1] = "total"
    key = "c." + strings.Join(parts, ".")
    store.Get(key)
    store.Incr(key)
  }
  log.Printf("hint %s %s\n", response.clientIp, hint)
}

func handleHints(store *bg.Store, ch chan BackendResponse) {
  for {
    response := <-ch
    hints := strings.Fields(response.hints)
    for _, hint := range hints {
      /* Unquote */
      hint = strings.Trim(hint, `"`)
      handleHint(store, response, hint)
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
    /* Normally we run behind a load-balancer which will set X-Real-IP. */
    realIp := req.Header.Get("X-Real-IP")
    if realIp == "" {
      /* When testing locally, the X-Real-IP header is missing. */
      colonIndex := strings.LastIndex(req.RemoteAddr, `:`)
      realIp = req.RemoteAddr[0:colonIndex]
      realIp = strings.Trim(realIp, `[]`)
    }
    t.hintsChannel <- BackendResponse{realIp, hints}
  }
  res.Header.Set("X-Guarded", "true")
  return
}

//
// Main
//

/* Loads and returns Config from redis. */
func LoadStoreConfig(rc *redis.Client) (bg.StoreConfig) {
  var err error
  var tempInt int64
  var tempFloat float64
  s := bg.NewStoreConfig()
  if tempInt, err = rc.Get("config.counters.local_cache_size").Int64(); err == nil {
    s.LocalCacheSize = int(tempInt)
  }
  if tempInt, err = rc.Get("config.counters.ttl").Int64(); err == nil {
    s.CounterTtl = int(tempInt)
  }
  if tempInt, err = rc.Get("config.counters.local_maximum").Int64(); err == nil {
    s.LocalMaximum = tempInt
  }
  if tempInt, err = rc.Get("config.counters.reload_interval").Int64(); err == nil {
    s.ReloadInterval = tempInt
  }
  if tempInt, err = rc.Get("config.counters.flush_interval").Int64(); err == nil {
    s.FlushInterval = tempInt
  }
  if tempFloat, err = rc.Get("config.counters.flush_ratio").Float64(); err == nil {
    s.FlushRatio = tempFloat
  }
  return s
}

func main() {
  var err error
  var tempInt int64

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

  /* Build and configure the counter store. */
  var store *bg.Store = bg.NewCounterStore(redisClient)
  store.Configure(LoadStoreConfig(redisClient))

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

  /* Buffer a number of responses without blocking. */
  var responseQueueSize int = 128
  if tempInt, err = redisClient.Get("config.response_queue_size").Int64(); err != nil {
    responseQueueSize = int(tempInt)
  }
  hintsChannel := make(chan BackendResponse, responseQueueSize)
  go handleHints(store, hintsChannel)

  /* Select the backend transport and director. */
  var backendTransport http.RoundTripper
  var backendHost string
  forceHttpHost := os.Getenv("FORCE_HTTP_HOST")
  if backendSocket := os.Getenv("BACKEND_SOCKET"); backendSocket != "" {
    backendTransport = bg.FixedUnixTransport(backendSocket)
    backendHost = "unix:" + backendSocket // (ignored)
  } else {
    backendTransport = http.DefaultTransport
    backendHost = os.Getenv("BACKEND_HOST")
  }
  proxyDirector := func (req *http.Request) {
    req.URL.Scheme = "http"
    req.URL.Host = backendHost
    if forceHttpHost != "" {
      req.Host = forceHttpHost
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
