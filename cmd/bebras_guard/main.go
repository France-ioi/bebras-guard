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
  "unicode"
  "unicode/utf8"
  "encoding/hex"
  "bytes"
  "time"
  "fmt"
  bg "github.com/France-ioi/bebras-guard"
)

type BackendResponse struct {
  realIp string
  hexIp string
  hints string
}

/* Goroutine for handling hints. */

type ResponseHandler struct {
  redis   *redis.Client
  counters *bg.CounterCache
  activity *bg.ActivityCache
}

type TagSet map[string]struct{}

func (this ResponseHandler) handleHint(clientIpTag string, tagSet TagSet, hint string) {
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
      path[tagIndex] = clientIpTag
    } else {
      r, _ := utf8.DecodeRuneInString(tag)
      if (unicode.IsUpper(r)) {
        tagSet[tag] = struct{}{}
      }
    }
  }
  /* Build the key and increment the counter */
  parts[0] = strings.Join(path, ".")
  key := "c." + strings.Join(parts, ".")
  this.counters.Get(key)
  this.counters.Incr(key)
  /* If a counter name is given, also increment the "total" counter. */
  if len(parts) == 2 {
    parts[1] = "total"
    key = "c." + strings.Join(parts, ".")
    this.counters.Get(key)
    this.counters.Incr(key)
  }
}

func unquote(value string) (result string) {
  return strings.Trim(value, `"`)
}

func (this ResponseHandler) Run(ch chan BackendResponse) {
  for {
    response := <-ch
    clientIpTag := "IP(" + response.hexIp + ")"
    tagSet := make(TagSet)
    hints := strings.Fields(response.hints)
    for _, hint := range hints {
      this.handleHint(clientIpTag, tagSet, unquote(hint))
    }
    this.activity.Bump(clientIpTag)
    for tag, _ := range tagSet {
      this.activity.Bump(tag)
    }
  }
}

// Helper class to send static body responses

type ClosingBuffer struct {
  *bytes.Buffer
}

func (cb *ClosingBuffer) Close() (error) {
  return nil
}

// Reverse proxy

type ProxyTransport struct {
  responseChannel chan BackendResponse
  backendTransport http.RoundTripper
  actions *bg.ActionCache
}

func plainTextResponse(statusCode int, body string) (*http.Response) {
  res := &http.Response{
    Proto:      "HTTP/1.1",
    ProtoMajor: 1,
    ProtoMinor: 1,
    Header:     make(http.Header),
    StatusCode: statusCode,
    Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
    Body:       &ClosingBuffer{bytes.NewBufferString(body)},
  }
  res.Header.Set("Content-Type", "text/plain")
  return res
}

func (this ProxyTransport) RoundTrip(req *http.Request) (res *http.Response, err error) {
  /* Normally we run behind a load-balancer which will set X-Real-IP. */
  realIp := req.Header.Get("X-Real-IP")
  if realIp == "" {
    /* When testing locally, the X-Real-IP header is missing. */
    colonIndex := strings.LastIndex(req.RemoteAddr, `:`)
    realIp = req.RemoteAddr[0:colonIndex]
    realIp = strings.Trim(realIp, `[]`)
  }
  /* Convert the IP-address to HEX representation for use in keys.
     ParseIP always returns an IPv6 address, try to convert to IPv4. */
  parsedIp := net.ParseIP(realIp)
  if parsedIp4 := parsedIp.To4(); parsedIp4 != nil {
    parsedIp = parsedIp4
  }
  hexIp := hex.EncodeToString(parsedIp)
  /* Look up in the action cache */
  action := this.actions.Get("a." + hexIp)
  if action.Block {
    res = plainTextResponse(429, "too many requests")
    err = nil
    return
  }
  /* IP request */
  if req.URL.Path == "/ip" {
    res = plainTextResponse(200, realIp)
    err = nil
    return
  }
  /* Pass the request to the backend */
  res, err = this.backendTransport.RoundTrip(req)
  // res.Header.Set("X-Guarded", "true")
  if err != nil {
    return
  }
  /* Process the hints header, unless the Quick flag is set */
  if !action.Quick {
    hints := res.Header.Get("X-Backend-Hints")
    if hints != "" {
      this.responseChannel <- BackendResponse{realIp, hexIp, hints}
    }
  }
  return
}

//
// Main
//

func LoadCounterCacheConfig(c *bg.ConfigStore) (bg.CounterCacheConfig) {
  s := bg.NewCounterCacheConfig()
  c.GetInt("counters.local_cache_size", &s.LocalCacheSize)
  c.GetInt("counters.ttl", &s.CounterTtl)
  c.GetInt64("counters.local_maximum", &s.LocalMaximum)
  c.GetInt64("counters.reload_interval", &s.ReloadInterval)
  c.GetInt64("counters.flush_interval", &s.FlushInterval)
  c.GetFloat64("counters.flush_ratio", &s.FlushRatio)
  c.GetBool("counters.debug", &s.Debug)
  c.GetBool("counters.quiet", &s.Quiet)
  return s
}

func LoadActivityCacheConfig(c *bg.ConfigStore) (out bg.ActivityCacheConfig) {
  out = bg.NewActivityCacheConfig()
  c.GetInt("activity_cache.max_entries", &out.MaxEntries)
  c.GetInt64("activity_cache.threshold", &out.Threshold)
  c.GetBool("activity_cache.debug", &out.Debug)
  return
}

func LoadActionCacheConfig(c *bg.ConfigStore) (out bg.ActionCacheConfig) {
  out = bg.NewActionCacheConfig()
  c.GetInt("action_cache.max_entries", &out.MaxEntries)
  c.GetInt64("action_cache.reload_interval", &out.ReloadInterval)
  c.GetBool("action_cache.debug", &out.Debug)
  return
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
    DialTimeout: 1 * time.Second,
  })

  config := bg.NewConfigStore(redisClient)

  /* Build and configure the caches. */
  var counterCache *bg.CounterCache = bg.NewCounterCache(redisClient)
  var activityCache *bg.ActivityCache = bg.NewActivityCache(redisClient)
  var actionCache *bg.ActionCache = bg.NewActionCache(redisClient)
  reconfigure := func() {
    counterCache.Configure(LoadCounterCacheConfig(config))
    activityCache.Configure(LoadActivityCacheConfig(config))
    actionCache.Configure(LoadActionCacheConfig(config))
  }
  reconfigure()

  /* Reload the configuration every 60 seconds */
  reconfigTicker := time.NewTicker(60 * time.Second)
  go func() {
    for {
      select {
      case <-reconfigTicker.C:
        reconfigure()
      }
    }
  }()

  /* Add signal handlers to flush the counter cache on exit. */
  quitChannel := make(chan os.Signal, 1)
  signal.Notify(quitChannel, syscall.SIGINT)  // Ctrl-C.
  signal.Notify(quitChannel, syscall.SIGHUP)  // runit graceful shutdown
  go func(){
    <-quitChannel
    log.Printf("flushing counters...")
    counterCache.Trim(1)
    log.Printf(" done\n")
    os.Exit(0)
  }()

  /* Buffer a number of responses without blocking. */
  var responseQueueSize int = 128
  if tempInt, err = redisClient.Get("config.response_queue_size").Int64(); err != nil {
    responseQueueSize = int(tempInt)
  }
  responseChannel := make(chan BackendResponse, responseQueueSize)
  responseHandler := ResponseHandler{
    redis: redisClient,
    counters: counterCache,
    activity: activityCache,
  }
  go responseHandler.Run(responseChannel)

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
    responseChannel: responseChannel,
    backendTransport: backendTransport,
    actions: actionCache,
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
