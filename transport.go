package bebras_guard

import (
  "net"
  "context"
  "net/http"
  "net/url"
  "time"
)

type FixedUnixDialer struct {
  Path string
  net.Dialer
}

func (d *FixedUnixDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
  return d.Dialer.DialContext(ctx, "unix", d.Path);
}

func NoProxy(req *http.Request) (*url.URL, error) {
  return nil, nil
}

func FixedUnixTransport(path string) (*http.Transport) {
  return &http.Transport{
    Proxy: NoProxy,
    DialContext: (&FixedUnixDialer{
      Path: path,
      Dialer: net.Dialer{
        Timeout:   30 * time.Second,
        KeepAlive: 30 * time.Second,
      },
    }).DialContext,
    MaxIdleConns:          100,
    IdleConnTimeout:       90 * time.Second,
    TLSHandshakeTimeout:   10 * time.Second,
    ExpectContinueTimeout: 1 * time.Second,
  }
}
