
# Installation

  go get github.com/France-ioi/bebras-guard
  go install github.com/France-ioi/bebras-guard/cmd/bebras_guard

# Configuration

Configuration is done via environment variables and redis.

Environment variables:

    REDIS_SERVER
    LISTEN
    BACKEND_SOCKET
    BACKEND_HOST
    FORCE_HTTP_HOST

Redis keys:

    config.max_counters
    config.counter_ttl
    config.local_counter_threshold
    config.flush_interval
    config.flush_ratio
    config.response_queue_size
