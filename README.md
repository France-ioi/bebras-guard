
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

    config.response_queue_size
    config.counters.local_cache_size
    config.counters.ttl
    config.counters.local_maximum
    config.counters.reload_interval
    config.counters.flush_interval
    config.counters.flush_ratio
    config.counters.debug
    config.counters.quiet
    config.activity_cache.max_entries
    config.activity_cache.threshold
    config.activity_cache.debug
    config.action_cache.max_entries
    config.action_cache.reload_interval
    config.action_cache.debug
