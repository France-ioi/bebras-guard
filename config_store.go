package bebras_guard

import (
  "gopkg.in/redis.v5"
)

type ConfigStore struct {
  r *redis.Client
}

func (c ConfigStore) GetInt(key string, result *int) {
  if val, err := c.r.Get("config." + key).Int64(); err == nil {
    *result = int(val)
  }
}

func (c ConfigStore) GetInt64(key string, result *int64) {
  if val, err := c.r.Get("config." + key).Int64(); err == nil {
    *result = val
  }
}

func (c ConfigStore) GetFloat64(key string, result *float64) {
  if val, err := c.r.Get("config." + key).Float64(); err == nil {
    *result = val
  }
}

func (c ConfigStore) GetBool(key string, result *bool) {
  if val, err := c.r.Get("config." + key).Result(); err == nil {
    *result = val == "true"
  }
}

func NewConfigStore(r *redis.Client) (*ConfigStore) {
  return &ConfigStore{r: r}
}
