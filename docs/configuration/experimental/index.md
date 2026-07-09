# Experimental

!!! quote "Changes in sing-box 1.8.0"

    :material-plus: [cache_file](#cache_file)  
    :material-alert-decagram: [clash_api](#clash_api)

### Structure

```json
{
  "experimental": {
    "cache_file": {},
    "clash_api": {},
    "v2ray_api": {},
    "urltest_unified_delay": false
  }
}
```

### Fields

| Key                     | Format                      |
|-------------------------|-----------------------------|
| `cache_file`            | [Cache File](./cache-file/) |
| `clash_api`             | [Clash API](./clash-api/)   |
| `v2ray_api`             | [V2Ray API](./v2ray-api/)   |
| `urltest_unified_delay` | Boolean                     |

#### urltest_unified_delay

Perform the URL test request twice over the same connection and use the second measurement, excluding handshake latency from the result.

If the test target does not keep the connection alive, the first measurement is used instead.

Disabled by default.
