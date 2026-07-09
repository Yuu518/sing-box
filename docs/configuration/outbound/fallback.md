### Structure

```json
{
  "type": "fallback",
  "tag": "fallback",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "url": "",
  "interval": "",
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

Selects the first available outbound in listed order. When the currently selected outbound is detected as unavailable, it switches to the next available one immediately; when a higher-priority outbound recovers, it falls back automatically.

Unlike `urltest`, latency never influences selection: the URL test only determines whether an outbound is available.

### Fields

#### outbounds

==Required==

List of outbound tags, in priority order.

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

#### interval

The test interval. `3m` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
