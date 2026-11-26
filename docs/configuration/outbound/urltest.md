### Structure

```json
{
  "type": "urltest",
  "tag": "auto",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "url": "",
  "interval": "",
  "tolerance": 0,
  "idle_timeout": "",
  "interrupt_exist_connections": false,
  "fallback": {
    "enabled": false,
    "max_delay": ""
  }
}
```

### Fields

#### outbounds

==Required==

List of outbound tags to test.

#### url

The URL to test. `https://www.gstatic.com/generate_204` will be used if empty.

#### interval

The test interval. `3m` will be used if empty.

#### tolerance

The test tolerance in milliseconds. `50` will be used if empty.

#### idle_timeout

The idle timeout. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections when the selected outbound has changed.

Only inbound connections are affected by this setting, internal connections will always be interrupted.

#### fallback

Fallback mode settings.

##### enabled

Enable fallback mode. Disabled by default.

When enabled, outbounds are selected in the order listed in `outbounds` instead of by lowest delay, falling back to subsequent outbounds when earlier ones become unavailable and switching back as soon as a higher-priority outbound recovers.

##### max_delay

Maximum acceptable delay when fallback mode is enabled. No limit if empty.

Outbounds with a delay greater than this value are skipped; if all available outbounds exceed this value, the one with the lowest delay is selected.
