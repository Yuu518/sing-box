### Structure

```json
{
  "enabled": true,
  "disable_sni": false,
  "server_name": "example.com",
  "insecure": false,
  "kernel_tx": false,
  "kernel_rx": false
}
```

### Fields

Explicitly configured fields override the subscription. Omitted fields, including fields inside `ech`, `utls`, and `reality`, preserve their original values. `enabled: true` also enables TLS for nodes without a TLS configuration; `enabled: false` disables TLS.

Supported fields: `enabled`, `disable_sni`, `server_name`, `insecure`, `alpn`, `min_version`, `max_version`, `cipher_suites`, `curve_preferences`, `certificate`, `certificate_path`, `certificate_public_key_sha256`, `certificate_pin_sha256`, `client_certificate`, `client_certificate_path`, `client_key`, `client_key_path`, `fragment`, `fragment_fallback_delay`, `record_fragment`, `kernel_tx`, `kernel_rx`, `ech`, `utls`, and `reality`. See [TLS Fields](/configuration/shared/tls/#outbound).

`certificate_server_name` is not supported by the current TLS options.
