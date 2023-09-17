### 结构

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

### 字段

显式配置的字段覆盖订阅原值。未配置的字段（包括 `ech`、`utls` 和 `reality` 内的字段）保留原值。`enabled: true` 也能为没有 TLS 配置的节点开启 TLS；`enabled: false` 关闭 TLS。

支持的字段：`enabled`、`disable_sni`、`server_name`、`insecure`、`alpn`、`min_version`、`max_version`、`cipher_suites`、`curve_preferences`、`certificate`、`certificate_path`、`certificate_public_key_sha256`、`certificate_pin_sha256`、`client_certificate`、`client_certificate_path`、`client_key`、`client_key_path`、`fragment`、`fragment_fallback_delay`、`record_fragment`、`kernel_tx`、`kernel_rx`、`ech`、`utls` 和 `reality`。详情参阅 [TLS 字段](/zh/configuration/shared/tls/#outbound)。

当前 TLS 选项不支持 `certificate_server_name`。
