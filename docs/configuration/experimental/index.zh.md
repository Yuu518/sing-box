# 实验性

!!! quote "sing-box 1.8.0 中的更改"

    :material-plus: [cache_file](#cache_file)  
    :material-alert-decagram: [clash_api](#clash_api)

### 结构

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

### 字段

| 键                       | 格式                        |
|-------------------------|---------------------------|
| `cache_file`            | [缓存文件](./cache-file/)     |
| `clash_api`             | [Clash API](./clash-api/) |
| `v2ray_api`             | [V2Ray API](./v2ray-api/) |
| `urltest_unified_delay` | 布尔                        |

#### urltest_unified_delay

在同一连接上执行两次 URL 测试请求并采用第二次的耗时，以排除握手延迟。

如果测试目标不保持连接，将回退为使用第一次的耗时。

默认禁用。
