### 结构

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

按列表顺序选择第一个可用的出站。当检测到当前出站不可用时，立即切换到下一个可用出站；当排位更靠前的出站恢复时，自动回退。

与 `urltest` 不同，延迟不影响选择：URL 测试仅用于判定出站是否可用。

### 字段

#### outbounds

==必填==

按优先级排序的出站标签列表。

#### url

用于测试的链接。默认使用 `https://www.gstatic.com/generate_204`。

#### interval

测试间隔。 默认使用 `3m`。

#### idle_timeout

空闲超时。默认使用 `30m`。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
