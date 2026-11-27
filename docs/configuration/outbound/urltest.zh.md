### 结构

```json
{
  "type": "urltest",
  "tag": "auto",
  
  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "providers": [
    "provider-a",
    "provider-b",
  ],
  "exclude": "",
  "include": "",
  "use_all_providers": false,
  "url": "",
  "interval": "",
  "tolerance": 50,
  "idle_timeout": "",
  "interrupt_exist_connections": false,
  "fallback": {
    "enabled": false,
    "max_delay": ""
  }
}
```

### 字段

#### outbounds

用于测试的出站标签列表。

#### providers

用于测试的[订阅](/zh/configuration/provider)标签列表。

#### exclude

排除 `providers` 节点的正则表达式。

#### include

包含 `providers` 节点的正则表达式。

#### use_all_providers

是否使用所有提供者。默认使用 `false`。

#### url

用于测试的链接。默认使用 `https://www.gstatic.com/generate_204`。

#### interval

测试间隔。 默认使用 `3m`。

#### tolerance

以毫秒为单位的测试容差。 默认使用 `50`。

#### idle_timeout

空闲超时。默认使用 `30m`。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。

#### fallback

故障转移（fallback）模式配置。

##### enabled

启用故障转移模式。默认关闭。

开启后将按 `outbounds` 列表顺序优先选择首个可用出站，当前出站不可用时自动按顺序切换至后续出站，高优先级出站恢复后自动切回。

##### max_delay

开启故障转移模式后可接受的最大延迟，留空时不限制。

延迟超过该值的出站会被跳过；如果所有可用出站均超过该值，则选择其中延迟最低的出站。
