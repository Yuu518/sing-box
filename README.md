# sing-box

The universal proxy platform.

[![Packaging status](https://repology.org/badge/vertical-allrepos/sing-box.svg)](https://repology.org/project/sing-box/versions)

## Documentation

https://sing-box.sagernet.org

## Inbound TLS

```json
{
  "inbounds": [
    {
      "type": "trojan",
      "tag": "trojan-in",
      "tls": {
        "enabled": true,
        "server_name": "sekai.love",
        "certificate_path": "cert.pem",
        "key_path": "key.key",
        "reject_unknown_sni": true
      }
    },
    {
      "type": "anytls",
      "tag": "anytls-in",
      "tls": {
        "enabled": true,
        "server_names": [
          "sagernet.sekai.love",
          "sekai.love"
        ],
        "certificate_path": "cert.pem",
        "key_path": "key.key",
        "reject_unknown_sni": true
      }
    }
  ]
}
```

Reject unknown SNI: If the server name of connection does not match `server_name` or any domain in `server_names`,
and is not included in the certificate, it will be rejected.

拒绝未知 SNI：如果连接的 server name 与 `server_name` 或者 `server_names` 中包含的域名 不符 且 证书中不包含它，则拒绝连接。

## License

```
Copyright (C) 2022 by nekohasekai <contact-sagernet@sekai.icu>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.

In addition, no derivative work may use the name or imply association
with this application without prior consent.
```