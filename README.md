# sonos-exporter

Prometheus exporter for the network health of a Sonos household. It scrapes
the undocumented `/support/review` page that every Sonos player serves on
port 1400 and turns the SonosNet mesh details into metrics: per-interface
traffic counters, WiFi mode and radio state, mesh link quality between
players, STP state, battery levels, and optional ICMP reachability probes.

Point it at one player and it discovers the rest of the household from that
dump. Zone names are used as labels, so a `Kitchen` speaker shows up as
`zone="Kitchen"` instead of a MAC address.

## Running

With Docker Compose:

```sh
SONOS_IP=192.168.1.50 docker compose up -d
```

Directly:

```sh
pip install -r requirements.txt
python sonos_exporter.py --sonos-ip 192.168.1.50
```

Metrics are served on `http://localhost:9814/metrics`, a health check on
`/health`.

| Flag | Default | Meaning |
|---|---|---|
| `--sonos-ip` | required | IP of any player in the household |
| `--interval` | 60 | Scrape interval in seconds |
| `--port` | 9814 | Listen port |
| `--icmp-interval` | 30 | ICMP probe interval in seconds, 0 disables ICMP |
| `--icmp-target` | | Extra IP to ping, repeatable |
| `--log-level` | INFO | Python log level |

## ICMP without root

The exporter prefers unprivileged ICMP datagram sockets and only falls back to
raw sockets when those are unavailable. Unprivileged ICMP needs the kernel to
allow it for the exporter's group:

```sh
sysctl -w net.ipv4.ping_group_range='0 2147483647'
```

The Compose file sets this sysctl for the container and additionally grants
`NET_RAW` as a fallback. If ping sockets work on your host you can drop the
`cap_add` block. With `--icmp-interval 0` no socket is needed at all and
`sonos_device_up` is derived from the HTTP scrape instead.

## Metrics

All metrics carry a `zone` label. The most useful ones:

| Metric | Labels | Description |
|---|---|---|
| `sonos_device_up` | | 1 if reachable (ICMP when enabled, else HTTP) |
| `sonos_device_info` | serial, versions, ip, mac, ... | Always 1, carries device metadata |
| `sonos_rx_bytes`, `sonos_tx_bytes`, `..._packets`, `..._errors`, `..._dropped` | interface | Interface counters |
| `sonos_wifi_mode` | mode_string | Station, SonosNet satellite, ... |
| `sonos_idle_state`, `sonos_busy_state` | state | Radio idle/busy flags |
| `sonos_operating_channel`, `sonos_ieee_channel`, `sonos_ht_channel`, `sonos_ht_ap_mode` | | Channel details |
| `sonos_rf_chains` | direction | RX/TX chain count |
| `sonos_noise_floor_dbm`, `sonos_phy_errors`, `sonos_ofdm_ani_level` | | Radio quality |
| `sonos_link_connection` | peer_zone, model, stp, ... | One entry per SonosNet mesh link |
| `sonos_link_quality_from`, `sonos_link_quality_to` | peer_zone, model | Link quality per direction |
| `sonos_bridge_stp_enabled` | | Bridge STP state |
| `sonos_enet_connected`, `sonos_enet_speed_mbps` | port | Ethernet port state |
| `sonos_battery_level_percent`, `sonos_battery_info` | health, temperature, power_source | Portable players only |
| `icmp_loss`, `icmp_latency_ms`, `sonos_icmp_last_success_timestamp` | ip | ICMP probe results |

## Tests

```sh
python -m unittest discover -s tests -t .
```

The fixtures in `tests/fixtures` are real support dumps with MAC addresses,
IPs and household IDs replaced by documentation ranges.

## License

MIT, see [LICENSE](LICENSE).
