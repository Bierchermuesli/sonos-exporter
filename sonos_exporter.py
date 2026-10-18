#!/usr/bin/env python3
"""
Sonos Prometheus Exporter
-------------------------
Features:
 - RX/TX bytes, packets, errors, drops
 - PHY errors, noise floor, channels, WiFi mode, device up
 - Link quality (FROM / TO)
 - OFDM ANI level
 - MAC→Zone mapping for human-readable topology
 - Optional ICMP probe metrics (loss/latency) for all Sonos IPs and extra targets
 - Optional battery metrics when LocalBatteryStatus is present
 - Player uptime, exporter scrape health
 - Threaded scrape + ICMP
 - Custom HTTP server with:
     * /metrics        - Prometheus metrics
     * /health         - healthcheck (for Docker)
     * /metrics/health - additional health endpoint
     * /               - simple ok page
"""

import re
import time
import logging
import socket
import struct
import select
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
import xml.etree.ElementTree as ET
from prometheus_client import (
    Gauge,
    REGISTRY,
    generate_latest,
    CONTENT_TYPE_LATEST,
)


class SonosExporter:
    def __init__(
        self,
        sonos_ip,
        interval=60,
        icmp_interval=30,
        extra_icmp_targets=None,
    ):
        self.sonos_ip = sonos_ip
        self.interval = interval
        # 0 disables ICMP entirely; otherwise never probe slower than we scrape
        self.icmp_interval = min(icmp_interval, interval) if icmp_interval > 0 else 0
        self.extra_icmp_targets = extra_icmp_targets or []

        self.logger = logging.getLogger("sonos_exporter")
        self.zone_ip_map = {}
        self._map_lock = threading.Lock()

        # Track mutable label sets so stale entries can be removed from the registry
        self._known_busy_states = {}  # zone -> set of state label values
        self._known_wifi_modes = {}   # zone -> mode_string label value

        # --- Core interface counters ---
        self.rx_bytes = Gauge("sonos_rx_bytes", "Received bytes", ["zone", "interface"])
        self.tx_bytes = Gauge("sonos_tx_bytes", "Transmitted bytes", ["zone", "interface"])
        self.rx_packets = Gauge("sonos_rx_packets", "Received packets", ["zone", "interface"])
        self.tx_packets = Gauge("sonos_tx_packets", "Transmitted packets", ["zone", "interface"])
        self.rx_errors = Gauge("sonos_rx_errors", "Receive errors", ["zone", "interface"])
        self.tx_errors = Gauge("sonos_tx_errors", "Transmit errors", ["zone", "interface"])
        self.rx_dropped = Gauge("sonos_rx_dropped", "Receive drops", ["zone", "interface"])
        self.tx_dropped = Gauge("sonos_tx_dropped", "Transmit drops", ["zone", "interface"])

        # --- Radio / PHY ---
        self.phy_errors = Gauge("sonos_phy_errors", "PHY errors", ["zone"])
        self.noise_floor = Gauge("sonos_noise_floor_dbm", "Noise floor", ["zone"])
        self.ofdm_ani = Gauge("sonos_ofdm_ani_level", "OFDM ANI", ["zone"])
        self.operating_channel = Gauge("sonos_operating_channel", "Channel", ["zone"])
        self.ieee_channel = Gauge("sonos_ieee_channel", "IEEE channel", ["zone"])
        self.wifi_mode = Gauge("sonos_wifi_mode", "WifiMode value", ["zone", "mode_string"])
        self.idle_state = Gauge("sonos_idle_state", "IdleState value", ["zone"])
        self.busy_state = Gauge("sonos_busy_state", "BusyClient flags", ["zone", "state"])

        # --- Ethernet ---
        self.enet_link = Gauge("sonos_enet_connected", "Ethernet link state (0=down, 1=up)", ["zone", "port"])
        self.enet_speed = Gauge("sonos_enet_speed_mbps", "Ethernet speed", ["zone", "port"])

        # --- Advanced Wifi/Radio ---
        self.ht_channel = Gauge("sonos_ht_channel", "High Throughput (HT) IEEE Channel", ["zone"])
        self.ht_ap_mode = Gauge("sonos_ht_ap_mode", "HT AP Mode (0=off, 1=on)", ["zone"])
        self.rf_chains = Gauge("sonos_rf_chains", "RF Chains count", ["zone", "direction"])  # direction=rx|tx

        # --- Bridge / STP ---
        self.bridge_stp_enabled = Gauge("sonos_bridge_stp_enabled", "Bridge STP enabled", ["zone"])

        # Device up (overridden by ICMP if enabled)
        self.device_up = Gauge("sonos_device_up", "Reachable (ICMP if enabled)", ["zone"])
        self.uptime = Gauge("sonos_uptime_seconds", "Seconds since the player booted", ["zone"])

        # --- Device Info ---
        self.device_info = Gauge(
            "sonos_device_info",
            "Device metadata",
            [
                "zone",
                "serial_number",
                "software_version",
                "sw_gen",
                "ip_address",
                "mac_address",
                "hardware_version",
                "series_id",
                "local_uid",
            ],
        )

        # --- Battery metrics ---
        self.battery_level = Gauge("sonos_battery_level_percent", "Battery %", ["zone"])
        self.battery_info = Gauge(
            "sonos_battery_info",
            "Battery metadata",
            ["zone", "health", "temperature", "power_source"],
        )

        # --- SonosNet link quality ---
        self.link_metric = Gauge(
            "sonos_link_connection",
            "SonosNet links",
            ["zone", "peer_zone", "from_node", "to_node", "stp", "model", "key"],
        )
        self.link_from = Gauge(
            "sonos_link_quality_from", "FROM link quality", ["zone", "peer_zone", "model"]
        )
        self.link_to = Gauge(
            "sonos_link_quality_to", "TO link quality", ["zone", "peer_zone", "model"]
        )

        # --- Exporter health ---
        self.scrape_success = Gauge(
            "sonos_scrape_success", "1 if the last /support/review scrape succeeded"
        )
        self.scrape_duration = Gauge(
            "sonos_scrape_duration_seconds", "Duration of the last scrape"
        )
        self.scrape_last_success = Gauge(
            "sonos_scrape_last_success_timestamp_seconds",
            "Unix timestamp of the last successful scrape",
        )

        # --- ICMP-related metrics ---
        # No icmp_up exposed: device_up is driven by ICMP if enabled
        self.icmp_loss = Gauge("icmp_loss", "Loss ratio", ["zone", "ip"])
        self.icmp_latency = Gauge("icmp_latency_ms", "Latency ms", ["zone", "ip"])
        self.icmp_last_success = Gauge(
            "sonos_icmp_last_success_timestamp",
            "Unix timestamp of last successful ICMP response",
            ["zone", "ip"],
        )

        self._icmp_use_raw = None
        if self.icmp_interval > 0:
            self._detect_icmp_mode()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _detect_icmp_mode(self):
        """Decide once whether to use unprivileged DGRAM or raw ICMP sockets.

        Sets self._icmp_use_raw to False (DGRAM), True (RAW) or None (neither
        available; probes will fail until permissions change).
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.getprotobyname("icmp"))
            s.close()
            self._icmp_use_raw = False
            return
        except OSError as e:
            dgram_err = e
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.getprotobyname("icmp"))
            s.close()
            self._icmp_use_raw = True
            self.logger.info(
                "Unprivileged ICMP (SOCK_DGRAM) not available (%s); using raw socket "
                "(CAP_NET_RAW). To avoid root: "
                "sysctl -w net.ipv4.ping_group_range='0 65535'",
                dgram_err,
            )
        except OSError as e:
            self._icmp_use_raw = None
            self.logger.warning(
                "ICMP probes will fail: neither SOCK_DGRAM ICMP (%s, needs ping_group_range) "
                "nor SOCK_RAW (%s, needs CAP_NET_RAW) is accessible.",
                dgram_err,
                e,
            )

    def _open_icmp_socket(self):
        """Open an ICMP socket in the mode chosen by _detect_icmp_mode().

        Returns (sock, use_raw). If no mode was available at startup, retry
        detection so a later permission change is picked up without restart.
        """
        if self._icmp_use_raw is None:
            self._detect_icmp_mode()
            if self._icmp_use_raw is None:
                raise PermissionError("no ICMP socket type accessible")
        sock_type = socket.SOCK_RAW if self._icmp_use_raw else socket.SOCK_DGRAM
        sock = socket.socket(socket.AF_INET, sock_type, socket.getprotobyname("icmp"))
        return sock, self._icmp_use_raw

    @staticmethod
    def _strip_ns(tag):
        return tag.split("}", 1)[-1] if "}" in tag else tag

    @staticmethod
    def _find_text(element, tag):
        for child in element.iter():
            if SonosExporter._strip_ns(child.tag).lower() == tag.lower():
                if child.text:
                    return child.text.strip()
        return None

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------
    def parse_ifconfig(self, text, zone, mac_map):
        """Extract RX/TX metrics from /sbin/ifconfig and record MACs"""
        blocks = re.split(r"\n(?=\S)", text.strip())
        for b in blocks:
            m = re.match(r"(\S+)\s+Link encap", b)
            if not m:
                continue
            iface = m.group(1)
            mac_match = re.search(r"HWaddr\s+([0-9A-F:]{17})", b, re.I)
            if mac_match:
                mac = mac_match.group(1).upper()
                mac_map[mac] = zone

            rx_bytes = re.search(r"RX bytes:(\d+)", b)
            tx_bytes = re.search(r"TX bytes:(\d+)", b)
            rx_packets = re.search(r"RX packets:(\d+)", b)
            tx_packets = re.search(r"TX packets:(\d+)", b)
            rx_errors = re.search(r"RX packets:\d+ errors:(\d+)", b)
            tx_errors = re.search(r"TX packets:\d+ errors:(\d+)", b)
            rx_dropped = re.search(r"RX packets:\d+ errors:\d+ dropped:(\d+)", b)
            tx_dropped = re.search(r"TX packets:\d+ errors:\d+ dropped:(\d+)", b)

            if rx_bytes:
                self.rx_bytes.labels(zone, iface).set(float(rx_bytes.group(1)))
            if tx_bytes:
                self.tx_bytes.labels(zone, iface).set(float(tx_bytes.group(1)))
            if rx_packets:
                self.rx_packets.labels(zone, iface).set(float(rx_packets.group(1)))
            if tx_packets:
                self.tx_packets.labels(zone, iface).set(float(tx_packets.group(1)))
            if rx_errors:
                self.rx_errors.labels(zone, iface).set(float(rx_errors.group(1)))
            if tx_errors:
                self.tx_errors.labels(zone, iface).set(float(tx_errors.group(1)))
            if rx_dropped:
                self.rx_dropped.labels(zone, iface).set(float(rx_dropped.group(1)))
            if tx_dropped:
                self.tx_dropped.labels(zone, iface).set(float(tx_dropped.group(1)))

    def parse_proc_status(self, text, zone, mac_map, seen_links):
        """Extract PHY, channel, noise floor, OFDM ANI, link quality, HT info, RF chains"""
        ch = re.search(r"Operating on channel\s+(\d+)", text)
        ieee = re.search(r"IEEE channel:\s*(\d+)", text)
        phy = re.search(r"PHY errors.*?:\s*(\d+)", text)
        noise = [int(n) for n in re.findall(r"Noise Floor:\s*(-?\d+)", text)]
        ofdm = re.search(r"OFDM ANI level:\s*(\d+)", text)
        # "Debug info for INFRA mode at N": N advances once per second since boot
        uptime = re.search(r"INFRA mode at\s+(\d+)", text)
        if uptime:
            self.uptime.labels(zone).set(float(uptime.group(1)))

        # HT Channel (multiline check)
        # Look for HT Channel line, then capture the IEEE channel below it
        ht_match = re.search(r"HT Channel.*?\n.*?IEEE channel:\s*(\d+)", text, re.DOTALL | re.IGNORECASE)
        if ht_match:
             self.ht_channel.labels(zone).set(float(ht_match.group(1)))

        # HT AP Mode
        ht_ap = re.search(r"HT AP mode:\s*(on|off)", text, re.IGNORECASE)
        if ht_ap:
            val = 1 if ht_ap.group(1).lower() == "on" else 0
            self.ht_ap_mode.labels(zone).set(val)

        # RF Chains: "RF Chains: RX:4 TX:4"
        rf = re.search(r"RF Chains:\s*RX:(\d+)\s+TX:(\d+)", text, re.IGNORECASE)
        if rf:
            self.rf_chains.labels(zone, "rx").set(float(rf.group(1)))
            self.rf_chains.labels(zone, "tx").set(float(rf.group(2)))

        if ch:
            self.operating_channel.labels(zone).set(float(ch.group(1)))
        if ieee:
            self.ieee_channel.labels(zone).set(float(ieee.group(1)))
        if phy:
            self.phy_errors.labels(zone).set(float(phy.group(1)))
        # Newer (MediaTek based) players report 0 dBm on every chain, meaning
        # "not available" rather than a real noise floor; skip those.
        if noise and any(noise):
            avg_noise = sum(noise) / len(noise)
            self.noise_floor.labels(zone).set(avg_noise)
        if ofdm:
            self.ofdm_ani.labels(zone).set(float(ofdm.group(1)))

        self.parse_sonosnet_links(text, zone, mac_map, seen_links)

    def parse_sonosnet_links(self, text, zone, mac_map, seen_links):
        """Parse Node … FROM … TO … STP … MODEL … KEY … with zone mapping.

        Deduplicates bidirectional pairs via seen_links (mutated in-place).
        """
        pattern = re.compile(
            r"Node\s+([0-9A-F:]{17})\s+-\s+FROM\s+(\d+)\s+:\s+TO\s+(\d+)\s+:\s+STP\s+(\S+)\s+:\s+MODEL\s+([\d.]+):\s+KEY\s+(\d+)",
            re.I,
        )
        for m in pattern.finditer(text):
            mac, from_n, to_n, stp, model, key = m.groups()
            peer_zone = mac_map.get(mac.upper(), mac)

            # Skip the reverse direction of an already-emitted pair
            key_pair = tuple(sorted([zone, peer_zone])) + (from_n, to_n)
            if key_pair in seen_links:
                continue
            seen_links.add(key_pair)

            self.link_metric.labels(
                zone=zone,
                peer_zone=peer_zone,
                from_node=from_n,
                to_node=to_n,
                stp=stp,
                model=model,
                key=key,
            ).set(1.0)
            self.link_from.labels(zone, peer_zone, model).set(float(from_n))
            self.link_to.labels(zone, peer_zone, model).set(float(to_n))

    def parse_wireless(self, elem, zone):
        """WiFi mode, IdleState, BusyClients"""
        # WifiMode — remove stale label set if mode_string changed
        mode = self._find_text(elem, "WifiMode")
        mode_str = self._find_text(elem, "WifiModeString") or ""
        if mode:
            try:
                old_mode_str = self._known_wifi_modes.get(zone)
                if old_mode_str is not None and old_mode_str != mode_str:
                    try:
                        self.wifi_mode.remove(zone, old_mode_str)
                    except Exception:
                        pass
                self.wifi_mode.labels(zone, mode_str).set(int(mode))
                self._known_wifi_modes[zone] = mode_str
            except ValueError:
                pass

        # IdleState
        idle = self._find_text(elem, "IdleState")
        if idle:
            try:
                # Handle hex strings like '0x123'
                val = int(idle, 0)
                self.idle_state.labels(zone).set(val)
            except ValueError:
                pass

        # BusyClients — remove states that are no longer present
        busy = self._find_text(elem, "BusyClients")
        new_states = set()
        if busy:
            for item in (x.strip() for x in busy.split(",") if x.strip()):
                self.busy_state.labels(zone, item).set(1)
                new_states.add(item)
        for stale in self._known_busy_states.get(zone, set()) - new_states:
            try:
                self.busy_state.remove(zone, stale)
            except Exception:
                pass
        self._known_busy_states[zone] = new_states

    def parse_battery(self, zp, zone):
        for bs in zp.findall(".//LocalBatteryStatus"):
            fields = {}
            for d in bs.findall(".//Data"):
                name = d.attrib.get("name")
                if name and d.text:
                    fields[name] = d.text.strip()
            if "Level" in fields:
                try:
                    self.battery_level.labels(zone).set(float(fields["Level"]))
                except Exception:
                    self.logger.debug(
                        "Non-numeric battery level for zone %s: %s", zone, fields["Level"]
                    )
            # Only emit battery_info when at least one metadata field is present
            if any(k in fields for k in ("Health", "Temperature", "PowerSource")):
                self.battery_info.labels(
                    zone,
                    fields.get("Health", "unknown"),
                    fields.get("Temperature", "unknown"),
                    fields.get("PowerSource", "unknown"),
                ).set(1)

    # ------------------------------------------------------------------
    # Scraper
    # ------------------------------------------------------------------
    def scrape_once(self):
        start = time.time()
        try:
            self._scrape()
        except Exception as e:
            self.logger.error("Scrape failed: %s", e)
            self.scrape_success.set(0)
        else:
            self.scrape_success.set(1)
            self.scrape_last_success.set(time.time())
        finally:
            self.scrape_duration.set(time.time() - start)

    def _scrape(self):
        url = f"http://{self.sonos_ip}:1400/support/review"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        # Use r.content (bytes) so ElementTree honours the XML declaration's
        # encoding instead of relying on requests' charset detection.
        root = ET.fromstring(r.content)

        zones = 0
        mac_map = {}
        zp_nodes = list(root.findall(".//ZPSupportInfo"))

        # 1st pass: build MAC→zone map
        explicit_macs = []
        for zp in zp_nodes:
            zone = self._find_text(zp, "ZoneName") or "unknown"
            for cmd in zp.findall(".//Command"):
                if "ifconfig" in cmd.attrib.get("cmdline", ""):
                    text = cmd.text or ""
                    for mac in re.findall(r"HWaddr\s+([0-9A-F:]{17})", text, re.I):
                        m = mac.upper()
                        mac_map[m] = zone
                        explicit_macs.append((m, zone))
            for f in zp.findall(".//File"):
                if "ifconfig" in f.attrib.get("name", ""):
                    text = f.text or ""
                    for mac in re.findall(r"HWaddr\s+([0-9A-F:]{17})", text, re.I):
                        m = mac.upper()
                        mac_map[m] = zone
                        explicit_macs.append((m, zone))

        # Infer MAC+1 for each explicit MAC (e.g. 5GHz radio often +1 but not in ifconfig)
        for mac, zone in explicit_macs:
            try:
                mac_val = int(mac.replace(":", ""), 16)
                next_mac_str = "{:012X}".format(mac_val + 1)
                next_mac_pretty = ":".join(next_mac_str[i:i+2] for i in range(0, 12, 2))
                if next_mac_pretty not in mac_map:
                    mac_map[next_mac_pretty] = zone
            except Exception:
                pass

        # 2nd pass: parse metrics & build zone->ip map
        zone_ip_map = {}
        seen_links = set()
        for zp in zp_nodes:
            zone = self._find_text(zp, "ZoneName") or "unknown"
            ip_addr = self._find_text(zp, "IPAddress") or None
            if ip_addr:
                zone_ip_map[zone] = ip_addr

            zones += 1
            # Only set device_up from HTTP reachability when ICMP is disabled;
            # otherwise the ICMP thread is authoritative to avoid brief stale values.
            if self.icmp_interval == 0:
                self.device_up.labels(zone).set(1.0)

            self.device_info.labels(
                zone=zone,
                serial_number=self._find_text(zp, "SerialNumber") or "",
                software_version=self._find_text(zp, "SoftwareVersion") or "",
                sw_gen=self._find_text(zp, "SWGen") or "",
                ip_address=ip_addr or "",
                mac_address=self._find_text(zp, "MACAddress") or "",
                hardware_version=self._find_text(zp, "HardwareVersion") or "",
                series_id=self._find_text(zp, "SeriesID") or "",
                local_uid=self._find_text(zp, "LocalUID") or "",
            ).set(1)

            # Enet
            enet = zp.find(".//EnetPorts")
            if enet is not None:
                for p in enet.findall("Port"):
                    port_idx = p.attrib.get("port", "0")
                    link = self._find_text(p, "Link") == "1"
                    speed_str = self._find_text(p, "Speed") or "0"
                    try:
                        speed = int(speed_str)
                    except ValueError:
                        speed = 0
                    self.enet_link.labels(zone, port_idx).set(1 if link else 0)
                    self.enet_speed.labels(zone, port_idx).set(speed)

            for w in zp.findall(".//Wireless"):
                self.parse_wireless(w, zone)

            self.parse_battery(zp, zone)

            for cmd in zp.findall(".//Command"):
                cmdline = cmd.attrib.get("cmdline", "")
                text = cmd.text or ""
                if "ifconfig" in cmdline:
                    self.parse_ifconfig(text, zone, mac_map)
                elif "ath_rincon/status" in cmdline:
                    self.parse_proc_status(text, zone, mac_map, seen_links)
                elif "brctl showstp" in cmdline:
                    if "STP is disabled" in text:
                        self.bridge_stp_enabled.labels(zone).set(0)
                    else:
                        self.bridge_stp_enabled.labels(zone).set(1)

            for f in zp.findall(".//File"):
                fname = f.attrib.get("name", "")
                text = f.text or ""
                if "ath_rincon/status" in fname:
                    self.parse_proc_status(text, zone, mac_map, seen_links)

        # Update shared zone->ip map for ICMP thread
        with self._map_lock:
            self.zone_ip_map = zone_ip_map

        self.logger.info(
            "Scraped %d zones, MAC map size=%d, links=%d, zone_ip_map=%d",
            zones,
            len(mac_map),
            len(seen_links),
            len(zone_ip_map),
        )

    # ------------------------------------------------------------------
    # ICMP helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _icmp_checksum(source: bytes) -> int:
        if len(source) % 2:
            source += b"\x00"
        s = 0
        for i in range(0, len(source), 2):
            s += (source[i] << 8) + source[i + 1]
        s = (s >> 16) + (s & 0xFFFF)
        s += s >> 16
        return ~s & 0xFFFF

    def _icmp_echo(self, dest_addr, timeout=1, seq=1, ident=0x1234):
        try:
            sock, use_raw = self._open_icmp_socket()
        except OSError as e:
            self.logger.error("ICMP socket error: %s", e)
            return None

        try:
            sock.settimeout(timeout)
            dest_ip = socket.gethostbyname(dest_addr)

            if not use_raw:
                # On Linux ping sockets the kernel overwrites the echo identifier
                # with the socket's local "port". Bind to our ident so replies
                # carry it; if that fails (in use), we read back whatever the
                # kernel assigned after sending.
                try:
                    sock.bind(("", ident))
                except OSError:
                    pass

            header = struct.pack("!BBHHH", 8, 0, 0, ident, seq)
            data = struct.pack("!d", time.time())
            chksum = self._icmp_checksum(header + data)
            header = struct.pack("!BBHHH", 8, 0, chksum, ident, seq)
            packet = header + data

            send_time = time.time()
            sock.sendto(packet, (dest_ip, 1))

            expect_id = ident
            if not use_raw:
                # Autobind happens on send; the assigned port is the echo id.
                expect_id = sock.getsockname()[1] or ident

            # Loop until we get our matching reply or the deadline passes.
            # Raw sockets receive all ICMP traffic on the host, so non-matching
            # packets must be skipped rather than causing a false loss reading.
            deadline = send_time + timeout
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                if not select.select([sock], [], [], remaining)[0]:
                    return None

                recv_packet, addr = sock.recvfrom(1024)
                recv_time = time.time()

                if addr[0] != dest_ip:
                    self.logger.debug(
                        "ICMP %s: skipping packet from unexpected source %s", dest_addr, addr[0]
                    )
                    continue

                # RAW sockets always include the IP header. DGRAM sockets strip
                # it on Linux but not on macOS/BSD, so detect it by the version
                # nibble (an ICMP echo reply starts with type 0, never 0x4X).
                if recv_packet and (use_raw or (recv_packet[0] >> 4) == 4):
                    ip_header_len = (recv_packet[0] & 0x0F) * 4
                    icmp_data = recv_packet[ip_header_len:]
                else:
                    icmp_data = recv_packet

                if len(icmp_data) < 8:
                    self.logger.debug("ICMP %s: packet too short (%d bytes)", dest_addr, len(icmp_data))
                    continue

                icmp_type, _code, _checksum, packet_id, sequence = struct.unpack(
                    "!BBHHH", icmp_data[:8]
                )

                if icmp_type == 0 and packet_id == expect_id and sequence == seq:
                    return (recv_time - send_time) * 1000.0

                self.logger.debug(
                    "ICMP %s: skipping type=%d id=%d seq=%d (want type=0 id=%d seq=%d)",
                    dest_addr, icmp_type, packet_id, sequence, expect_id, seq,
                )
        except socket.timeout:
            return None
        except Exception as e:
            self.logger.debug("ICMP to %s failed: %s", dest_addr, e)
            return None
        finally:
            sock.close()

    def _get_icmp_targets(self):
        with self._map_lock:
            targets = list(self.zone_ip_map.items())
        for ip in self.extra_icmp_targets:
            targets.append(("custom", ip))
        return targets

    def icmp_thread(self):
        self.logger.info("ICMP thread started")
        seq = 1
        ident = (int(time.time()) & 0xFFFF) or 1

        while True:
            start = time.time()
            for zone, ip in self._get_icmp_targets():
                sent, received = 3, 0
                rtts = []
                for _ in range(3):
                    rtt = self._icmp_echo(ip, timeout=1, seq=seq, ident=ident)
                    # Wrap at 65535; avoid seq=0 as some hosts ignore it
                    seq = (seq + 1) & 0xFFFF or 1
                    if rtt is not None:
                        received += 1
                        rtts.append(rtt)
                    time.sleep(0.2)

                up = received > 0
                loss = (sent - received) / sent
                latency = sum(rtts) / len(rtts) if rtts else 0.0
                self.logger.debug(
                    "ICMP %s %s: sent=%d, received=%d, loss=%.2f, latency=%.2fms",
                    zone, ip, sent, received, loss, latency,
                )

                # drive device_up when ICMP enabled (for real Sonos zones)
                if zone != "custom":
                    self.device_up.labels(zone).set(1.0 if up else 0.0)

                self.icmp_loss.labels(zone, ip).set(loss)
                self.icmp_latency.labels(zone, ip).set(latency)
                if up:
                    self.icmp_last_success.labels(zone, ip).set(time.time())

            sleep_time = max(0, self.icmp_interval - (time.time() - start))
            time.sleep(sleep_time)

    # ------------------------------------------------------------------
    # Threaded scrape
    # ------------------------------------------------------------------
    def scrape_thread(self):
        self.logger.info("Scrape thread started")
        while True:
            start = time.time()
            self.scrape_once()
            sleep_time = max(0, self.interval - (time.time() - start))
            time.sleep(sleep_time)

    def run(self):
        # Start scrape thread
        threading.Thread(target=self.scrape_thread, daemon=True).start()
        # Start ICMP thread
        if self.icmp_interval > 0:
            threading.Thread(target=self.icmp_thread, daemon=True).start()
        # Main thread idle
        while True:
            time.sleep(10)


# ----------------------------------------------------------------------
# HTTP server with /metrics and /health
# ----------------------------------------------------------------------
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health", "/metrics/health"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok\n")
        elif self.path.startswith("/metrics"):
            output = generate_latest(REGISTRY)
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Avoid noisy default logging; route via logging module instead
        logging.getLogger("sonos_exporter.http").info(
            "%s - - %s", self.address_string(), format % args
        )


def start_http_server_with_health(port):
    server = ThreadingHTTPServer(("", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logging.info(f"HTTP server started on port {port} with /metrics and /health")
    return server


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sonos Prometheus Exporter (threaded)")
    parser.add_argument("--sonos-ip", required=True)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--port", type=int, default=9814)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--icmp-interval", type=int, default=30, help="set 0 to disable ICMP metrics",)
    parser.add_argument("--icmp-target", action="append", default=[])

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    exporter = SonosExporter(
        args.sonos_ip,
        interval=args.interval,
        icmp_interval=args.icmp_interval,
        extra_icmp_targets=args.icmp_target,
    )

    # Start HTTP server with /metrics and /health
    start_http_server_with_health(args.port)

    exporter.run()
