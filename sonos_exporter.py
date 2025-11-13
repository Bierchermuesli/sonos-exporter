#!/usr/bin/env python3
"""
Features:
 - RX/TX bytes, packets, errors
 - PHY errors, noise floor, channels, WiFi mode, device up
 - Link quality (FROM / TO)
 - OFDM ANI level
 - MAC→Zone mapping for human-readable topology
 - Optional ICMP probe metrics (up/loss/latency) for all Sonos IPs and extra targets
 - Optional battery metrics when LocalBatteryStatus is present
"""

import re
import time
import logging
import socket
import struct
import select
import threading
import requests
import xml.etree.ElementTree as ET
from prometheus_client import Gauge, start_http_server


class SonosExporter:
    def __init__(
        self,
        sonos_ip,
        interval=60,
        icmp_enabled=True,
        icmp_interval=30,
        extra_icmp_targets=None,
    ):
        self.sonos_ip = sonos_ip
        self.interval = interval
        self.icmp_enabled = icmp_enabled
        self.icmp_interval = min(icmp_interval, interval) if icmp_interval else interval
        self.extra_icmp_targets = extra_icmp_targets or []

        self.logger = logging.getLogger("sonos_exporter")
        self.zone_ip_map = {}
        self._map_lock = threading.Lock()

        # Gauges start here
        self.rx_bytes = Gauge("sonos_rx_bytes", "Received bytes", ["zone", "interface"])
        self.tx_bytes = Gauge("sonos_tx_bytes", "Transmitted bytes", ["zone", "interface"])
        self.rx_packets = Gauge("sonos_rx_packets", "Received packets", ["zone", "interface"])
        self.tx_packets = Gauge("sonos_tx_packets", "Transmitted packets", ["zone", "interface"])
        self.rx_errors = Gauge("sonos_rx_errors", "Receive errors", ["zone", "interface"])
        self.tx_errors = Gauge("sonos_tx_errors", "Transmit errors", ["zone", "interface"])
        self.rx_errors = Gauge("sonos_rx_dropped", "Receive drops", ["zone", "interface"])
        self.tx_errors = Gauge("sonos_tx_dropped", "Transmit drops", ["zone", "interface"])        
        self.phy_errors = Gauge("sonos_phy_errors", "PHY errors", ["zone"])
        self.noise_floor = Gauge("sonos_noise_floor_dbm", "Noise floor", ["zone"])
        self.ofdm_ani = Gauge("sonos_ofdm_ani_level", "OFDM ANI", ["zone"])
        self.operating_channel = Gauge("sonos_operating_channel", "Channel", ["zone"])
        self.ieee_channel = Gauge("sonos_ieee_channel", "IEEE channel", ["zone"])
        self.wifi_mode = Gauge("sonos_wifi_mode", "0=SonosNet 1=Station", ["zone"])
        self.device_up = Gauge("sonos_device_up", "Reachable", ["zone"])

        # Battery metrics
        self.battery_level = Gauge("sonos_battery_level_percent", "Battery %", ["zone"])
        self.battery_info = Gauge(
            "sonos_battery_info",
            "Battery metadata",
            ["zone", "health", "temperature", "power_source"],
        )

        # SonosNet link quality
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

        # ICMP metrics
        self.icmp_up = Gauge("icmp_up", "1 if up", ["zone", "ip"])
        self.icmp_loss = Gauge("icmp_loss", "Loss ratio", ["zone", "ip"])
        self.icmp_latency = Gauge("icmp_latency_ms", "Latency ms", ["zone", "ip"])

    # Helper functions
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
            tx_dropped = re.search(r"TX packets:\d+ errors:\d+ dropped:(\d+)", b)
            rx_dropped = re.search(r"RX packets:\d+ errors:\d+ dropped:(\d+)", b)

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
            if tx_dropped:
                self.rx_errors.labels(zone, iface).set(float(tx_dropped.group(1)))
            if rx_dropped:
                self.tx_errors.labels(zone, iface).set(float(rx_dropped.group(1)))

    def parse_proc_status(self, text, zone, mac_map):
        """Extract PHY, channel, noise floor, OFDM ANI, and link quality"""
        ch = re.search(r"Operating on channel\s+(\d+)", text)
        ieee = re.search(r"IEEE channel:\s*(\d+)", text)
        phy = re.search(r"PHY errors.*?:\s*(\d+)", text)
        noise = [int(n) for n in re.findall(r"Noise Floor:\s*(-?\d+)", text)]
        ofdm = re.search(r"OFDM ANI level:\s*(\d+)", text)

        if ch:
            self.operating_channel.labels(zone).set(float(ch.group(1)))
        if ieee:
            self.ieee_channel.labels(zone).set(float(ieee.group(1)))
        if phy:
            self.phy_errors.labels(zone).set(float(phy.group(1)))
        if noise:
            avg_noise = sum(noise) / len(noise)
            self.noise_floor.labels(zone).set(avg_noise)
        if ofdm:
            self.ofdm_ani.labels(zone).set(float(ofdm.group(1)))

        self.parse_sonosnet_links(text, zone, mac_map)

    def parse_sonosnet_links(self, text, zone, mac_map):
        """Parse Node … FROM … TO … STP … MODEL … KEY … with zone mapping"""
        pattern = re.compile(
            r"Node\s+([0-9A-F:]{17})\s+-\s+FROM\s+(\d+)\s+:\s+TO\s+(\d+)\s+:\s+STP\s+(\S+)\s+:\s+MODEL\s+([\d.]+):\s+KEY\s+(\d+)",
            re.I,
        )
        for m in pattern.finditer(text):
            mac, from_n, to_n, stp, model, key = m.groups()
            peer_zone = mac_map.get(mac.upper(), mac)
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
        """WiFi mode detection"""
        mode_text = self._find_text(elem, "WifiModeString")
        if mode_text:
            val = 1.0 if "STATION" in mode_text.upper() else 0.0
            self.wifi_mode.labels(zone).set(val)

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
                except:
                    pass
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
        url = f"http://{self.sonos_ip}:1400/support/review"
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            root = ET.fromstring(r.text)
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return

        zones, updated = 0, 0
        mac_map = {}
        zp_nodes = list(root.findall(".//ZPSupportInfo"))

        # ---- 1st pass: build MAC→zone map ----
        for zp in zp_nodes:
            zone = self._find_text(zp, "ZoneName") or "unknown"
            for cmd in zp.findall(".//Command"):
                if "ifconfig" in cmd.attrib.get("cmdline", ""):
                    text = cmd.text or ""
                    for mac in re.findall(r"HWaddr\s+([0-9A-F:]{17})", text, re.I):
                        mac_map[mac.upper()] = zone
            for f in zp.findall(".//File"):
                if "ifconfig" in f.attrib.get("name", ""):
                    text = f.text or ""
                    for mac in re.findall(r"HWaddr\s+([0-9A-F:]{17})", text, re.I):
                        mac_map[mac.upper()] = zone

        # ---- 2nd pass: parse metrics & build zone->ip map ----
        zone_ip_map = {}
        seen_links = set()
        for zp in zp_nodes:
            zone = self._find_text(zp, "ZoneName") or "unknown"
            ip_addr = self._find_text(zp, "IPAddress") or None
            if ip_addr:
                zone_ip_map[zone] = ip_addr

            zones += 1
            self.device_up.labels(zone).set(1.0)

            for w in zp.findall(".//Wireless"):
                self.parse_wireless(w, zone)

            self.parse_battery(zp, zone)

            for cmd in zp.findall(".//Command"):
                cmdline = cmd.attrib.get("cmdline", "")
                text = cmd.text or ""
                if "ifconfig" in cmdline:
                    self.parse_ifconfig(text, zone, mac_map)
                elif "ath_rincon/status" in cmdline:
                    self.parse_proc_status(text, zone, mac_map)

            for f in zp.findall(".//File"):
                fname = f.attrib.get("name", "")
                text = f.text or ""
                if "ath_rincon/status" in fname:
                    self.parse_proc_status(text, zone, mac_map)

            # Deduplicate link metrics (bidirectional pairs)
            for k in list(self.link_metric._metrics.keys()):
                z, peer, from_n, to_n, *_ = k
                key_pair = tuple(sorted([z, peer])) + (from_n, to_n)
                if key_pair in seen_links:
                    del self.link_metric._metrics[k]
                else:
                    seen_links.add(key_pair)

            updated += 1

        # Update shared zone->ip map for ICMP thread
        with self._map_lock:
            self.zone_ip_map = zone_ip_map

        self.logger.info(
            f"Scraped {zones} zones, MAC map size={len(mac_map)}, "
            f"links={len(seen_links)}, zone_ip_map={len(zone_ip_map)}"
        )
    # ICMP helpers
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
            sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.getprotobyname("icmp"))
        except Exception as e:
            self.logger.error(f"ICMP socket error: {e}")
            return None

        try:
            sock.settimeout(timeout)
            dest_ip = socket.gethostbyname(dest_addr)
            header = struct.pack("!BBHHH", 8, 0, 0, ident, seq)
            data = struct.pack("!d", time.time())
            chksum = self._icmp_checksum(header + data)
            header = struct.pack("!BBHHH", 8, 0, chksum, ident, seq)
            packet = header + data

            send_time = time.time()
            sock.sendto(packet, (dest_ip, 1))

            ready = select.select([sock], [], [], timeout)
            if not ready[0]:
                return None
            recv_packet, addr = sock.recvfrom(1024)
            recv_time = time.time()
            return (recv_time - send_time) * 1000
        except:
            return None
        finally:
            sock.close()

    def _get_icmp_targets(self):
        with self._map_lock:
            items = list(self.zone_ip_map.items())
        targets = [(zone, ip) for zone, ip in items]
        for ip in self.extra_icmp_targets:
            targets.append(("custom", ip))
        return targets
    def _icmp_loop(self):
        self.logger.info("ICMP thread started")
        seq = 1
        ident = int(time.time()) & 0xFFFF

        while True:
            start = time.time()
            for zone, ip in self._get_icmp_targets():
                sent, received = 3, 0
                rtts = []
                for _ in range(3):
                    rtt = self._icmp_echo(ip, timeout=1, seq=seq, ident=ident)
                    seq = (seq + 1) & 0xFFFF or 1
                    if rtt is not None:
                        received += 1
                        rtts.append(rtt)
                    time.sleep(0.2)

                up = 1.0 if received else 0.0
                loss = (sent - received) / sent
                latency = sum(rtts) / len(rtts) if rtts else 0

                self.icmp_up.labels(zone, ip).set(up)
                self.icmp_loss.labels(zone, ip).set(loss)
                self.icmp_latency.labels(zone, ip).set(latency)

            sleep_time = max(0, self.icmp_interval - (time.time() - start))
            time.sleep(sleep_time)

    # Threaded scrape
    def scrape_thread(self):
        self.logger.info("Scrape thread started")
        while True:
            start = time.time()
            self.scrape_once()
            sleep_time = max(0, self.interval - (time.time() - start))
            time.sleep(sleep_time)

    def run(self):
        threading.Thread(target=self.scrape_thread, daemon=True).start()
        if self.icmp_enabled:
            threading.Thread(target=self._icmp_loop, daemon=True).start()
        while True:
            time.sleep(10)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sonos Prometheus Exporter (threaded)")

    parser.add_argument("--sonos-ip", required=True)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--port", type=int, default=9814)
    parser.add_argument("--log-level", default="INFO")

    parser.add_argument(
        "--icmp-disable",
        action="store_true",
        help="Disable ICMP metrics (enabled by default)",
    )
    parser.add_argument("--icmp-interval", type=int, default=30)
    parser.add_argument("--icmp-target", action="append", default=[])

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    exporter = SonosExporter(
        args.sonos_ip,
        interval=args.interval,
        icmp_enabled=not args.icmp_disable,
        icmp_interval=args.icmp_interval,
        extra_icmp_targets=args.icmp_target,
    )

    start_http_server(args.port)
    exporter.run()
