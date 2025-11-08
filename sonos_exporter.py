#!/usr/bin/env python3
"""
Sonos Prometheus Exporter (final unified)
-----------------------------------------
Features:
 - RX/TX bytes, packets, errors
 - PHY errors, noise floor, channels, WiFi mode, device up
 - Link quality (FROM / TO)
 - OFDM ANI level
 - MAC→Zone mapping for human-readable topology
"""

import re
import time
import logging
import requests
import xml.etree.ElementTree as ET
from prometheus_client import Gauge, start_http_server


class SonosExporter:
    def __init__(self, sonos_ip, interval=60):
        self.sonos_ip = sonos_ip
        self.interval = interval
        self.logger = logging.getLogger("sonos_exporter")

        # --- Core Gauges ---
        self.rx_bytes = Gauge("sonos_rx_bytes", "Received bytes", ["zone", "interface"])
        self.tx_bytes = Gauge("sonos_tx_bytes", "Transmitted bytes", ["zone", "interface"])
        self.rx_packets = Gauge("sonos_rx_packets", "Received packets", ["zone", "interface"])
        self.tx_packets = Gauge("sonos_tx_packets", "Transmitted packets", ["zone", "interface"])
        self.rx_errors = Gauge("sonos_rx_errors", "Receive errors", ["zone", "interface"])
        self.tx_errors = Gauge("sonos_tx_errors", "Transmit errors", ["zone", "interface"])

        self.phy_errors = Gauge("sonos_phy_errors", "PHY errors since last reset", ["zone"])
        self.noise_floor = Gauge("sonos_noise_floor_dbm", "Average noise floor (dBm)", ["zone"])
        self.ofdm_ani = Gauge("sonos_ofdm_ani_level", "OFDM Adaptive Noise Immunity level", ["zone"])
        self.operating_channel = Gauge("sonos_operating_channel", "Operating channel", ["zone"])
        self.ieee_channel = Gauge("sonos_ieee_channel", "IEEE channel", ["zone"])
        self.wifi_mode = Gauge("sonos_wifi_mode", "WiFi mode (0=SonosNet, 1=Station)", ["zone"])
        self.device_up = Gauge("sonos_device_up", "1 if device reachable", ["zone"])

        # SonosNet topology and link quality (peer_zone replaces MAC)
        self.link_metric = Gauge(
            "sonos_link_connection",
            "SonosNet link relationships (FROM/TO/STP/MODEL/KEY)",
            ["zone", "peer_zone", "from_node", "to_node", "stp", "model", "key"],
        )
        self.link_from = Gauge(
            "sonos_link_quality_from",
            "SonosNet FROM link quality index (higher=better)",
            ["zone", "peer_zone", "model"],
        )
        self.link_to = Gauge(
            "sonos_link_quality_to",
            "SonosNet TO link quality index (higher=better)",
            ["zone", "peer_zone", "model"],
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------
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

        # ---- 2nd pass: parse metrics ----
        seen_links = set()
        for zp in zp_nodes:
            zone = self._find_text(zp, "ZoneName") or "unknown"
            zones += 1
            self.device_up.labels(zone).set(1.0)

            for w in zp.findall(".//Wireless"):
                self.parse_wireless(w, zone)

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

        self.logger.info(f"Scraped {zones} zones, MAC map size={len(mac_map)}, links={len(seen_links)}")


    def run_forever(self):
        while True:
            start = time.time()
            self.scrape_once()
            elapsed = time.time() - start
            self.logger.info(f"Scrape completed in {elapsed:.2f}s")
            time.sleep(self.interval)


# ------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sonos Prometheus Exporter (final unified)")
    parser.add_argument("--sonos-ip", required=True)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--port", type=int, default=9814)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    exporter = SonosExporter(args.sonos_ip, args.interval)
    logging.info(
        f"Starting Sonos Exporter on :{args.port}, scraping {args.sonos_ip} every {args.interval}s"
    )
    start_http_server(args.port)
    exporter.run_forever()
