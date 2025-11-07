#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from prometheus_client import start_http_server, Gauge
import time
import re
import argparse

# Define Prometheus metrics
rx_bytes = Gauge("sonos_rx_bytes", "Received bytes on Sonos device", ["zone", "interface"])
tx_bytes = Gauge("sonos_tx_bytes", "Transmitted bytes on Sonos device", ["zone", "interface"])
phy_errors = Gauge("sonos_phy_errors", "PHY err" \
"ors since last reading/reset", ["zone"])
wifi_mode = Gauge("sonos_wifi_mode", "WiFi mode (0=SonosNet, 1=Station)", ["zone"])
link_speed = Gauge("sonos_link_speed", "Ethernet link speed (Mbps)", ["zone"])
noise_floor = Gauge("sonos_noise_floor_dbm", "Noise floor in dBm", ["zone", "chain"])
device_up = Gauge("sonos_device_up", "1 if device reachable", ["zone"])

def parse_ifconfig(zone_name, text):
    """Extract RX/TX byte counters from ifconfig output."""
    interfaces = {}
    current = None
    for line in text.splitlines():
        if re.match(r"^\w", line):  # new interface section
            current = line.split()[0]
            interfaces[current] = {}
        elif "RX bytes" in line:
            m = re.findall(r"RX bytes:(\d+).+TX bytes:(\d+)", line)
            if m:
                rx, tx = m[0]
                interfaces[current]["rx_bytes"] = int(rx)
                interfaces[current]["tx_bytes"] = int(tx)
    for iface, stats in interfaces.items():
        if "rx_bytes" in stats:
            rx_bytes.labels(zone_name, iface).set(stats["rx_bytes"])
            tx_bytes.labels(zone_name, iface).set(stats["tx_bytes"])

def parse_proc_status(zone_name, text):
    """Extract PHY errors and noise floors."""
    phy_match = re.search(r"PHY errors since last reading/reset:\s*(\d+)", text)
    if phy_match:
        phy_errors.labels(zone_name).set(int(phy_match.group(1)))

    for chain, dbm in re.findall(r"Noise Floor:\s*(-?\d+)\s*dBm\s*\(chain\s*(\d+)", text):
        noise_floor.labels(zone_name, chain).set(int(dbm))

def parse_enet(zone_name, xml_block):
    """Parse Ethernet link speed."""
    port = xml_block.find(".//Port")
    if port is not None and port.find("Speed") is not None:
        speed = port.find("Speed").text
        if speed and speed.endswith("FD"):
            val = int(re.findall(r"\d+", speed)[0])
            link_speed.labels(zone_name).set(val)

def parse_wireless(zone_name, xml_block):
    """Parse wireless info such as mode."""
    mode = xml_block.find(".//WifiMode")
    if mode is not None:
        wifi_mode.labels(zone_name).set(int(mode.text))

def scrape_once(base_url):
    try:
        r = requests.get(f"http://{base_url}:1400/support/review", timeout=5)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        for zp in root.findall(".//ZPSupportInfo"):
            zone_elem = zp.find(".//ZoneName")
            if zone_elem is None:
                continue
            zone_name = zone_elem.text
            device_up.labels(zone_name).set(1)
            # Enet
            parse_enet(zone_name, zp.find("EnetPorts"))
            # Wireless
            parse_wireless(zone_name, zp.find("Wireless"))
            # Commands
            for cmd in zp.findall("Command"):
                cmd_text = cmd.text or ""
                if "ifconfig" in cmd.get("cmdline", ""):
                    parse_ifconfig(zone_name, cmd_text)
                elif "/proc/ath_rincon/status" in cmd.get("cmdline", "") or "Debug info" in cmd_text:
                    parse_proc_status(zone_name, cmd_text)
    except Exception as e:
        print(f"Error scraping {base_url}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Sonos Prometheus Exporter")
    parser.add_argument("--host", default="0.0.0.0", help="Exporter listen address")
    parser.add_argument("--port", type=int, default=9814, help="Exporter listen port")
    parser.add_argument("--sonos-ip", required=True, help="IP of Sonos device to scrape")
    parser.add_argument("--interval", type=int, default=60, help="Scrape interval seconds")
    args = parser.parse_args()

    print(f"Starting Sonos exporter on {args.host}:{args.port}, scraping {args.sonos_ip} every {args.interval}s")
    start_http_server(args.port, addr=args.host)

    while True:
        scrape_once(args.sonos_ip)
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
