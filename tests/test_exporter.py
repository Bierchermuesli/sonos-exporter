import os
import struct
import unittest
from unittest.mock import MagicMock, patch

from prometheus_client import REGISTRY

import sonos_exporter

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name):
    with open(os.path.join(FIXTURES, name), "rb") as f:
        return f.read()


def samples(name):
    """Return all samples of a metric family from the global registry."""
    for metric in REGISTRY.collect():
        if metric.name == name:
            return list(metric.samples)
    return []


def sample_value(name, **labels):
    for s in samples(name):
        if all(s.labels.get(k) == v for k, v in labels.items()):
            return s.value
    return None


class ExporterFixtureTest(unittest.TestCase):
    """Scrape one fixture into a fresh exporter and unregister it afterwards."""

    fixture = None

    def setUp(self):
        self.exporter = sonos_exporter.SonosExporter(sonos_ip="192.0.2.41", icmp_interval=0)
        with patch("sonos_exporter.requests.get") as get:
            get.return_value = MagicMock(
                content=load_fixture(self.fixture), raise_for_status=MagicMock()
            )
            self.exporter.scrape_once()

    def tearDown(self):
        for collector in list(REGISTRY._names_to_collectors.values()):
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass


class TestReview(ExporterFixtureTest):
    fixture = "review.xml"

    def test_device_info(self):
        value = sample_value(
            "sonos_device_info",
            zone="Stue (LF,RF)",
            ip_address="192.0.2.41",
            mac_address="02:00:03:31:BD:A9",
            serial_number="02-00-03-31-BD-A9:3",
            software_version="92.0-70280",
            series_id="A203",
        )
        self.assertEqual(value, 1.0)

    def test_zone_ip_map(self):
        self.assertEqual(self.exporter.zone_ip_map["Stue (LF,RF)"], "192.0.2.41")
        self.assertEqual(self.exporter.zone_ip_map["Bedroom"], "192.0.2.49")
        self.assertEqual(len(self.exporter.zone_ip_map), 8)

    def test_wireless_state(self):
        self.assertEqual(
            sample_value("sonos_wifi_mode", zone="Stue (LF,RF)", mode_string="STATION_MODE"), 1.0
        )
        self.assertEqual(
            sample_value(
                "sonos_wifi_mode",
                zone="Stue (LR)",
                mode_string="SONOSNET_SATELLITE_STATION_PRIMARY_MODE",
            ),
            2.0,
        )
        # IdleState 0x00000014 -> 20, 0x00000004 -> 4
        self.assertEqual(sample_value("sonos_idle_state", zone="Stue (LF,RF)"), 20.0)
        self.assertEqual(sample_value("sonos_idle_state", zone="Stue (LR)"), 4.0)

    def test_busy_state_flags(self):
        for state in ("NO_SONOSNET_PEERS", "NO_PRIMARY"):
            self.assertEqual(sample_value("sonos_busy_state", zone="Stue (LF,RF)", state=state), 1.0)
        self.assertIsNone(sample_value("sonos_busy_state", zone="Stue (LF,RF)", state="AUDIO_OUT"))

    def test_radio_details(self):
        self.assertEqual(sample_value("sonos_rf_chains", zone="Stue (LF,RF)", direction="rx"), 4.0)
        self.assertEqual(sample_value("sonos_rf_chains", zone="Stue (LF,RF)", direction="tx"), 4.0)
        self.assertEqual(sample_value("sonos_rf_chains", zone="Kitchen", direction="rx"), 3.0)
        self.assertEqual(sample_value("sonos_phy_errors", zone="Stue (LF,RF)"), 19342.0)
        self.assertEqual(sample_value("sonos_ht_channel", zone="Stue (LF,RF)"), 0.0)
        self.assertEqual(sample_value("sonos_bridge_stp_enabled", zone="Stue (LF,RF)"), 0.0)
        self.assertEqual(sample_value("sonos_bridge_stp_enabled", zone="Stue (LR)"), 1.0)
        self.assertEqual(sample_value("sonos_enet_connected", zone="Stue (LF,RF)", port="0"), 0.0)

    def test_interface_counters(self):
        self.assertEqual(
            sample_value("sonos_rx_bytes", zone="Stue (LF,RF)", interface="apcli0"), 33665050459.0
        )
        self.assertEqual(sample_value("sonos_rx_errors", zone="Stue (LF,RF)", interface="ra0"), 2030.0)
        self.assertEqual(sample_value("sonos_rx_dropped", zone="Stue (LF,RF)", interface="ath1"), 22.0)

    def test_links_resolve_mac_plus_one_to_zone(self):
        # The ath_rincon node list reports the peer's wireless MAC, which is the
        # device MAC + 1. Every peer in this fixture must resolve to a zone name.
        peers = {s.labels["peer_zone"] for s in samples("sonos_link_connection")}
        self.assertIn("Kitchen", peers)
        self.assertIn("Bedroom", peers)
        for peer in peers:
            self.assertNotRegex(peer, r"^([0-9A-F]{2}:){5}[0-9A-F]{2}$", f"unresolved MAC {peer}")

    def test_link_quality(self):
        self.assertEqual(
            sample_value(
                "sonos_link_quality_from", zone="Stue (LF,RF)", peer_zone="Sub Mini (SW)", model="37.1"
            ),
            65.0,
        )
        self.assertEqual(
            sample_value(
                "sonos_link_quality_to", zone="Stue (LF,RF)", peer_zone="Sub Mini (SW)", model="37.1"
            ),
            67.0,
        )

    def test_no_battery_metrics_without_battery(self):
        self.assertEqual(samples("sonos_battery_level_percent"), [])
        self.assertEqual(samples("sonos_battery_info"), [])

    def test_device_up_from_http_when_icmp_disabled(self):
        self.assertEqual(sample_value("sonos_device_up", zone="Stue (LF,RF)"), 1.0)


class TestReviewWithBattery(ExporterFixtureTest):
    fixture = "review_battery.xml"

    def test_battery(self):
        self.assertEqual(sample_value("sonos_battery_level_percent", zone="Sonos Roam SL"), 100.0)
        self.assertEqual(
            sample_value(
                "sonos_battery_info",
                zone="Sonos Roam SL",
                health="GREEN",
                temperature="NORMAL",
                power_source="USB_POWER",
            ),
            1.0,
        )

    def test_ht_channel_and_busy_states(self):
        self.assertEqual(sample_value("sonos_ht_channel", zone="Stue (LF,RF)"), 44.0)
        for state in ("AUDIO_OUT", "NO_SONOSNET_PEERS", "NO_PRIMARY"):
            self.assertEqual(sample_value("sonos_busy_state", zone="Stue (LF,RF)", state=state), 1.0)

    def test_unknown_peer_keeps_raw_mac(self):
        # The Kitchen zone is missing from this dump, so links to it stay as MAC.
        self.assertEqual(
            sample_value(
                "sonos_link_connection", zone="Stue (LF,RF)", peer_zone="02:00:02:35:6E:88"
            ),
            1.0,
        )


class TestIcmpHelpers(unittest.TestCase):
    def test_checksum_verifies(self):
        header = struct.pack("!BBHHH", 8, 0, 0, 0x1234, 1)
        payload = b"\x00" * 8
        chksum = sonos_exporter.SonosExporter._icmp_checksum(header + payload)
        packet = struct.pack("!BBHHH", 8, 0, chksum, 0x1234, 1) + payload
        self.assertEqual(sonos_exporter.SonosExporter._icmp_checksum(packet), 0)


if __name__ == "__main__":
    unittest.main()
