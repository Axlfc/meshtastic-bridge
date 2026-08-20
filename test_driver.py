"""
Pruebas para MeshHardwareDriver.
"""
import asyncio
import pytest
from unittest.mock import MagicMock, patch
from config import TransportConfig
from driver import MeshHardwareDriver
from schemas import TxTextCommand

@pytest.mark.asyncio
async def test_driver_reconnect_backoff():
    config = TransportConfig(
        mode="serial",
        serial_port="/dev/ttyMOCK",
        reconnect_initial_delay_s=0.05,
        reconnect_max_delay_s=0.2,
        reconnect_jitter_s=0.01,
    )
    received_packets = []

    def on_packet(pkt):
        received_packets.append(pkt)

    driver = MeshHardwareDriver(config=config, on_packet_callback=on_packet)

    with patch("meshtastic.serial_interface.SerialInterface") as mock_serial:
        mock_serial.side_effect = [Exception("Port not found"), MagicMock()]

        await driver.start()
        await asyncio.sleep(0.3)
        assert driver.is_connected is True

        await driver.stop()
        assert driver.is_connected is False

@pytest.mark.asyncio
async def test_driver_send_text():
    config = TransportConfig(mode="serial", serial_port="/dev/ttyMOCK")
    driver = MeshHardwareDriver(config=config, on_packet_callback=lambda p: None)

    mock_iface = MagicMock()
    driver._interface = mock_iface
    driver._connected = True

    cmd = TxTextCommand(to_node="!a1b2c3d4", text="Test message", channel=0, want_ack=False)
    success = await driver.send_text(cmd)

    assert success is True
    mock_iface.sendText.assert_called_once_with(
        text="Test message",
        destinationId=0xa1b2c3d4,
        wantAck=False,
        channelIndex=0,
    )
