"""
Pruebas para MqttBridgeClient y WebhookDriver.
"""
import asyncio
import json
import httpx
import pytest
from unittest.mock import MagicMock
from config import MqttConfig, WebhookConfig
from mqtt_client import MqttBridgeClient
from webhook import WebhookDriver
from schemas import TextEvent, TextPayload, TxTextCommand

@pytest.mark.asyncio
async def test_mqtt_bridge_client_publish_event():
    config = MqttConfig(host="localhost", port=1883)
    tx_cmds = []

    client = MqttBridgeClient(config=config, on_tx_text_callback=lambda cmd: tx_cmds.append(cmd))

    mock_paho = MagicMock()
    mock_info = MagicMock()
    mock_info.rc = 0  # MQTT_ERR_SUCCESS
    mock_paho.publish.return_value = mock_info

    client._client = mock_paho
    client._connected = True

    event = TextEvent(
        packet_id=123,
        from_node="!a1b2c3d4",
        payload=TextPayload(text="Hola MQTT")
    )

    success = await client.publish_event(event)
    assert success is True

    mock_paho.publish.assert_called_once()
    args, kwargs = mock_paho.publish.call_args
    assert args[0] == "meshtastic/rx/text"
    payload_data = json.loads(kwargs["payload"])
    assert payload_data["packet_id"] == 123
    assert payload_data["payload"]["text"] == "Hola MQTT"

@pytest.mark.asyncio
async def test_webhook_driver():
    config = WebhookConfig(
        enabled=True,
        urls=["http://example.com/webhook"],
        timeout_s=2.0
    )
    driver = WebhookDriver(config=config)

    captured_requests = []

    def mock_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, json={"status": "ok"})

    transport = httpx.MockTransport(mock_handler)
    driver._client = httpx.AsyncClient(transport=transport, timeout=2.0)

    event = TextEvent(
        packet_id=456,
        from_node="!a1b2c3d4",
        payload=TextPayload(text="Hola Webhook")
    )

    await driver.send_event(event)

    assert len(captured_requests) == 1
    req = captured_requests[0]
    assert req.url == "http://example.com/webhook"
    req_data = json.loads(req.content)
    assert req_data["packet_id"] == 456
    assert req_data["payload"]["text"] == "Hola Webhook"

    await driver.stop()
