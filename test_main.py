"""
Pruebas de orquestación de la aplicación (main.py).
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from main import Application

@pytest.mark.asyncio
async def test_application_lifecycle():
    with patch("main.MeshHardwareDriver") as MockDriver, \
         patch("main.MqttBridgeClient") as MockMqtt, \
         patch("main.WebhookDriver") as MockWebhook, \
         patch("uvicorn.Server.serve", new_callable=AsyncMock):

        mock_driver_instance = MockDriver.return_value
        mock_driver_instance.is_connected = True
        mock_driver_instance.start = AsyncMock()
        mock_driver_instance.stop = AsyncMock()

        mock_mqtt_instance = MockMqtt.return_value
        mock_mqtt_instance.start = AsyncMock()
        mock_mqtt_instance.stop = AsyncMock()
        mock_mqtt_instance.publish_event = AsyncMock(return_value=True)

        mock_webhook_instance = MockWebhook.return_value
        mock_webhook_instance.start = AsyncMock()
        mock_webhook_instance.stop = AsyncMock()
        mock_webhook_instance.send_event = AsyncMock()

        app = Application()

        # Probamos procesamiento de paquete raw
        packet = {
            "fromId": "!a1b2c3d4",
            "id": 111,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "text": "Prueba orchestrator"
            }
        }

        await app._process_packet(packet)

        # Verificamos que se incrementaron los contadores de métricas
        assert app.metrics.rx_messages == 1
        assert app.metrics.dedup_seen == 1

        # Segundo paquete igual -> deduplicado
        await app._process_packet(packet)
        assert app.metrics.rx_messages == 1
        assert app.metrics.dedup_duplicates == 1

        await app.start()
        await asyncio.sleep(0.05)
        await app.stop()
