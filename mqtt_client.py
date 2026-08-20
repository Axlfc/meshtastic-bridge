"""
mqtt_client.py
==============

R6 — Publicación y Suscripción MQTT (E/S External).

Publica eventos normalizados en los temas:
  - `meshtastic/rx/text`
  - `meshtastic/rx/telemetry`
  - `meshtastic/rx/position`
  - `meshtastic/rx/nodeinfo`
  - `meshtastic/status/bridge` (LWT / Will Message)

Se suscribe a:
  - `meshtastic/tx/text`
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from config import MqttConfig
from schemas import BridgeStatus, RxEvent, TxTextCommand, get_event_topic

logger = logging.getLogger("meshtastic_bridge.mqtt")


class MqttBridgeClient:
    """Cliente MQTT asíncrono para meshtastic-bridge."""

    def __init__(
        self,
        config: MqttConfig,
        on_tx_text_callback: Callable[[TxTextCommand], None],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.config = config
        self.on_tx_text_callback = on_tx_text_callback
        self.loop = loop or asyncio.get_event_loop()

        self._client: Optional[mqtt.Client] = None
        self._connected: bool = False
        self._running: bool = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _get_topic(self, subtopic: str) -> str:
        prefix = self.config.topic_prefix.rstrip("/")
        return f"{prefix}/{subtopic.lstrip('/')}"

    async def start(self) -> None:
        """Inicializa y conecta el cliente MQTT."""
        self._running = True

        client_id = self.config.client_id
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )

        if self.config.username and self.config.password:
            self._client.username_pw_set(self.config.username, self.config.password)

        if self.config.tls:
            self._client.tls_set()

        # Configurar Will Message (LWT)
        status_topic = self._get_topic("status/bridge")
        offline_payload = BridgeStatus(status="offline").model_dump_json()
        self._client.will_set(status_topic, payload=offline_payload, qos=self.config.qos, retain=True)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        def _blocking_connect():
            logger.info("mqtt_connecting", extra={"host": self.config.host, "port": self.config.port})
            self._client.connect(self.config.host, self.config.port, keepalive=self.config.keepalive)
            self._client.loop_start()

        try:
            await self.loop.run_in_executor(None, _blocking_connect)
        except Exception as e:
            logger.error("mqtt_connect_failed", extra={"error": str(e)})

    async def stop(self) -> None:
        """Desconecta limpiamente publicando el estado offline."""
        self._running = False
        if self._client and self._connected:
            status_topic = self._get_topic("status/bridge")
            offline_payload = BridgeStatus(status="offline").model_dump_json()

            def _blocking_disconnect():
                try:
                    self._client.publish(status_topic, payload=offline_payload, qos=self.config.qos, retain=True)
                    self._client.loop_stop()
                    self._client.disconnect()
                except Exception:
                    pass

            await self.loop.run_in_executor(None, _blocking_disconnect)
            self._connected = False

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        logger.info("mqtt_connected", extra={"rc": str(rc)})
        self._connected = True

        # Publicar status online (retained)
        status_topic = self._get_topic("status/bridge")
        online_payload = BridgeStatus(status="online").model_dump_json()
        client.publish(status_topic, payload=online_payload, qos=self.config.qos, retain=True)

        # Suscribir a comandos de envío tx/text
        tx_topic = self._get_topic("tx/text")
        client.subscribe(tx_topic, qos=self.config.qos)
        logger.info("mqtt_subscribed", extra={"topic": tx_topic})

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any, rc: Any, properties: Any = None) -> None:
        logger.warning("mqtt_disconnected", extra={"rc": str(rc)})
        self._connected = False

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            payload_str = msg.payload.decode("utf-8")
            tx_topic = self._get_topic("tx/text")
            if msg.topic == tx_topic:
                data = json.loads(payload_str)
                cmd = TxTextCommand.model_validate(data)
                self.loop.call_soon_threadsafe(self.on_tx_text_callback, cmd)
        except Exception as e:
            logger.error("mqtt_message_error", extra={"error": str(e), "topic": msg.topic})

    async def publish_event(self, event: RxEvent) -> bool:
        """Publica un evento RxEvent Pydantic en su tópico MQTT correspondiente."""
        if not self._connected or not self._client:
            logger.warning("mqtt_publish_dropped_not_connected")
            return False

        full_topic = get_event_topic(event)
        if full_topic.startswith("meshtastic/"):
            subtopic = full_topic[len("meshtastic/"):]
        else:
            subtopic = full_topic

        topic = self._get_topic(subtopic)
        payload = event.model_dump_json()

        def _blocking_publish():
            info = self._client.publish(topic, payload=payload, qos=self.config.qos)
            rc = getattr(info, "rc", mqtt.MQTT_ERR_SUCCESS)
            return rc == mqtt.MQTT_ERR_SUCCESS

        try:
            success = await self.loop.run_in_executor(None, _blocking_publish)
            logger.debug("mqtt_event_published", extra={"topic": topic, "packet_id": getattr(event, "packet_id", None)})
            return success
        except Exception as e:
            logger.error("mqtt_publish_error", extra={"error": str(e), "topic": topic})
            return False
