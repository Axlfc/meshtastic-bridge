"""
main.py
=======

Punto de entrada principal que orquesta todos los componentes del demonio:
  - Configuración (config.py)
  - NodeDB con persistencia atómica (nodedb.py)
  - Planificador de topología (planner_import.py)
  - Deserializador y Ruteador (decoder.py)
  - Filtro Deduplicador de paquetes (deduplicator.py)
  - Cola Leaky Bucket de salida (ratelimiter.py)
  - Driver de Hardware con auto-healing (driver.py)
  - Cliente MQTT con LWT (mqtt_client.py)
  - Driver Webhook HTTP (webhook.py)
  - Servidor de Salud FastAPI / uvicorn (health.py)
  - Manejo limpio de señales SIGINT / SIGTERM para apagar el demonio sin perder datos.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional

import structlog
import uvicorn

from config import load_settings
from decoder import PacketDecoder
from deduplicator import PacketDeduplicator
from driver import MeshHardwareDriver
from health import MetricsTracker, create_health_app
from mqtt_client import MqttBridgeClient
from nodedb import NodeDB, format_node_id
from planner_import import load_planned_topology
from ratelimiter import OutboundQueue
from schemas import TxTextCommand
from webhook import WebhookDriver


def setup_logging(level_str: str, json_output: bool) -> None:
    log_level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(level=log_level, stream=sys.stdout, format="%(message)s")

    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


class Application:
    def __init__(self) -> None:
        self.settings = load_settings()
        setup_logging(self.settings.logging.level, self.settings.logging.json_output)
        self.logger = structlog.get_logger("meshtastic_bridge.main")

        self.start_time = time.time()
        self.metrics = MetricsTracker()

        # 1. NodeDB
        self.nodedb = NodeDB(
            persist_path=self.settings.nodedb.persist_path,
            persist_interval_s=self.settings.nodedb.persist_interval_s,
        )

        # 2. Planned Topology Import
        self.planned_sites = []
        if self.settings.planner.enabled and self.settings.planner.planned_topology_path:
            try:
                self.planned_sites = load_planned_topology(self.settings.planner.planned_topology_path)
                self.logger.info("planner_topology_loaded", count=len(self.planned_sites), path=self.settings.planner.planned_topology_path)
            except Exception as e:
                self.logger.error("planner_topology_load_failed", error=str(e))

        # 3. Decoder
        self.decoder = PacketDecoder(
            nodedb=self.nodedb,
            planned_sites=self.planned_sites,
            match_radius_m=self.settings.planner.match_radius_m,
        )

        # 4. Deduplicator
        self.deduplicator = PacketDeduplicator(
            window_size=self.settings.dedup.window_size,
            ttl_seconds=self.settings.dedup.ttl_seconds,
        )

        # 5. Outbound Queue (Leaky Bucket Airtime Guard)
        self.outbound_queue: OutboundQueue[TxTextCommand] = OutboundQueue(
            min_interval_s=self.settings.rate_limit.min_interval_s,
            max_queue_size=self.settings.rate_limit.max_queue_size,
        )

        # 6. Webhooks
        self.webhook_driver = WebhookDriver(config=self.settings.webhook)

        # 7. MQTT Client
        self.mqtt_client = MqttBridgeClient(
            config=self.settings.mqtt,
            on_tx_text_callback=self._handle_incoming_tx_text,
        )

        # 8. Hardware Driver
        self.driver = MeshHardwareDriver(
            config=self.settings.transport,
            on_packet_callback=self._handle_raw_packet,
        )

        # 9. Health Server App
        self.health_app = create_health_app(
            settings=self.settings,
            nodedb=self.nodedb,
            outbound_queue=self.outbound_queue,
            is_radio_connected_fn=lambda: self.driver.is_connected,
            metrics=self.metrics,
            start_time=self.start_time,
        )

        self._drain_task: Optional[asyncio.Task] = None
        self._uvicorn_server: Optional[uvicorn.Server] = None
        self._stop_event = asyncio.Event()

    def _handle_raw_packet(self, packet: Dict[str, Any]) -> None:
        """Callback invocado de forma segura desde el driver cuando llega un paquete raw."""
        asyncio.create_task(self._process_packet(packet))

    async def _process_packet(self, packet: Dict[str, Any]) -> None:
        packet_id = packet.get("id") or 0
        from_id_raw = packet.get("fromId") or packet.get("from")
        if from_id_raw is None:
            return
        from_node = format_node_id(from_id_raw)

        self.metrics.dedup_seen += 1

        # R4 Deduplicación
        if self.deduplicator.is_duplicate(packet_id, from_node):
            self.metrics.dedup_duplicates += 1
            self.logger.debug("packet_deduplicated_dropped", packet_id=packet_id, from_node=from_node)
            return

        # R2 Deserialización a evento Pydantic
        event = self.decoder.decode_packet(packet)
        if not event:
            return

        self.metrics.rx_messages += 1

        # Publicar en MQTT
        await self.mqtt_client.publish_event(event)

        # Enviar a Webhooks
        await self.webhook_driver.send_event(event)

    def _handle_incoming_tx_text(self, cmd: TxTextCommand) -> None:
        """Callback invocado cuando llega un comando `meshtastic/tx/text` desde MQTT."""
        asyncio.create_task(self._enqueue_tx_text(cmd))

    async def _enqueue_tx_text(self, cmd: TxTextCommand) -> None:
        enqueued = await self.outbound_queue.put(cmd)
        if enqueued:
            self.logger.info("tx_text_enqueued", to=cmd.to_node, text=cmd.text)
        else:
            self.logger.warning("tx_text_enqueue_failed_full", to=cmd.to_node)

    async def _outbound_drain_loop(self) -> None:
        """Tarea de fondo que drena la cola con Leaky Bucket hacia la radio."""
        async for cmd in self.outbound_queue.drain():
            success = await self.driver.send_text(cmd)
            if success:
                self.metrics.tx_messages += 1

    async def start(self) -> None:
        self.logger.info("starting_meshtastic_bridge")

        # Iniciar NodeDB
        await self.nodedb.start_periodic_save()

        # Iniciar Webhooks
        await self.webhook_driver.start()

        # Iniciar MQTT
        await self.mqtt_client.start()

        # Iniciar Driver Hardware
        await self.driver.start()

        # Iniciar drenador de cola de salida
        self._drain_task = asyncio.create_task(self._outbound_drain_loop())

        # Iniciar servidor Healthcheck uvicorn
        config = uvicorn.Config(
            app=self.health_app,
            host=self.settings.health.host,
            port=self.settings.health.port,
            log_level="warning",
        )
        self._uvicorn_server = uvicorn.Server(config)

        # Correr uvicorn en tarea de fondo
        asyncio.create_task(self._uvicorn_server.serve())

        self.logger.info(
            "meshtastic_bridge_running",
            health_endpoint=f"http://{self.settings.health.host}:{self.settings.health.port}/healthz"
        )

    async def stop(self) -> None:
        self.logger.info("stopping_meshtastic_bridge")

        if self._drain_task:
            self._drain_task.cancel()

        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True

        await self.driver.stop()
        await self.mqtt_client.stop()
        await self.webhook_driver.stop()
        await self.nodedb.stop()

        self.logger.info("meshtastic_bridge_stopped")


async def main() -> None:
    app = Application()

    loop = asyncio.get_running_loop()

    def _signal_handler():
        print("\nRecibida señal de parada. Iniciando graceful shutdown...")
        app._stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    await app.start()

    try:
        await app._stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
