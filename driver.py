"""
driver.py
=========

R1 — Capa de Conexión de Hardware (Hardware Transport Engine).

Maneja conexiones SERIAL (`/dev/ttyUSB*`, `/dev/ttyACM*`) y TCP (`192.168.x.x:4403`)
a nodos Meshtastic con auto-healing resiliente (reintentos exponenciales con jitter),
heartbeat monitor y cruce seguro de hilos hacia el bucle de eventos principal de asyncio.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Callable, Dict, Optional

from config import TransportConfig
from schemas import TxTextCommand

logger = logging.getLogger("meshtastic_bridge.driver")


class MeshHardwareDriver:
    """Gestor de conexión serie/TCP con reconexión automática y heartbeat."""

    def __init__(
        self,
        config: TransportConfig,
        on_packet_callback: Callable[[Dict[str, Any]], None],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.config = config
        self.on_packet_callback = on_packet_callback
        self.loop = loop or asyncio.get_event_loop()

        self._interface: Any = None
        self._connected: bool = False
        self._running: bool = False
        self._last_rx_time: float = time.monotonic()
        self._reconnect_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        """Inicia el bucle de mantenimiento de conexión y heartbeat."""
        self._running = True
        self._reconnect_task = asyncio.create_task(self._auto_healing_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Detiene de forma limpia el driver y cierra la interfaz hardware."""
        self._running = False
        if self._reconnect_task:
            self._reconnect_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        await self._close_interface()

    async def _close_interface(self) -> None:
        if self._interface is not None:
            try:
                # Cierre en ejecutor por si `close()` es bloqueante
                def _do_close():
                    try:
                        self._interface.close()
                    except Exception:
                        pass

                await self.loop.run_in_executor(None, _do_close)
            except Exception as e:
                logger.warning("interface_close_error", extra={"error": str(e)})
            finally:
                self._interface = None
                self._connected = False

    def _pubsub_on_receive(self, packet: Dict[str, Any], interface: Any = None) -> None:
        """Callback invocado por el hilo de fondo del SDK de Meshtastic / pubsub."""
        self._last_rx_time = time.monotonic()
        # Transferir de forma segura al event loop de asyncio
        self.loop.call_soon_threadsafe(self.on_packet_callback, packet)

    async def _connect_hardware(self) -> bool:
        """Intenta establecer conexión Serie o TCP según la configuración."""
        def _blocking_connect() -> Any:
            import meshtastic.serial_interface
            import meshtastic.tcp_interface
            from pubsub import pub

            if self.config.mode == "serial":
                logger.info("connecting_serial", extra={"port": self.config.serial_port})
                iface = meshtastic.serial_interface.SerialInterface(
                    devPath=self.config.serial_port,
                    connectNow=True
                )
            elif self.config.mode == "tcp":
                logger.info("connecting_tcp", extra={"host": self.config.tcp_host, "port": self.config.tcp_port})
                iface = meshtastic.tcp_interface.TCPInterface(
                    hostname=self.config.tcp_host,
                    portNumber=self.config.tcp_port,
                    connectNow=True
                )
            else:
                raise ValueError(f"Modo de transporte desconocido: {self.config.mode}")

            # Suscribirse a mensajes recibidos vía pypubsub
            pub.subscribe(self._pubsub_on_receive, "meshtastic.receive")
            return iface

        try:
            self._interface = await self.loop.run_in_executor(None, _blocking_connect)
            self._connected = True
            self._last_rx_time = time.monotonic()
            logger.info("hardware_connected", extra={"mode": self.config.mode})
            return True
        except Exception as e:
            logger.error("hardware_connect_failed", extra={"error": str(e), "mode": self.config.mode})
            await self._close_interface()
            return False

    async def _auto_healing_loop(self) -> None:
        """Bucle infinito de auto-healing con Exponential Backoff y Jitter."""
        delay = self.config.reconnect_initial_delay_s

        while self._running:
            if not self._connected:
                success = await self._connect_hardware()
                if success:
                    delay = self.config.reconnect_initial_delay_s
                else:
                    jitter = random.uniform(0, self.config.reconnect_jitter_s)
                    sleep_time = min(delay + jitter, self.config.reconnect_max_delay_s)
                    logger.info("reconnect_waiting", extra={"seconds": round(sleep_time, 2)})
                    await asyncio.sleep(sleep_time)
                    delay = min(delay * 2.0, self.config.reconnect_max_delay_s)
            else:
                await asyncio.sleep(1.0)

    async def _heartbeat_loop(self) -> None:
        """Monitor de inactividad / heartbeat."""
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval_s)
            if self._connected:
                elapsed = time.monotonic() - self._last_rx_time
                if elapsed > self.config.heartbeat_timeout_s:
                    logger.warning(
                        "heartbeat_timeout_exceeded",
                        extra={"elapsed_s": round(elapsed, 1), "timeout_s": self.config.heartbeat_timeout_s}
                    )
                    # Forzar reconexión
                    self._connected = False
                    await self._close_interface()

    async def send_text(self, cmd: TxTextCommand) -> bool:
        """Envía un mensaje de texto a la radio de forma asíncrona no bloqueante."""
        if not self._connected or self._interface is None:
            logger.error("send_text_failed_not_connected")
            return False

        def _blocking_send():
            destination_id = cmd.to_node
            # Convertir !hex a entero si es necesario para el SDK de Meshtastic
            if destination_id.startswith("!"):
                try:
                    destination_id = int(destination_id[1:], 16)
                except ValueError:
                    pass

            self._interface.sendText(
                text=cmd.text,
                destinationId=destination_id,
                wantAck=cmd.want_ack,
                channelIndex=cmd.channel,
            )

        try:
            await self.loop.run_in_executor(None, _blocking_send)
            logger.info("text_sent_to_radio", extra={"to": cmd.to_node, "text": cmd.text})
            return True
        except Exception as e:
            logger.error("send_text_error", extra={"error": str(e), "to": cmd.to_node})
            return False
