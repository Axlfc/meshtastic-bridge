"""
ratelimiter.py
==============

R5 — Control de tráfico de salida (Airtime Guard).

LoRa está sujeto a restricciones regulatorias de duty cycle (en EU868,
normalmente 1% o 10% según sub-banda). `OutboundQueue` implementa un
**leaky bucket**: los mensajes salientes se encolan sin límite (hasta
`max_queue_size`) pero se drenan hacia la radio a un ritmo constante,
nunca por debajo de `min_interval_s` entre transmisiones.

Uso típico:
    queue = OutboundQueue(min_interval_s=5.0, max_queue_size=200)
    await queue.put(tx_command)
    ...
    async for cmd in queue.drain():
        await driver.send_text(cmd)   # nunca más rápido que min_interval_s
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator, Generic, TypeVar

logger = logging.getLogger("meshtastic_bridge.ratelimiter")

T = TypeVar("T")


class QueueFullError(Exception):
    """La cola de salida alcanzó `max_queue_size`; el mensaje se descarta."""


class OutboundQueue(Generic[T]):
    """
    Cola de salida con limitador leaky-bucket.

    - `put()` nunca bloquea la parte "productora" (MQTT/webhook handlers):
      si la cola está llena, se descarta el mensaje más antiguo (o el
      nuevo, según `drop_oldest`) y se cuenta como `dropped`.
    - `drain()` es un generador async pensado para ejecutarse como una
      única tarea de fondo que envía a la radio; asegura que entre dos
      `yield` consecutivos pasan al menos `min_interval_s` segundos.
    """

    def __init__(
        self,
        min_interval_s: float = 5.0,
        max_queue_size: int = 200,
        drop_oldest: bool = True,
    ) -> None:
        if min_interval_s < 0:
            raise ValueError("min_interval_s no puede ser negativo")
        self.min_interval_s = min_interval_s
        self.max_queue_size = max_queue_size
        self.drop_oldest = drop_oldest

        self._queue: asyncio.Queue[T] = asyncio.Queue(maxsize=max_queue_size)
        self._last_emit_monotonic: float | None = None
        self.dropped = 0
        self.sent = 0

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, item: T) -> bool:
        """
        Encola un mensaje saliente. Devuelve True si se encoló, False si
        se descartó por cola llena (después de intentar liberar hueco si
        `drop_oldest=True`).
        """
        if self._queue.full():
            if not self.drop_oldest:
                self.dropped += 1
                logger.warning("outbound_queue_full_drop_new", extra={"qsize": self.qsize()})
                return False
            try:
                discarded = self._queue.get_nowait()
                self.dropped += 1
                logger.warning(
                    "outbound_queue_full_drop_oldest",
                    extra={"qsize": self.qsize(), "discarded": repr(discarded)},
                )
            except asyncio.QueueEmpty:
                pass

        await self._queue.put(item)
        return True

    async def drain(self) -> AsyncIterator[T]:
        """
        Generador infinito que respeta el intervalo mínimo entre envíos.
        Debe consumirse desde una única tarea de fondo (el "leak" del
        leaky bucket).
        """
        while True:
            item = await self._queue.get()
            await self._respect_min_interval()
            self._last_emit_monotonic = time.monotonic()
            self.sent += 1
            yield item

    async def _respect_min_interval(self) -> None:
        if self._last_emit_monotonic is None or self.min_interval_s <= 0:
            return
        elapsed = time.monotonic() - self._last_emit_monotonic
        remaining = self.min_interval_s - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)