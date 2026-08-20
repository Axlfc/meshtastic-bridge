"""
deduplicator.py
================

R4 — Filtro de deduplicación de paquetes.

Las redes mesh (incluida Meshtastic, que hace flood-routing con
retransmisión limitada por `hop_limit`) reenvían el mismo paquete varias
veces. `PacketDeduplicator` combina dos estrategias:

1. **Buffer circular FIFO** de tamaño fijo (`window_size`, por defecto 500)
   con las claves `(packet_id, from_node)` más recientes — acota el uso
   de memoria sin importar el tráfico.
2. **TTL** (`ttl_seconds`, por defecto 30s) — una clave vista hace más de
   `ttl_seconds` ya no cuenta como duplicado, aunque siga en el buffer,
   porque en teoría un `packet_id` de 32 bits puede reaparecer legítimamente
   tras un reinicio de nodo.

Uso:
    dedup = PacketDeduplicator(window_size=500, ttl_seconds=30.0)
    if dedup.is_duplicate(packet_id, from_node):
        return  # descartar, no publicar
"""
from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class DedupStats:
    seen: int = 0
    duplicates: int = 0
    evicted: int = 0


class PacketDeduplicator:
    """Deduplicador de paquetes mesh basado en ventana deslizante + TTL."""

    def __init__(self, window_size: int = 500, ttl_seconds: float = 30.0) -> None:
        if window_size <= 0:
            raise ValueError("window_size debe ser > 0")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds debe ser > 0")

        self.window_size = window_size
        self.ttl_seconds = ttl_seconds

        # OrderedDict actúa como buffer FIFO: la clave más antigua está al
        # principio. Guardamos el timestamp de última vez visto como valor.
        self._seen: "OrderedDict[tuple[int, str], float]" = OrderedDict()
        self.stats = DedupStats()

    @staticmethod
    def _key(packet_id: int, from_node: str) -> tuple[int, str]:
        return (packet_id, from_node)

    def is_duplicate(self, packet_id: int, from_node: str, now: float | None = None) -> bool:
        """
        Devuelve True si el paquete ya fue procesado dentro del TTL vigente
        (y por tanto debe descartarse), False si es nuevo (y debe
        registrarse + publicarse).

        Esta llamada tiene efecto secundario: registra el paquete como
        visto. Se llama una sola vez por paquete recibido.
        """
        now = time.monotonic() if now is None else now
        self.stats.seen += 1
        key = self._key(packet_id, from_node)

        last_seen = self._seen.get(key)
        if last_seen is not None and (now - last_seen) < self.ttl_seconds:
            self.stats.duplicates += 1
            # Refrescamos el timestamp y la posición FIFO
            self._seen.move_to_end(key)
            self._seen[key] = now
            return True

        # Nuevo paquete (o el mismo packet_id reapareciendo tras expirar el TTL)
        self._seen[key] = now
        self._seen.move_to_end(key)
        self._evict_overflow()
        return False

    def _evict_overflow(self) -> None:
        while len(self._seen) > self.window_size:
            self._seen.popitem(last=False)
            self.stats.evicted += 1

    def __len__(self) -> int:
        return len(self._seen)

    def purge_expired(self, now: float | None = None) -> int:
        """
        Limpieza opcional de entradas caducadas (no es estrictamente
        necesaria porque `is_duplicate` ya revalida el TTL, pero libera
        memoria del buffer antes de llenarse si el tráfico es escaso).
        """
        now = time.monotonic() if now is None else now
        expired = [k for k, ts in self._seen.items() if (now - ts) >= self.ttl_seconds]
        for k in expired:
            del self._seen[k]
        return len(expired)