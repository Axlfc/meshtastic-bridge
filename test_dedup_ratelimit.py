"""
Pruebas para deduplicador y ratelimiter.
"""
import asyncio
import time
import pytest
from deduplicator import PacketDeduplicator
from ratelimiter import OutboundQueue

def test_deduplicator():
    dedup = PacketDeduplicator(window_size=3, ttl_seconds=1.0)

    # Nuevo paquete
    assert dedup.is_duplicate(packet_id=1, from_node="!a1") is False

    # Duplicado inmediato
    assert dedup.is_duplicate(packet_id=1, from_node="!a1") is True

    # Otro paquete distinto
    assert dedup.is_duplicate(packet_id=2, from_node="!a1") is False

    # Esperar expiración TTL
    time.sleep(1.1)
    assert dedup.is_duplicate(packet_id=1, from_node="!a1") is False

@pytest.mark.asyncio
async def test_outbound_queue_rate_limiting():
    queue = OutboundQueue[str](min_interval_s=0.1, max_queue_size=2)

    await queue.put("msg1")
    await queue.put("msg2")

    items = []
    t0 = time.monotonic()

    async for item in queue.drain():
        items.append(item)
        if len(items) == 2:
            break

    t1 = time.monotonic()
    assert items == ["msg1", "msg2"]
    # El intervalo mínimo entre dos envíos fue de 0.1s
    assert (t1 - t0) >= 0.08
