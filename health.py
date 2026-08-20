"""
health.py
=========

Sección 5 — Observabilidad y salud.

Servidor HTTP FastAPI que expone:
  - `GET /healthz`: Sonda liveness/readiness de Docker/K8s.
  - `GET /nodes`: Estado actual completo del NodeDB en memoria.
  - `GET /metrics`: Métricas de Prometheus (mensajes RX/TX, deduplicación drops, queue size).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from config import HealthConfig, Settings
from nodedb import NodeDB
from ratelimiter import OutboundQueue
from schemas import HealthStatus

logger = logging.getLogger("meshtastic_bridge.health")


class MetricsTracker:
    """Contadores y métricas simples para Prometheus y observabilidad."""

    def __init__(self) -> None:
        self.rx_messages = 0
        self.tx_messages = 0
        self.dedup_duplicates = 0
        self.dedup_seen = 0
        self.connection_retries = 0

    def generate_prometheus_metrics(self, nodes_count: int, outbound_qsize: int) -> str:
        lines = [
            "# HELP meshtastic_rx_messages_total Total received mesh packets decoded.",
            "# TYPE meshtastic_rx_messages_total counter",
            f"meshtastic_rx_messages_total {self.rx_messages}",
            "# HELP meshtastic_tx_messages_total Total transmitted mesh packets.",
            "# TYPE meshtastic_tx_messages_total counter",
            f"meshtastic_tx_messages_total {self.tx_messages}",
            "# HELP meshtastic_dedup_duplicates_total Total duplicate packets dropped.",
            "# TYPE meshtastic_dedup_duplicates_total counter",
            f"meshtastic_dedup_duplicates_total {self.dedup_duplicates}",
            "# HELP meshtastic_nodes_in_cache Total nodes in NodeDB cache.",
            "# TYPE meshtastic_nodes_in_cache gauge",
            f"meshtastic_nodes_in_cache {nodes_count}",
            "# HELP meshtastic_outbound_queue_size Pending messages in outbound queue.",
            "# TYPE meshtastic_outbound_queue_size gauge",
            f"meshtastic_outbound_queue_size {outbound_qsize}",
        ]
        return "\n".join(lines) + "\n"


def create_health_app(
    settings: Settings,
    nodedb: NodeDB,
    outbound_queue: OutboundQueue,
    is_radio_connected_fn: Callable[[], bool],
    metrics: MetricsTracker,
    start_time: float,
) -> FastAPI:
    app = FastAPI(title="meshtastic-bridge observability")

    @app.get("/healthz", response_model=HealthStatus)
    async def get_healthz():
        connected = is_radio_connected_fn()
        uptime = int(time.time() - start_time)
        status_str = "healthy" if connected else "degraded"

        health = HealthStatus(
            status=status_str,
            radio_connected=connected,
            transport=settings.transport.mode,
            port=settings.transport.serial_port if settings.transport.mode == "serial" else f"{settings.transport.tcp_host}:{settings.transport.tcp_port}",
            nodes_in_cache=nodedb.count(),
            outbound_queue_size=outbound_queue.qsize(),
            uptime_seconds=uptime,
        )

        status_code = 200 if connected else 503
        return JSONResponse(content=health.model_dump(mode="json"), status_code=status_code)

    @app.get("/nodes")
    async def get_nodes():
        all_nodes = nodedb.get_all_nodes()
        return {k: v.model_dump(mode="json") for k, v in all_nodes.items()}

    @app.get("/metrics")
    async def get_metrics():
        text = metrics.generate_prometheus_metrics(
            nodes_count=nodedb.count(),
            outbound_qsize=outbound_queue.qsize(),
        )
        return Response(content=text, media_type="text/plain; version=0.0.4")

    return app
