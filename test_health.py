"""
Pruebas para servidor de observabilidad y salud (health.py).
"""
import time
import pytest
from fastapi.testclient import TestClient
from config import Settings
from nodedb import NodeDB
from ratelimiter import OutboundQueue
from health import create_health_app, MetricsTracker

def test_health_app_healthy():
    settings = Settings()
    nodedb = NodeDB(persist_path=None)
    nodedb.update_node("!a1b2c3d4", long_name="Nodo Test")

    outbound_queue = OutboundQueue()
    metrics = MetricsTracker()
    start_time = time.time() - 100

    app = create_health_app(
        settings=settings,
        nodedb=nodedb,
        outbound_queue=outbound_queue,
        is_radio_connected_fn=lambda: True,
        metrics=metrics,
        start_time=start_time,
    )

    client = TestClient(app)

    resp = client.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["radio_connected"] is True
    assert data["nodes_in_cache"] == 1
    assert data["uptime_seconds"] >= 100

    nodes_resp = client.get("/nodes")
    assert nodes_resp.status_code == 200
    nodes_data = nodes_resp.json()
    assert "!a1b2c3d4" in nodes_data
    assert nodes_data["!a1b2c3d4"]["long_name"] == "Nodo Test"

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "meshtastic_rx_messages_total" in metrics_resp.text

def test_health_app_degraded():
    settings = Settings()
    nodedb = NodeDB(persist_path=None)
    outbound_queue = OutboundQueue()
    metrics = MetricsTracker()

    app = create_health_app(
        settings=settings,
        nodedb=nodedb,
        outbound_queue=outbound_queue,
        is_radio_connected_fn=lambda: False,
        metrics=metrics,
        start_time=time.time(),
    )

    client = TestClient(app)

    resp = client.get("/healthz")
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "degraded"
    assert data["radio_connected"] is False
