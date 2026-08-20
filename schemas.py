"""
schemas.py
==========

Modelos Pydantic v2 que definen el contrato JSON estricto de todos los
eventos que entran y salen del bridge (sección 4 del spec).

Estos modelos son la "fuente de verdad" del formato de mensajes: tanto
`decoder.py` (Protobuf -> JSON) como `mqtt_client.py` (JSON -> tópicos MQTT)
y `webhook.py` dependen de ellos, así que cualquier cambio de formato debe
hacerse aquí primero.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SenderInfo(BaseModel):
    """Metadatos del nodo emisor, enriquecidos desde el NodeDB en memoria."""

    long_name: Optional[str] = None
    short_name: Optional[str] = None
    hw_model: Optional[str] = None


class SignalInfo(BaseModel):
    """Calidad de enlace del paquete recibido."""

    rssi: Optional[int] = None
    snr: Optional[float] = None
    hop_limit: Optional[int] = None
    hops_away: Optional[int] = None


class PlannedSiteMatch(BaseModel):
    """
    Enlace opcional con un emplazamiento troncal planificado por
    `mesh-propagation-planner` (proyecto hermano). Se adjunta cuando la
    posición reportada por un nodo cae dentro del radio de coincidencia
    (`planner.match_radius_m`) de alguno de los sitios exportados por el
    planificador (`*_resultado.json` / `*_nodos.csv`).
    """

    site_id: str
    distance_m: float
    elev_m: Optional[float] = None
    vecinos_visibles: Optional[int] = None
    added_for_redundancy: Optional[bool] = None


class TextPayload(BaseModel):
    text: str


class TextEvent(BaseModel):
    """Evento publicado en `meshtastic/rx/text`."""

    model_config = ConfigDict(json_schema_extra={"topic": "meshtastic/rx/text"})

    packet_id: int
    timestamp: datetime = Field(default_factory=_utcnow)
    from_node: str
    to_node: str = "^all"
    channel: int = 0
    sender: SenderInfo = Field(default_factory=SenderInfo)
    signal: SignalInfo = Field(default_factory=SignalInfo)
    payload: TextPayload
    planned_site: Optional[PlannedSiteMatch] = None


class TelemetryPayload(BaseModel):
    battery_level: Optional[int] = None
    voltage: Optional[float] = None
    channel_utilization: Optional[float] = None
    air_util_tx: Optional[float] = None
    temperature: Optional[float] = None
    relative_humidity: Optional[float] = None
    barometric_pressure: Optional[float] = None


class TelemetryEvent(BaseModel):
    """Evento publicado en `meshtastic/rx/telemetry`."""

    model_config = ConfigDict(json_schema_extra={"topic": "meshtastic/rx/telemetry"})

    packet_id: int
    timestamp: datetime = Field(default_factory=_utcnow)
    from_node: str
    sender: SenderInfo = Field(default_factory=SenderInfo)
    payload: TelemetryPayload
    planned_site: Optional[PlannedSiteMatch] = None


class PositionPayload(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None
    sats_in_view: Optional[int] = None
    ground_speed: Optional[float] = None


class PositionEvent(BaseModel):
    """Evento publicado en `meshtastic/rx/position`."""

    model_config = ConfigDict(json_schema_extra={"topic": "meshtastic/rx/position"})

    packet_id: int
    timestamp: datetime = Field(default_factory=_utcnow)
    from_node: str
    sender: SenderInfo = Field(default_factory=SenderInfo)
    signal: SignalInfo = Field(default_factory=SignalInfo)
    payload: PositionPayload
    planned_site: Optional[PlannedSiteMatch] = None


class NodeInfoPayload(BaseModel):
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    hw_model: Optional[str] = None
    mac_addr: Optional[str] = None


class NodeInfoEvent(BaseModel):
    """Evento publicado en `meshtastic/rx/nodeinfo`."""

    model_config = ConfigDict(json_schema_extra={"topic": "meshtastic/rx/nodeinfo"})

    packet_id: int
    timestamp: datetime = Field(default_factory=_utcnow)
    from_node: str
    payload: NodeInfoPayload
    planned_site: Optional[PlannedSiteMatch] = None


class BridgeStatus(BaseModel):
    """Will Message / estado de vida publicado en `meshtastic/status/bridge`."""

    status: Literal["online", "offline"]
    timestamp: datetime = Field(default_factory=_utcnow)
    transport: Optional[Literal["serial", "tcp"]] = None


class TxTextCommand(BaseModel):
    """Comando de entrada recibido en `meshtastic/tx/text`."""

    to_node: str = "^all"
    text: str
    channel: int = 0
    want_ack: bool = False


class HealthStatus(BaseModel):
    """Respuesta del endpoint HTTP `/healthz`."""

    status: Literal["healthy", "degraded"]
    radio_connected: bool
    transport: Literal["serial", "tcp"]
    port: str
    nodes_in_cache: int
    outbound_queue_size: int
    uptime_seconds: int


RxEvent = TextEvent | TelemetryEvent | PositionEvent | NodeInfoEvent