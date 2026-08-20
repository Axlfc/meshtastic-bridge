"""
config.py
=========

Modelo de configuración 12-Factor: carga por capas, con prioridad
creciente:

    valores por defecto  <  config.yaml  <  .env  <  variables de entorno

Todas las variables de entorno usan el prefijo `MESHBRIDGE_` y `__` como
separador de anidación, p.ej.:

    MESHBRIDGE_TRANSPORT__MODE=tcp
    MESHBRIDGE_MQTT__HOST=broker.local
    MESHBRIDGE_MQTT__PASSWORD=xxxxx

La ruta del YAML se controla con `MESHBRIDGE_CONFIG_FILE` (por defecto
`config.yaml` en el directorio de trabajo). Ver `config.example.yaml`.
"""
from __future__ import annotations

import os
from typing import Literal, Optional

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class TransportConfig(BaseModel):
    """R1 — Capa de conexión de hardware."""

    mode: Literal["serial", "tcp"] = "serial"
    serial_port: str = "/dev/ttyUSB0"
    tcp_host: str = "192.168.1.50"
    tcp_port: int = 4403

    # Auto-healing: backoff exponencial con jitter
    reconnect_initial_delay_s: float = 1.0
    reconnect_max_delay_s: float = 60.0
    reconnect_jitter_s: float = 0.5

    # Heartbeat monitor
    heartbeat_interval_s: float = 30.0
    heartbeat_timeout_s: float = 90.0


class DedupConfig(BaseModel):
    """R4 — Filtro de deduplicación."""

    window_size: int = 500
    ttl_seconds: float = 30.0


class RateLimitConfig(BaseModel):
    """R5 — Control de tráfico de salida (Airtime Guard)."""

    min_interval_s: float = 5.0
    bucket_capacity: int = 5
    max_queue_size: int = 200


class MqttConfig(BaseModel):
    """R6 — Publicación y suscripción MQTT."""

    host: str = "localhost"
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    tls: bool = False
    client_id: str = "meshtastic-bridge"
    topic_prefix: str = "meshtastic"
    keepalive: int = 60
    qos: int = 0
    reconnect_min_delay_s: float = 1.0
    reconnect_max_delay_s: float = 60.0


class WebhookConfig(BaseModel):
    """Salida adicional opcional vía HTTP POST."""

    enabled: bool = False
    urls: list[str] = Field(default_factory=list)
    timeout_s: float = 5.0
    events: list[str] = Field(
        default_factory=lambda: ["text", "telemetry", "position", "nodeinfo"]
    )


class HealthConfig(BaseModel):
    """Sección 5 — Observabilidad y salud."""

    host: str = "0.0.0.0"
    port: int = 8080


class NodeDbConfig(BaseModel):
    """R3 — Caché de estado de red."""

    persist_path: Optional[str] = "data/nodedb.json"
    persist_interval_s: float = 60.0


class PlannerConfig(BaseModel):
    """
    Compatibilidad con `mesh-propagation-planner`: importa la topología de
    nodos troncales planificada (`*_resultado.json` o `*_nodos.csv`) y
    la usa para enriquecer los eventos de posición/nodeinfo con el sitio
    planificado más cercano.
    """

    enabled: bool = False
    planned_topology_path: Optional[str] = None
    match_radius_m: float = 300.0


class LoggingConfig(BaseModel):
    level: str = "INFO"
    json_output: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MESHBRIDGE_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    transport: TransportConfig = Field(default_factory=TransportConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    mqtt: MqttConfig = Field(default_factory=MqttConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)
    health: HealthConfig = Field(default_factory=HealthConfig)
    nodedb: NodeDbConfig = Field(default_factory=NodeDbConfig)
    planner: PlannerConfig = Field(default_factory=PlannerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_file = os.environ.get("MESHBRIDGE_CONFIG_FILE", "config.yaml")
        sources: list[PydanticBaseSettingsSource] = [init_settings]

        if os.path.exists(yaml_file):
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=yaml_file))

        # env_settings/dotenv_settings tienen prioridad sobre el YAML
        sources.extend([dotenv_settings, env_settings, file_secret_settings])
        return tuple(sources)


def load_settings() -> Settings:
    """Punto de entrada único para obtener la configuración resuelta."""
    return Settings()