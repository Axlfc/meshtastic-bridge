"""
nodedb.py
=========

R3 — Caché de estado de red (NodeDB In-Memory).

Mantiene la tabla de nodos conocidos de la red mesh en memoria, con
persistencia atómica opcional en disco (por defecto `data/nodedb.json`).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from schemas import SenderInfo

logger = logging.getLogger("meshtastic_bridge.nodedb")


def format_node_id(node_id: int | str) -> str:
    """
    Normaliza el ID de nodo al formato hexadecimal de Meshtastic con prefijo '!'.
    Ejemplos:
        123456789 -> "!075bcd15"
        "075bcd15" -> "!075bcd15"
        "!075bcd15" -> "!075bcd15"
        "^all" -> "^all"
    """
    if isinstance(node_id, str):
        node_id_str = node_id.strip()
        if node_id_str.startswith("^") or node_id_str == "broadcast":
            return node_id_str
        if node_id_str.startswith("!"):
            return node_id_str.lower()
        if node_id_str.startswith("0x") or node_id_str.startswith("0X"):
            return f"!{node_id_str[2:].lower()}"
        try:
            val = int(node_id_str)
            return f"!{val:08x}"
        except ValueError:
            return f"!{node_id_str.lower()}"
    elif isinstance(node_id, int):
        return f"!{node_id:08x}"
    return str(node_id)


class NodeRecord(BaseModel):
    """Estructura de un registro de nodo en NodeDB."""

    node_id: str
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    hw_model: Optional[str] = None
    mac_addr: Optional[str] = None
    last_seen: Optional[datetime] = None
    last_rssi: Optional[int] = None
    last_snr: Optional[float] = None
    battery_level: Optional[int] = None
    voltage: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None

    def to_sender_info(self) -> SenderInfo:
        return SenderInfo(
            long_name=self.long_name,
            short_name=self.short_name,
            hw_model=self.hw_model,
        )


class NodeDB:
    """Tabla en memoria de nodos Mesh con sincronización atómica a disco."""

    def __init__(
        self,
        persist_path: Optional[str | Path] = "data/nodedb.json",
        persist_interval_s: float = 60.0,
    ) -> None:
        self.persist_path = Path(persist_path) if persist_path else None
        self.persist_interval_s = persist_interval_s
        self._nodes: Dict[str, NodeRecord] = {}
        self._save_task: Optional[asyncio.Task] = None
        self._dirty: bool = False

        if self.persist_path and self.persist_path.exists():
            self.load_from_disk()

    def update_node(
        self,
        node_id: int | str,
        long_name: Optional[str] = None,
        short_name: Optional[str] = None,
        hw_model: Optional[str] = None,
        mac_addr: Optional[str] = None,
        rssi: Optional[int] = None,
        snr: Optional[float] = None,
        battery_level: Optional[int] = None,
        voltage: Optional[float] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        altitude_m: Optional[float] = None,
    ) -> NodeRecord:
        """
        Crea o actualiza parcialmente un nodo en NodeDB.
        Cualquier campo `None` no sobrescribirá un valor previamente existente.
        """
        formatted_id = format_node_id(node_id)
        existing = self._nodes.get(formatted_id)

        now = datetime.now(timezone.utc)

        if existing:
            if long_name is not None:
                existing.long_name = long_name
            if short_name is not None:
                existing.short_name = short_name
            if hw_model is not None:
                existing.hw_model = hw_model
            if mac_addr is not None:
                existing.mac_addr = mac_addr
            if rssi is not None:
                existing.last_rssi = rssi
            if snr is not None:
                existing.last_snr = snr
            if battery_level is not None:
                existing.battery_level = battery_level
            if voltage is not None:
                existing.voltage = voltage
            if latitude is not None:
                existing.latitude = latitude
            if longitude is not None:
                existing.longitude = longitude
            if altitude_m is not None:
                existing.altitude_m = altitude_m
            existing.last_seen = now
            record = existing
        else:
            record = NodeRecord(
                node_id=formatted_id,
                long_name=long_name,
                short_name=short_name,
                hw_model=hw_model,
                mac_addr=mac_addr,
                last_seen=now,
                last_rssi=rssi,
                last_snr=snr,
                battery_level=battery_level,
                voltage=voltage,
                latitude=latitude,
                longitude=longitude,
                altitude_m=altitude_m,
            )
            self._nodes[formatted_id] = record

        self._dirty = True
        return record

    def get_node(self, node_id: int | str) -> Optional[NodeRecord]:
        formatted_id = format_node_id(node_id)
        return self._nodes.get(formatted_id)

    def get_sender_info(self, node_id: int | str) -> SenderInfo:
        record = self.get_node(node_id)
        if record:
            return record.to_sender_info()
        return SenderInfo()

    def get_all_nodes(self) -> Dict[str, NodeRecord]:
        return dict(self._nodes)

    def count(self) -> int:
        return len(self._nodes)

    def save_to_disk(self) -> None:
        """Persistencia atómica escribiendo en archivo temporal y reemplazando."""
        if not self.persist_path:
            return

        try:
            self.persist_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self.persist_path.with_suffix(".tmp")

            data = {
                k: v.model_dump(mode="json")
                for k, v in self._nodes.items()
            }

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            os.replace(tmp_path, self.persist_path)
            self._dirty = False
            logger.debug("nodedb_saved_to_disk", extra={"path": str(self.persist_path), "nodes": len(self._nodes)})
        except Exception as e:
            logger.error("nodedb_save_error", extra={"error": str(e)})

    def load_from_disk(self) -> None:
        if not self.persist_path or not self.persist_path.exists():
            return

        try:
            with open(self.persist_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for node_id, node_data in data.items():
                record = NodeRecord.model_validate(node_data)
                self._nodes[node_id] = record

            logger.info("nodedb_loaded_from_disk", extra={"nodes": len(self._nodes)})
        except Exception as e:
            logger.error("nodedb_load_error", extra={"error": str(e)})

    async def start_periodic_save(self) -> None:
        if not self.persist_path or self.persist_interval_s <= 0:
            return

        async def _save_loop():
            while True:
                await asyncio.sleep(self.persist_interval_s)
                if self._dirty:
                    self.save_to_disk()

        self._save_task = asyncio.create_task(_save_loop())

    async def stop(self) -> None:
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass
        if self._dirty:
            self.save_to_disk()
