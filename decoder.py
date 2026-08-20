"""
decoder.py
==========

R2 — Motor de Deserialización y Ruteo (Protobuf to JSON / Dictionary to Pydantic Event).

Decodifica paquetes de la librería oficial de Meshtastic a eventos Pydantic
tipados y los enriquece con información de NodeDB y `mesh-propagation-planner`.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from config import Settings
from nodedb import NodeDB, format_node_id
from planner_import import PlannedSite, nearest_site
from schemas import (
    NodeInfoEvent,
    NodeInfoPayload,
    PlannedSiteMatch,
    PositionEvent,
    PositionPayload,
    RxEvent,
    SenderInfo,
    SignalInfo,
    TelemetryEvent,
    TelemetryPayload,
    TextEvent,
    TextPayload,
)

logger = logging.getLogger("meshtastic_bridge.decoder")


class PacketDecoder:
    """Decodifica paquetes nativos/dict de Meshtastic a objetos RxEvent Pydantic."""

    def __init__(
        self,
        nodedb: NodeDB,
        planned_sites: Optional[List[PlannedSite]] = None,
        match_radius_m: float = 300.0,
    ) -> None:
        self.nodedb = nodedb
        self.planned_sites = planned_sites or []
        self.match_radius_m = match_radius_m

    def _extract_signal_info(self, packet: Dict[str, Any]) -> SignalInfo:
        rssi = packet.get("rxRssi") if "rxRssi" in packet else packet.get("rssi")
        snr = packet.get("rxSnr") if "rxSnr" in packet else packet.get("snr")
        hop_limit = packet.get("hopLimit") if "hopLimit" in packet else packet.get("hop_limit")
        hops_away = packet.get("hopsAway") if "hopsAway" in packet else packet.get("hops_away")

        return SignalInfo(
            rssi=rssi,
            snr=snr,
            hop_limit=hop_limit,
            hops_away=hops_away,
        )

    def _match_planned_site(
        self, lat: Optional[float], lon: Optional[float]
    ) -> Optional[PlannedSiteMatch]:
        if lat is None or lon is None or not self.planned_sites:
            return None

        result = nearest_site(lat, lon, self.planned_sites)
        if not result:
            return None

        site, dist_m = result
        if dist_m <= self.match_radius_m:
            return PlannedSiteMatch(
                site_id=site.site_id,
                distance_m=round(dist_m, 2),
                elev_m=site.elev_m,
                vecinos_visibles=site.vecinos_visibles,
                added_for_redundancy=site.added_for_redundancy,
            )
        return None

    def decode_packet(self, packet: Dict[str, Any]) -> Optional[RxEvent]:
        """
        Decodifica un diccionario de paquete Meshtastic.
        Devuelve una instancia de RxEvent o None si el paquete no es de un tipo soportado.
        """
        try:
            from_id_raw = packet.get("fromId") or packet.get("from")
            if from_id_raw is None:
                logger.warning("packet_missing_from_id", extra={"packet": packet})
                return None

            from_node = format_node_id(from_id_raw)
            to_id_raw = packet.get("toId") or packet.get("to") or "^all"
            to_node = format_node_id(to_id_raw)
            packet_id = packet.get("id") or 0
            channel = packet.get("channel", 0)

            signal = self._extract_signal_info(packet)

            # Actualizar NodeDB con métricas de señal
            self.nodedb.update_node(
                node_id=from_node,
                rssi=signal.rssi,
                snr=signal.snr,
            )

            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")

            # Mapeo de portnum (string o int)
            portnum_str = str(portnum)

            # 1. TEXT_MESSAGE_APP
            if portnum_str in ("TEXT_MESSAGE_APP", "1", "1"):
                text = decoded.get("text", "")
                sender_info = self.nodedb.get_sender_info(from_node)

                # Intentar buscar planned site si conocemos las coordenadas del nodo emisor en NodeDB
                node_rec = self.nodedb.get_node(from_node)
                planned_site = None
                if node_rec and node_rec.latitude is not None and node_rec.longitude is not None:
                    planned_site = self._match_planned_site(node_rec.latitude, node_rec.longitude)

                return TextEvent(
                    packet_id=packet_id,
                    from_node=from_node,
                    to_node=to_node,
                    channel=channel,
                    sender=sender_info,
                    signal=signal,
                    payload=TextPayload(text=text),
                    planned_site=planned_site,
                )

            # 2. TELEMETRY_APP
            elif portnum_str in ("TELEMETRY_APP", "67"):
                telemetry = decoded.get("telemetry", {})
                device_metrics = telemetry.get("deviceMetrics", telemetry)
                environment_metrics = telemetry.get("environmentMetrics", {})

                batt = device_metrics.get("batteryLevel") or device_metrics.get("battery_level")
                volt = device_metrics.get("voltage")
                chan_util = device_metrics.get("channelUtilization") or device_metrics.get("channel_utilization")
                air_util = device_metrics.get("airUtilTx") or device_metrics.get("air_util_tx")

                temp = environment_metrics.get("temperature")
                rh = environment_metrics.get("relativeHumidity") or environment_metrics.get("relative_humidity")
                press = environment_metrics.get("barometricPressure") or environment_metrics.get("barometric_pressure")

                # Actualizar NodeDB con telemetría de batería/voltaje
                self.nodedb.update_node(
                    node_id=from_node,
                    battery_level=batt,
                    voltage=volt,
                )

                sender_info = self.nodedb.get_sender_info(from_node)
                node_rec = self.nodedb.get_node(from_node)
                planned_site = None
                if node_rec and node_rec.latitude is not None and node_rec.longitude is not None:
                    planned_site = self._match_planned_site(node_rec.latitude, node_rec.longitude)

                return TelemetryEvent(
                    packet_id=packet_id,
                    from_node=from_node,
                    sender=sender_info,
                    payload=TelemetryPayload(
                        battery_level=batt,
                        voltage=volt,
                        channel_utilization=chan_util,
                        air_util_tx=air_util,
                        temperature=temp,
                        relative_humidity=rh,
                        barometric_pressure=press,
                    ),
                    planned_site=planned_site,
                )

            # 3. POSITION_APP
            elif portnum_str in ("POSITION_APP", "3"):
                position = decoded.get("position", {})

                # Las coordenadas en Meshtastic pueden venir como flotantes o enteros escalados (* 1e7)
                lat = position.get("latitude") or position.get("latitudeI")
                if lat is not None and abs(lat) > 180:
                    lat = lat / 1e7

                lon = position.get("longitude") or position.get("longitudeI")
                if lon is not None and abs(lon) > 180:
                    lon = lon / 1e7

                alt = position.get("altitude") or position.get("altitude_m")
                sats = position.get("satsInView") or position.get("sats_in_view")
                speed = position.get("groundSpeed") or position.get("ground_speed")

                # Actualizar NodeDB con la posición del nodo
                self.nodedb.update_node(
                    node_id=from_node,
                    latitude=lat,
                    longitude=lon,
                    altitude_m=alt,
                )

                sender_info = self.nodedb.get_sender_info(from_node)
                planned_site = self._match_planned_site(lat, lon)

                return PositionEvent(
                    packet_id=packet_id,
                    from_node=from_node,
                    sender=sender_info,
                    signal=signal,
                    payload=PositionPayload(
                        latitude=lat,
                        longitude=lon,
                        altitude_m=alt,
                        sats_in_view=sats,
                        ground_speed=speed,
                    ),
                    planned_site=planned_site,
                )

            # 4. NODEINFO_APP
            elif portnum_str in ("NODEINFO_APP", "4"):
                user = decoded.get("user", {})
                long_name = user.get("longName") or user.get("long_name")
                short_name = user.get("shortName") or user.get("short_name")
                hw_model = user.get("hwModel") or user.get("hw_model")
                mac_addr = user.get("macaddr") or user.get("mac_addr")

                # Actualizar NodeDB con nombres y metadatos
                self.nodedb.update_node(
                    node_id=from_node,
                    long_name=long_name,
                    short_name=short_name,
                    hw_model=hw_model,
                    mac_addr=mac_addr,
                )

                node_rec = self.nodedb.get_node(from_node)
                planned_site = None
                if node_rec and node_rec.latitude is not None and node_rec.longitude is not None:
                    planned_site = self._match_planned_site(node_rec.latitude, node_rec.longitude)

                return NodeInfoEvent(
                    packet_id=packet_id,
                    from_node=from_node,
                    payload=NodeInfoPayload(
                        long_name=long_name,
                        short_name=short_name,
                        hw_model=hw_model,
                        mac_addr=mac_addr,
                    ),
                    planned_site=planned_site,
                )

            else:
                logger.debug("unsupported_portnum", extra={"portnum": portnum, "from_node": from_node})
                return None

        except Exception as e:
            logger.error("packet_decode_error", extra={"error": str(e), "packet": packet})
            return None
