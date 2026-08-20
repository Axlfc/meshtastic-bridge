"""
planner_import.py
==================

Capa de compatibilidad con el proyecto hermano
`mesh-propagation-planner` (https://github.com/Axlfc/mesh-propagation-planner).

Ese proyecto calcula, a partir de un DEM, qué emplazamientos son óptimos
para los nodos troncales de una mesh Meshtastic 868MHz (line-of-sight +
despeje de Fresnel + cobertura + redundancia de vecinos) y exporta el
resultado como:

    <prefix>_nodos.csv
        id,lat,lon,elev_m,x_utm31n,y_utm31n,vecinos_visibles,añadido_por_redundancia

    <prefix>_resultado.json
        {
          "n_nodos_troncales": int,
          "cobertura_pct": float,
          "min_vecinos_objetivo": int,
          "freq_mhz": float,
          "nodos": [ {mismos campos que el CSV} ]
        }

`meshtastic-bridge` no decide *dónde* poner los nodos (eso es
responsabilidad del planner) — solo consume su salida en modo lectura
para, una vez la radio está desplegada en campo, poder responder a la
pregunta "¿este nodo físico que acaba de aparecer en la mesh corresponde
a un emplazamiento planificado, y a cuál?". Eso permite:

  - Verificar en campo que un nodo troncal quedó donde tocaba (o a qué
    distancia del plan quedó).
  - Enriquecer los eventos de posición/nodeinfo salientes con el
    `site_id` planificado (p.ej. "TRUNK-03") en vez de solo el ID hex de
    hardware, útil en dashboards y en los propios logs.

El CSV es la única salida garantizada incluso sin `pandas`; el JSON es
preferible cuando está disponible porque ya viene tipado.
"""
from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PlannedSite:
    """Un nodo troncal planificado, tal como lo exporta el planner."""

    site_id: str
    lat: float
    lon: float
    elev_m: float | None = None
    vecinos_visibles: int | None = None
    added_for_redundancy: bool | None = None


class PlannerImportError(Exception):
    """La ruta indicada no es un export válido de mesh-propagation-planner."""


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "sí", "si"}


def load_planned_topology(path: str | Path) -> list[PlannedSite]:
    """
    Carga la topología planificada desde un `*_resultado.json` o un
    `*_nodos.csv` generado por `plan_mesh_tarragones.py`. Detecta el
    formato por la extensión del archivo.
    """
    p = Path(path)
    if not p.exists():
        raise PlannerImportError(f"No existe el archivo de topología planificada: {p}")

    if p.suffix.lower() == ".json":
        return _load_json(p)
    if p.suffix.lower() == ".csv":
        return _load_csv(p)
    raise PlannerImportError(
        f"Extensión no soportada '{p.suffix}': se esperaba .json (*_resultado.json) "
        f"o .csv (*_nodos.csv) de mesh-propagation-planner"
    )


def _load_json(p: Path) -> list[PlannedSite]:
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise PlannerImportError(f"JSON inválido en {p}: {e}") from e

    nodos = data.get("nodos")
    if nodos is None:
        raise PlannerImportError(
            f"{p} no parece un *_resultado.json de mesh-propagation-planner "
            f"(falta la clave 'nodos')"
        )

    sites = []
    for n in nodos:
        try:
            sites.append(
                PlannedSite(
                    site_id=str(n["id"]),
                    lat=float(n["lat"]),
                    lon=float(n["lon"]),
                    elev_m=_maybe_float(n.get("elev_m")),
                    vecinos_visibles=_maybe_int(n.get("vecinos_visibles")),
                    added_for_redundancy=n.get("añadido_por_redundancia"),
                )
            )
        except (KeyError, TypeError, ValueError) as e:
            raise PlannerImportError(f"Entrada 'nodos' inválida en {p}: {n!r} ({e})") from e
    return sites


def _load_csv(p: Path) -> list[PlannedSite]:
    sites = []
    with p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"id", "lat", "lon"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise PlannerImportError(
                f"{p} no parece un *_nodos.csv de mesh-propagation-planner "
                f"(cabecera encontrada: {reader.fieldnames})"
            )
        for row in reader:
            try:
                sites.append(
                    PlannedSite(
                        site_id=str(row["id"]),
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                        elev_m=_maybe_float(row.get("elev_m")),
                        vecinos_visibles=_maybe_int(row.get("vecinos_visibles")),
                        added_for_redundancy=(
                            _parse_bool(row["añadido_por_redundancia"])
                            if row.get("añadido_por_redundancia") not in (None, "")
                            else None
                        ),
                    )
                )
            except (KeyError, ValueError) as e:
                raise PlannerImportError(f"Fila inválida en {p}: {row!r} ({e})") from e
    return sites


def _maybe_float(v) -> float | None:
    return None if v in (None, "") else float(v)


def _maybe_int(v) -> int | None:
    return None if v in (None, "") else int(v)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en metros entre dos puntos lat/lon (WGS84 esférico)."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def nearest_site(
    lat: float, lon: float, sites: list[PlannedSite]
) -> tuple[PlannedSite, float] | None:
    """
    Devuelve `(sitio_más_cercano, distancia_m)` de entre `sites`, o None
    si `sites` está vacío. Búsqueda lineal — el número de nodos troncales
    de una comarca es pequeño (decenas), no hace falta un índice espacial.
    """
    if not sites:
        return None
    best = min(sites, key=lambda s: haversine_m(lat, lon, s.lat, s.lon))
    return best, haversine_m(lat, lon, best.lat, best.lon)