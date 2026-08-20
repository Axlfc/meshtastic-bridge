# meshtastic-bridge 📡🔗

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111%2B-009688.svg)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/docker-multi--arch-blue.svg)](https://www.docker.com/)
[![License: GPL-3.0](https://img.shields.io/badge/License-GPL--3.0-blue.svg)](LICENSE)

**`meshtastic-bridge`** es un demonio *headless*, ultrarresistente y de alto rendimiento diseñado para actuar como puente bidireccional entre hardware de radiofrecuencia Mesh (**dispositivos compatibles con Meshtastic** mediante interfaz Serie UART/USB o TCP/WiFi) y sistemas externos a través de **MQTT** y **Webhooks HTTP**.

---

## 🏗️ Arquitectura de Sistema

```text
[ Red LoRa / Meshtastic ]
        │
        ▼
┌────────────────────────────────────────────────────────────────────────┐
│                          meshtastic-bridge                             │
│                                                                        │
│  ┌──────────────────────┐   ┌───────────────────┐   ┌───────────────┐  │
│  │ Serial/TCP Driver    │──►│ Protobuf Decoder  │──►│ Deduplicator  │  │
│  │ (Auto-Reconnect)     │   │ & Router          │   │ (Sliding Win) │  │
│  └──────────────────────┘   └───────────────────┘   └───────────────┘  │
│                                                             │          │
│  ┌──────────────────────┐   ┌───────────────────┐           ▼          │
│  │ Outbound Queue       │◄──│ NodeDB Cache      │◄──────────┼──────────┤
│  │ (Rate Limiter /      │   │ (In-Memory Table) │           │          │
│  │  Leaky Bucket)       │   └───────────────────┘           │          │
│  └──────────────────────┘                                   ▼          │
│             ▲                                    ┌──────────────────┐  │
│             └────────────────────────────────────│ MQTT Publisher   │  │
│                                                  │ / Webhook Driver │  │
│                                                  └──────────────────┘  │
└───────────────────────────────────────────────────────────┬────────────┘
        │
        ▼
[ MQTT Broker / Webhooks / Dashboards ]
```

---

## ✨ Características Principales

* 🔌 **Hardware Transport Engine (Auto-Healing):**
  * Soporte para conexión **SERIAL** (`/dev/ttyUSB*`, `/dev/ttyACM*`) y **TCP** (`192.168.x.x:4403`).
  * Reintentos automáticos e infinitos con **Exponential Backoff** y *jitter* (evita thundering herd).
  * **Heartbeat Monitor** para detectar puertos serie colgados/zombies.
* 📦 **Decoder Protobuf to JSON:**
  * Soporte nativo para `TEXT_MESSAGE_APP`, `TELEMETRY_APP`, `POSITION_APP` y `NODEINFO_APP`.
  * Normalización automática de Node IDs a formato hexadecimal (ej. `!a1b2c3d4`).
* 🧠 **NodeDB In-Memory & Persistencia Atómica:**
  * Mantiene la tabla de nodos de la red mesh en memoria.
  * Enriquece automáticamente eventos entrantes con `sender.long_name`, `sender.short_name` y `sender.hw_model`.
  * Guardado atómico periódico en `data/nodedb.json` (usando escritura `.tmp` + `os.replace`).
* 🧹 **Filtro Deduplicador (Sliding Window + TTL):**
  * Descarta retransmisiones duplicadas típicas del flood routing de LoRa en función de un buffer circular y un TTL configurable.
* 🪣 **Airtime Guard & Outbound Rate Limiter:**
  * Cola de salida con algoritmo **Leaky Bucket** para mensajes enviados a la radio (`meshtastic/tx/text`).
  * Garantiza intervalos mínimos entre transmisiones para cumplir regulaciones de *Duty Cycle* (EU868 / US915).
* 🗺️ **Integración con `mesh-propagation-planner`:**
  * Importa topologías troncales planificadas (`*_resultado.json` o `*_nodos.csv`).
  * Asocia automáticamente coordenadas recibidas con emplazamientos planificados dentro del radio de coincidencia (`planned_site`).
* 🩺 **Observabilidad & Healthcheck HTTP (FastAPI):**
  * `GET /healthz`: Sonda liveness/readiness para Docker/K8s (`200 OK` si hay radio, `503` si se desconectó).
  * `GET /nodes`: Dump JSON del estado actual de NodeDB.
  * `GET /metrics`: Exposición de métricas en formato texto plano para Prometheus.

---

## 🚀 Instalación y Despliegue

### Opción 1: Docker Compose (Recomendado)

1. Clona el repositorio:
   ```bash
   git clone https://github.com/your-org/meshtastic-bridge.git
   cd meshtastic-bridge
   ```

2. Copia la configuración de ejemplo:
   ```bash
   cp config.example.yaml config.yaml
   ```

3. Inicia el contenedor con Docker Compose:
   ```bash
   docker-compose up -d
   ```

### Opción 2: Instalación Local en Python (Entorno Virtual)

1. Requisitos: **Python 3.11+**
2. Crear entorno virtual e instalar dependencias:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Iniciar el demonio:
   ```bash
   python main.py
   ```

---

## ⚙️ Configuración (12-Factor App)

La configuración sigue la metodología **12-Factor App** con la siguiente jerarquía de precedencia:

$$\text{Valores por defecto} < \text{config.yaml} < \text{.env} < \text{Variables de entorno}$$

Todas las variables de entorno usan el prefijo `MESHBRIDGE_` y `__` para anidación.

### Ejemplo de `config.yaml`
```yaml
transport:
  mode: "serial" # 'serial' o 'tcp'
  serial_port: "/dev/ttyUSB0"
  tcp_host: "192.168.1.50"
  tcp_port: 4403

rate_limit:
  min_interval_s: 5.0
  max_queue_size: 200

mqtt:
  host: "localhost"
  port: 1883
  topic_prefix: "meshtastic"

webhook:
  enabled: false
  urls:
    - "http://localhost:5000/webhook"

nodedb:
  persist_path: "data/nodedb.json"

planner:
  enabled: false
  planned_topology_path: "data/topologia_planificada.json"
  match_radius_m: 300.0
```

### Ejemplo con Variables de Entorno
```bash
export MESHBRIDGE_TRANSPORT__MODE=tcp
export MESHBRIDGE_TRANSPORT__TCP_HOST=192.168.1.50
export MESHBRIDGE_MQTT__HOST=broker.hivemq.com
python main.py
```

---

## 📡 Esquemas MQTT

### Tópicos de Salida (Radio $\rightarrow$ MQTT)

* **`meshtastic/rx/text`**: Mensajes de texto recibidos.
  ```json
  {
    "packet_id": 12345678,
    "timestamp": "2026-08-20T14:20:00Z",
    "from_node": "!a1b2c3d4",
    "to_node": "^all",
    "channel": 0,
    "sender": {
      "long_name": "Nodo Cima Montaña",
      "short_name": "CIMA",
      "hw_model": "HELTEC_V3"
    },
    "signal": {
      "rssi": -95,
      "snr": 6.25,
      "hop_limit": 3,
      "hops_away": 1
    },
    "payload": {
      "text": "Prueba de conectividad en la red mesh"
    }
  }
  ```

* **`meshtastic/rx/telemetry`**: Batería, voltaje y métricas de canal.
* **`meshtastic/rx/position`**: Coordenadas GPS y altitud.
* **`meshtastic/rx/nodeinfo`**: Nombre y metadatos del nodo.
* **`meshtastic/status/bridge`**: Will Message (`{"status": "online"}` / `{"status": "offline"}`).

### Tópicos de Entrada (MQTT $\rightarrow$ Radio)

* **`meshtastic/tx/text`**: Enviar mensaje de texto a la radio.
  ```json
  {
    "to_node": "^all",
    "text": "Mensaje desde Home Assistant a la Mesh",
    "channel": 0,
    "want_ack": false
  }
  ```

---

## 📊 Endpoints de Observabilidad

| Endpoint | Método | Descripción |
| :--- | :--- | :--- |
| `/healthz` | `GET` | Sonda de salud (`200 OK` si conectado, `503` si desconectado). |
| `/nodes` | `GET` | Estado completo del NodeDB en memoria. |
| `/metrics` | `GET` | Métricas en formato texto de Prometheus. |

### Ejemplo de respuesta `/healthz`
```json
{
  "status": "healthy",
  "radio_connected": true,
  "transport": "serial",
  "port": "/dev/ttyUSB0",
  "nodes_in_cache": 14,
  "outbound_queue_size": 0,
  "uptime_seconds": 3600
}
```

---

## 🧪 Pruebas Unitarias

Ejecuta el conjunto completo de pruebas con `pytest`:

```bash
python3 -m pytest -v
```

---

## 📄 Licencia

Este proyecto se distribuye bajo la licencia **GNU General Public License v3.0 (GPL-3.0)**. Ver `LICENSE` para más detalles.
