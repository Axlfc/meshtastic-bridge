# Dockerfile para meshtastic-bridge
FROM python:3.11-slim-bookworm

# Instalar dependencias del sistema necesarias para acceso serie / USB
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    udev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar archivos de dependencias
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente de la aplicación
COPY . .

# Directorio para la persistencia de NodeDB
RUN mkdir -p /app/data

# Exponer el puerto de healthcheck
EXPOSE 8080

# Variable de entorno por defecto
ENV MESHBRIDGE_CONFIG_FILE=/app/config.yaml

# Sonda de salud integrada
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz')" || exit 1

# Comando de inicio
CMD ["python", "main.py"]
