"""
webhook.py
==========

Salida adicional opcional vía HTTP POST para eventos entrantes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import httpx

from config import WebhookConfig
from schemas import RxEvent, get_event_topic

logger = logging.getLogger("meshtastic_bridge.webhook")


class WebhookDriver:
    """Envía eventos recibidos mediante HTTP POST a las URLs configuradas."""

    def __init__(self, config: WebhookConfig) -> None:
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        if self.config.enabled and self.config.urls:
            self._client = httpx.AsyncClient(timeout=self.config.timeout_s)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def send_event(self, event: RxEvent) -> None:
        """Envía el evento JSON vía HTTP POST a todas las URLs configuradas."""
        if not self.config.enabled or not self.config.urls or not self._client:
            return

        payload_dict = event.model_dump(mode="json")
        topic = get_event_topic(event)
        event_type = topic.split("/")[-1]

        # Verificar si el tipo de evento está en la lista de eventos habilitados
        if self.config.events and event_type not in self.config.events:
            return

        tasks = [
            self._post_to_url(url, payload_dict)
            for url in self.config.urls
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _post_to_url(self, url: str, payload: dict) -> None:
        if not self._client:
            return
        try:
            resp = await self._client.post(url, json=payload)
            if resp.status_code >= 400:
                logger.warning(
                    "webhook_post_failed",
                    extra={"url": url, "status_code": resp.status_code}
                )
            else:
                logger.debug("webhook_post_success", extra={"url": url, "status_code": resp.status_code})
        except Exception as e:
            logger.error("webhook_post_error", extra={"url": url, "error": str(e)})
