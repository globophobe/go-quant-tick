import asyncio
import json
from datetime import datetime
from decimal import Decimal
from typing import Any


def default_json(value: Any) -> str:
    """Encode datetimes and decimals as strings."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


class PubSubPublisher:
    """Publish trade payloads to Google Pub/Sub."""

    def __init__(self, project_id: str, topic: str) -> None:
        """Create a Pub/Sub publisher for a topic."""
        try:
            from google.cloud import pubsub_v1
        except ImportError as exc:
            msg = "Install the GCP extra to use Pub/Sub: uv sync --extra gcp"
            raise RuntimeError(msg) from exc

        publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
        self.publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
        self.topic_path = self.publisher.topic_path(project_id, topic)

    async def __call__(self, payload: dict, timestamp: float) -> None:
        """Publish a pipeline callback payload."""
        await self.publish(payload)

    async def publish(self, payload: dict) -> None:
        """Publish a payload and wait for acknowledgement."""
        data = json.dumps(payload, default=default_json, separators=(",", ":")).encode()
        ordering_key = self.get_ordering_key(payload)
        future = self.publisher.publish(
            self.topic_path,
            data,
            ordering_key=ordering_key,
            exchange=str(payload.get("exchange", "")),
            symbol=str(payload.get("symbol", "")),
        )
        await asyncio.to_thread(future.result)

    def get_ordering_key(self, payload: dict) -> str:
        """Order messages independently per exchange symbol."""
        exchange = payload.get("exchange", "")
        symbol = payload.get("symbol", "")
        return f"{exchange}:{symbol}"
