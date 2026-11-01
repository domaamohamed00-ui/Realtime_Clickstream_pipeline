"""
Pillar 02: High-Velocity Stream Ingestion & Immutable Raw Landing with DLQ.
Consumes from Kafka using aiokafka, micro-batches raw events, and flushes to MinIO Bronze S3.
"""

import asyncio
import gzip
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List
import boto3
from botocore.exceptions import ClientError
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [IngestionBronze] %(message)s"
)
logger = logging.getLogger("BronzeIngestion")

# Configuration
KAFKA_BOOTSTRAP = "localhost:9094"
TOPIC_RAW = "clickstream-raw"
TOPIC_DLQ = "clickstream-dlq"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadminpassword"
BRONZE_BUCKET = "lakehouse-bronze"

BATCH_SIZE = 50          # Flush every 50 events
FLUSH_INTERVAL_SEC = 5   # Flush at least every 5 seconds

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

class RawBronzeIngestor:
    def __init__(self):
        self.s3 = get_s3_client()
        self.consumer: AIOKafkaConsumer = None
        self.producer_dlq: AIOKafkaProducer = None
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush = time.time()

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            TOPIC_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="bronze-landing-consumer-group",
            value_deserializer=lambda m: m.decode("utf-8")
        )
        self.producer_dlq = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        await self.consumer.start()
        await self.producer_dlq.start()
        logger.info("Bronze Ingestion Worker listening on Kafka topic '%s'...", TOPIC_RAW)

        try:
            while True:
                # Poll with 1-second timeout to allow regular interval flushes
                result = await self.consumer.getmany(timeout_ms=1000, max_records=BATCH_SIZE)
                for tp, messages in result.items():
                    for message in messages:
                        raw_val = message.value
                        try:
                            event = json.loads(raw_val)
                            # Sanity check for critical identifiers
                            if not event.get("event_id") or not event.get("session_id") or not event.get("event_type"):
                                raise ValueError("Missing critical primary identifiers in event payload")
                            self.buffer.append(event)
                        except Exception as ex:
                            await self.route_to_dlq(raw_val, str(ex))

                # Flush condition: batch threshold reached or time interval elapsed
                if len(self.buffer) >= BATCH_SIZE or (time.time() - self.last_flush >= FLUSH_INTERVAL_SEC and len(self.buffer) > 0):
                    self.flush_batch_with_retry()

        except asyncio.CancelledError:
            logger.info("Shutdown requested. Flushing remaining buffer...")
            self.flush_batch_with_retry()
        finally:
            if self.consumer:
                await self.consumer.stop()
            if self.producer_dlq:
                await self.producer_dlq.stop()

    async def route_to_dlq(self, raw_str: str, error_msg: str):
        """Dead Letter Queue: Captures malformed events into Kafka DLQ and MinIO DLQ."""
        dlq_payload = {
            "failed_payload": raw_str,
            "error_reason": error_msg,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_topic": TOPIC_RAW
        }
        logger.warning("Routing corrupted record to DLQ: %s", error_msg)
        
        # 1. Forward to Kafka DLQ Topic
        if self.producer_dlq:
            await self.producer_dlq.send_and_wait(TOPIC_DLQ, dlq_payload)
        
        # 2. Persist to MinIO DLQ folder
        now = datetime.now(timezone.utc)
        dlq_key = f"dlq/date={now.strftime('%Y-%m-%d')}/error_{now.strftime('%H%M%S_%f')}.json"
        try:
            self.s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=dlq_key,
                Body=json.dumps(dlq_payload, indent=2).encode("utf-8")
            )
        except Exception as e:
            logger.error("Failed to write to MinIO DLQ: %s", e)

    def flush_batch_with_retry(self, max_retries: int = 3):
        """Flushes buffered records to S3 with exponential backoff."""
        if not self.buffer:
            return

        now = datetime.now(timezone.utc)
        s3_key = (
            f"raw_events/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"hour={now.strftime('%H')}/"
            f"clickstream_batch_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl.gz"
        )

        lines = "\n".join([json.dumps(ev) for ev in self.buffer])
        compressed_body = gzip.compress(lines.encode("utf-8"))

        delay = 1.0
        for attempt in range(1, max_retries + 1):
            try:
                self.s3.put_object(
                    Bucket=BRONZE_BUCKET,
                    Key=s3_key,
                    Body=compressed_body,
                    ContentEncoding="gzip",
                    ContentType="application/json"
                )
                logger.info("[SUCCESS] Flushed %d raw events to s3://%s/%s", len(self.buffer), BRONZE_BUCKET, s3_key)
                self.buffer.clear()
                self.last_flush = time.time()
                return
            except ClientError as e:
                logger.error("S3 upload attempt %d/%d failed: %s. Retrying in %.1fs...", attempt, max_retries, e, delay)
                time.sleep(delay)
                delay *= 2

        logger.critical("Failed to flush batch of %d records after %d attempts!", len(self.buffer), max_retries)

if __name__ == "__main__":
    ingestor = RawBronzeIngestor()
    try:
        asyncio.run(ingestor.start())
    except KeyboardInterrupt:
        logger.info("Process stopped.")
