#!/usr/bin/env python3

# python datagen.py --data-file ride_requests.jsonl --config config.json
import argparse
import base64
import datetime
import io
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import avro.io
import avro.schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

VALUE_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "ride_requests_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {"name": "request_id", "type": "string"},
        {"name": "customer_email", "type": "string"},
        {"name": "pickup_zone", "type": "string"},
        {"name": "drop_off_zone", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "number_of_passengers", "type": "int"},
        {"name": "request_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})

WINDOW_SIZE_MS = 5 * 60 * 1000

def _extract_ts(line: str, schema) -> int:
    record = json.loads(line)
    raw_value = base64.b64decode(record["value"]) if record.get("value") else None
    if raw_value is None or len(raw_value) < 5:
        return 0
    reader = avro.io.DatumReader(schema)
    decoder = avro.io.BinaryDecoder(io.BytesIO(raw_value[5:]))
    val = reader.read(decoder)
    ts = val["request_ts"]
    if isinstance(ts, datetime.datetime):
        ts = int(ts.timestamp() * 1000)
    return ts

def compute_timestamp_offset(lines: List[str]) -> tuple:
    schema = avro.schema.parse(VALUE_SCHEMA_STR)
    max_ts = 0
    for line in lines:
        ts = _extract_ts(line, schema)
        if ts > max_ts:
            max_ts = ts
    
    now_ms = int(time.time() * 1000)
    aligned_end = (now_ms // WINDOW_SIZE_MS) * WINDOW_SIZE_MS
    num_windows = 288
    aligned_start = aligned_end - (num_windows * WINDOW_SIZE_MS)
    
    watermark_buffer_ms = 10_000
    offset_ms = (aligned_end + watermark_buffer_ms) - max_ts
    return offset_ms, aligned_start

def decode_avro_bytes(raw_bytes: bytes, schema_str: str) -> Any:
    if len(raw_bytes) < 5:
        raise ValueError(f"Avro payload too short ({len(raw_bytes)} bytes)")
    avro_payload = raw_bytes[5:]
    schema = avro.schema.parse(schema_str)
    reader = avro.io.DatumReader(schema)
    decoder = avro.io.BinaryDecoder(io.BytesIO(avro_payload))
    result = reader.read(decoder)
    if isinstance(result, dict):
        for key, val in result.items():
            if isinstance(val, datetime.datetime):
                result[key] = int(val.timestamp() * 1000)
    return result

class Lab3DataPublisher:
    def __init__(self, config: Dict, dry_run=False):
        self.dry_run = dry_run
        
        sr_conf = {
            "url": config["sr_url"], 
            "basic.auth.user.info": f"{config['sr_key']}:{config['sr_secret']}"
        }
        sr_client = SchemaRegistryClient(sr_conf)
        
        self.value_serializer = AvroSerializer(sr_client, VALUE_SCHEMA_STR)
        self.key_serializer = StringSerializer("utf_8")
        
        self.producer_config = {
            "bootstrap.servers": config["bootstrap_servers"],
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": config["kafka_key"],
            "sasl.password": config["kafka_secret"],
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
        }
        self.producer = None if dry_run else Producer(self.producer_config)

    def publish_message(self, message_data: Dict, topic: str, ts_offset_ms: int, start_ms: int) -> str:
        raw_val = base64.b64decode(message_data["value"]) if message_data.get("value") else None
        if not raw_val:
            return "skipped"
        
        decoded_val = decode_avro_bytes(raw_val, VALUE_SCHEMA_STR)
        original_ts = decoded_val["request_ts"]
        rebased_ts = original_ts + ts_offset_ms
        
        if rebased_ts < start_ms:
            return "skipped"
            
        decoded_val["request_ts"] = rebased_ts
        raw_key = base64.b64decode(message_data["key"]) if message_data.get("key") else "unknown_key"
        key_str = raw_key.decode("utf-8", errors="ignore") if isinstance(raw_key, bytes) else str(raw_key)
        
        if not self.dry_run:
            from confluent_kafka.serialization import SerializationContext, MessageField
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(key_str, SerializationContext(topic, MessageField.KEY)),
                value=self.value_serializer(decoded_val, SerializationContext(topic, MessageField.VALUE)),
                timestamp=rebased_ts
            )
        return "ok"

    def publish_jsonl_file(self, file_path: Path, topic: str) -> dict:
        with open(file_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]
            
        logger.info("Calculating timestamp adjustments...")
        ts_offset_ms, start_ms = compute_timestamp_offset(lines)
        
        logger.info("Sorting records chronologically...")
        sort_schema = avro.schema.parse(VALUE_SCHEMA_STR)
        lines_with_ts = [(ts_offset_ms + _extract_ts(line, sort_schema), line) for line in lines]
        lines_with_ts.sort(key=lambda x: x[0])
        lines = [line for _, line in lines_with_ts]
        
        results = {"total": len(lines), "success": 0, "skipped": 0, "failed": 0}
        
        for idx, line in enumerate(lines, 1):
            try:
                message_data = json.loads(line)
                status = self.publish_message(message_data, topic, ts_offset_ms, start_ms)
                if status == "ok":
                    results["success"] += 1
                elif status == "skipped":
                    results["skipped"] += 1
            except Exception as e:
                logger.error(f"Error processing line {idx}: {e}")
                results["failed"] += 1
                
            if not self.dry_run and idx % 100 == 0:
                self.producer.poll(0)
            if not self.dry_run and idx % 1000 == 0:
                self.producer.flush()
                logger.info(f"Progress: {idx}/{results['total']} sent.")
                
        if not self.dry_run and self.producer:
            self.producer.flush()
        return results

def main():
    parser = argparse.ArgumentParser(description="Standalone Local Data Publisher via JSON configuration")
    parser.add_argument("--data-file", type=Path, required=True, help="Path to ride_requests.jsonl")
    parser.add_argument("--config", type=Path, default=Path("config.json"), help="Path to auth config.json")
    parser.add_argument("--topic", default="ride_requests", help="Kafka Destination Topic")
    parser.add_argument("--dry-run", action="store_true")
    
    args = parser.parse_args()
    
    if not args.config.exists():
        logger.error(f"Configuration file not found: {args.config}")
        return 1
        
    with open(args.config, "r") as cf:
        try:
            config_data = json.load(cf)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse config JSON file: {e}")
            return 1

    # Verify required parameters exist
    required_keys = ["bootstrap_servers", "kafka_key", "kafka_secret", "sr_url", "sr_key", "sr_secret"]
    missing = [k for k in required_keys if k not in config_data]
    if missing:
        logger.error(f"Missing configuration properties in JSON: {missing}")
        return 1
    
    publisher = Lab3DataPublisher(config_data, args.dry_run)
    logger.info(f"Starting data publication to topic: {args.topic}")
    summary = publisher.publish_jsonl_file(args.data_file, args.topic)
    print(f"\n=== JOB SUMMARY ===\nTotal processed: {summary['total']}\nSuccess: {summary['success']}\nTrimmed/Skipped: {summary['skipped']}\nFailed: {summary['failed']}")

if __name__ == "__main__":
    sys.exit(main())