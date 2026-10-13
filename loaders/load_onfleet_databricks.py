"""Ingest Onfleet JSON files into structured Bronze Delta tables on Databricks.

Expected source layout (matching the original load_onfleet.py):

    <source-folder>/
      2025_01/page_0001.json
      2025_01/page_0002.json
      2025_02/page_0001.json
      ...
      workers/workers.json

Task page files contain a top-level ``tasks`` array. ``workers/workers.json``
is normally a plain array, with support for an object containing a ``data``
array for compatibility with the original loader.

Tables created in the configured Unity Catalog catalog/schema:
    onfleet_deliveries
    onfleet_workers
    pipeline_state  # shared across source loaders

Task payload fields with stable shapes are stored as typed columns and nested
structs/arrays. Variable-shaped values (e.g. overrides and feedback) are kept
as JSON strings so their source information is not discarded. Since a worker
record example was not supplied, the workers table schema is inferred from the
full workers roster, recursively structured where values have consistent shapes.

Incremental policy: each relative source path is processed once per
(source_system, source_file). Existing paths are skipped even if overwritten.
Rows for a not-yet-registered file are deleted and rewritten before the file is
registered in pipeline_state, making retries safe after partial writes. Run one
instance at a time to avoid races in the shared pipeline_state table.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("load_onfleet")

SOURCE_SYSTEM = "onfleet"

# ---------------------------------------------------------------------------
# Explicit, source-aligned task schema
# ---------------------------------------------------------------------------

TASK_METADATA_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        # Onfleet metadata values may be strings, numbers, booleans, or objects.
        # JSON text preserves the source type and value without coercion.
        StructField("value_json", StringType(), True),
        StructField("visibility", ArrayType(StringType(), True), True),
    ]
)

TASK_COMPLETION_SCHEMA = StructType(
    [
        StructField("failure_reason", StringType(), True),
        StructField("success_notes", StringType(), True),
        StructField("success_evidence", StringType(), True),
        StructField("photo_upload_id", StringType(), True),
        StructField("photo_upload_ids", ArrayType(StringType(), True), True),
        StructField("signature_upload_id", StringType(), True),
        StructField("time", LongType(), True),
        StructField("first_photo_upload_id", StringType(), True),
        StructField("unavailable_attachments_json", StringType(), True),
        StructField("actions_json", StringType(), True),
        StructField("distance", DoubleType(), True),
        StructField("result", StringType(), True),
    ]
)

TASK_IDENTITY_SCHEMA = StructType(
    [
        StructField("failed_scan_count", LongType(), True),
        StructField("checksum", StringType(), True),
    ]
)

TASK_APPEARANCE_SCHEMA = StructType(
    [StructField("triangle_color", StringType(), True)]
)

TASK_CONTAINER_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("worker", StringType(), True),
    ]
)

TASK_RECIPIENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("organization", StringType(), True),
        StructField("time_created", LongType(), True),
        StructField("time_last_modified", LongType(), True),
        StructField("name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("skip_sms_notifications", BooleanType(), True),
        StructField("hashed_phone", StringType(), True),
    ]
)

TASK_ADDRESS_SCHEMA = StructType(
    [
        StructField("apartment", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("street", StringType(), True),
        StructField("number", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

TASK_DESTINATION_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("time_created", LongType(), True),
        StructField("time_last_modified", LongType(), True),
        # Source order is [longitude, latitude] in the supplied sample.
        StructField("location", ArrayType(DoubleType(), True), True),
        StructField("address", TASK_ADDRESS_SCHEMA, True),
        StructField("notes", StringType(), True),
        StructField("metadata", ArrayType(TASK_METADATA_SCHEMA, True), True),
    ]
)

TASK_BARCODES_SCHEMA = StructType(
    [
        StructField("require", ArrayType(StringType(), True), True),
        StructField("capture", ArrayType(StringType(), True), True),
        StructField("capture_max_count", LongType(), True),
    ]
)

TASK_SCHEMA = StructType(
    [
        StructField("task_id", StringType(), True),
        StructField("time_created", LongType(), True),
        StructField("time_last_modified", LongType(), True),
        StructField("organization", StringType(), True),
        StructField("short_id", StringType(), True),
        StructField("tracking_url", StringType(), True),
        StructField("worker", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("executor", StringType(), True),
        StructField("creator", StringType(), True),
        StructField("dependencies", ArrayType(StringType(), True), True),
        StructField("state", LongType(), True),
        StructField("complete_after", LongType(), True),
        StructField("complete_before", LongType(), True),
        StructField("pickup_task", BooleanType(), True),
        StructField("notes", StringType(), True),
        StructField("completion_details", TASK_COMPLETION_SCHEMA, True),
        StructField("feedback_json", StringType(), True),
        StructField("metadata", ArrayType(TASK_METADATA_SCHEMA, True), True),
        StructField("overrides_json", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("service_time", LongType(), True),
        StructField("identity", TASK_IDENTITY_SCHEMA, True),
        StructField("appearance", TASK_APPEARANCE_SCHEMA, True),
        StructField("container", TASK_CONTAINER_SCHEMA, True),
        StructField("recipients", ArrayType(TASK_RECIPIENT_SCHEMA, True), True),
        StructField("destination", TASK_DESTINATION_SCHEMA, True),
        StructField("estimated_arrival_time", LongType(), True),
        StructField("estimated_completion_time", LongType(), True),
        StructField("barcodes", TASK_BARCODES_SCHEMA, True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

PIPELINE_STATE_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("record_count", IntegerType(), False),
        StructField("processed_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

# ---------------------------------------------------------------------------
# Command-line configuration and common helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("ONFLEET_SOURCE_FOLDER"),
        help="Absolute Onfleet source root accessible to Databricks, e.g. /Volumes/.../onfleet",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("UC_CATALOG", "swiftroute"),
        help="Unity Catalog catalog (default: swiftroute)",
    )
    parser.add_argument(
        "--bronze-schema",
        default=os.getenv("BRONZE_SCHEMA", "bronze"),
        help="Target Bronze schema (default: bronze)",
    )
    return parser.parse_args()


def validate_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(
            f"Invalid {label} {value!r}. Use letters, numbers, and underscores, "
            "starting with a letter or underscore."
        )
    return value


def quote_table_name(catalog: str, schema: str, table: str) -> str:
    for value, label in ((catalog, "catalog"), (schema, "Bronze schema"), (table, "table")):
        validate_identifier(value, label)
    return f"`{catalog}`.`{schema}`.`{table}`"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def source_relative_path(source_root: Path, file_path: Path) -> str:
    return file_path.relative_to(source_root).as_posix()


def read_json_file(file_path: Path) -> Any:
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now_naive() -> datetime:
    """Return a UTC timestamp as a naive datetime for Spark TimestampType."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def json_text(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def as_text(value: Any, field_name: str = "field") -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        raise ValueError(f"Expected scalar value for {field_name}, got {type(value).__name__}")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def as_int(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Expected integer for {field_name}, got boolean")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected integer for {field_name}, got {value!r}") from exc
    if not numeric.is_integer():
        raise ValueError(f"Expected whole number for {field_name}, got {value!r}")
    return int(numeric)


def as_float(value: Any, field_name: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Expected numeric value for {field_name}, got boolean")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected numeric value for {field_name}, got {value!r}") from exc


def as_bool(value: Any, field_name: str) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ValueError(f"Expected boolean for {field_name}, got {value!r}")


def object_or_none(value: Any, field_name: str) -> Dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object or null")
    return value


def array_or_none(value: Any, field_name: str) -> List[Any] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a JSON array or null")
    return value


def normalize_metadata(entries: Any, field_name: str) -> List[Dict[str, Any]] | None:
    if entries is None:
        return None
    if not isinstance(entries, list):
        raise ValueError(f"{field_name} must be a JSON array or null")
    result: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{field_name}[{idx}] must be a JSON object")
        visibility = array_or_none(entry.get("visibility"), f"{field_name}[{idx}].visibility")
        result.append(
            {
                "name": as_text(entry.get("name"), f"{field_name}.name"),
                "type": as_text(entry.get("type"), f"{field_name}.type"),
                "value_json": json_text(entry.get("value")),
                "visibility": None if visibility is None else [as_text(v, "visibility item") for v in visibility],
            }
        )
    return result


def normalize_recipient(value: Any, field_name: str) -> Dict[str, Any] | None:
    recipient = object_or_none(value, field_name)
    if recipient is None:
        return None
    return {
        "id": as_text(recipient.get("id")),
        "organization": as_text(recipient.get("organization")),
        "time_created": as_int(recipient.get("timeCreated"), f"{field_name}.timeCreated"),
        "time_last_modified": as_int(recipient.get("timeLastModified"), f"{field_name}.timeLastModified"),
        "name": as_text(recipient.get("name")),
        "phone": as_text(recipient.get("phone")),
        "notes": as_text(recipient.get("notes")),
        "skip_sms_notifications": as_bool(recipient.get("skipSMSNotifications"), f"{field_name}.skipSMSNotifications"),
        "hashed_phone": as_text(recipient.get("hashedPhone")),
    }


def normalize_destination(value: Any) -> Dict[str, Any] | None:
    destination = object_or_none(value, "destination")
    if destination is None:
        return None
    address = object_or_none(destination.get("address"), "destination.address")
    location = array_or_none(destination.get("location"), "destination.location")
    return {
        "id": as_text(destination.get("id")),
        "time_created": as_int(destination.get("timeCreated"), "destination.timeCreated"),
        "time_last_modified": as_int(destination.get("timeLastModified"), "destination.timeLastModified"),
        "location": None if location is None else [as_float(value, "destination.location item") for value in location],
        "address": None if address is None else {
            "apartment": as_text(address.get("apartment")),
            "state": as_text(address.get("state")),
            "postal_code": as_text(address.get("postalCode")),
            "country": as_text(address.get("country")),
            "city": as_text(address.get("city")),
            "street": as_text(address.get("street")),
            "number": as_text(address.get("number")),
            "name": as_text(address.get("name")),
        },
        "notes": as_text(destination.get("notes")),
        "metadata": normalize_metadata(destination.get("metadata"), "destination.metadata"),
    }


def flatten_task(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    completion = object_or_none(record.get("completionDetails"), "completionDetails")
    identity = object_or_none(record.get("identity"), "identity")
    appearance = object_or_none(record.get("appearance"), "appearance")
    container = object_or_none(record.get("container"), "container")
    barcodes = object_or_none(record.get("barcodes"), "barcodes")
    dependencies = array_or_none(record.get("dependencies"), "dependencies")
    recipients = array_or_none(record.get("recipients"), "recipients")
    if recipients is not None:
        recipients = [normalize_recipient(item, f"recipients[{i}]") for i, item in enumerate(recipients)]

    return {
        "task_id": as_text(record.get("id")),
        "time_created": as_int(record.get("timeCreated"), "timeCreated"),
        "time_last_modified": as_int(record.get("timeLastModified"), "timeLastModified"),
        "organization": as_text(record.get("organization")),
        "short_id": as_text(record.get("shortId")),
        "tracking_url": as_text(record.get("trackingURL")),
        "worker": as_text(record.get("worker")),
        "merchant": as_text(record.get("merchant")),
        "executor": as_text(record.get("executor")),
        "creator": as_text(record.get("creator")),
        "dependencies": None if dependencies is None else [as_text(item, "dependencies item") for item in dependencies],
        "state": as_int(record.get("state"), "state"),
        "complete_after": as_int(record.get("completeAfter"), "completeAfter"),
        "complete_before": as_int(record.get("completeBefore"), "completeBefore"),
        "pickup_task": as_bool(record.get("pickupTask"), "pickupTask"),
        "notes": as_text(record.get("notes")),
        "completion_details": None if completion is None else {
            "failure_reason": as_text(completion.get("failureReason")),
            "success_notes": as_text(completion.get("successNotes")),
            "success_evidence": as_text(completion.get("successEvidence")),
            "photo_upload_id": as_text(completion.get("photoUploadId")),
            "photo_upload_ids": None if completion.get("photoUploadIds") is None else [as_text(x, "photoUploadIds item") for x in array_or_none(completion.get("photoUploadIds"), "completionDetails.photoUploadIds")],
            "signature_upload_id": as_text(completion.get("signatureUploadId")),
            "time": as_int(completion.get("time"), "completionDetails.time"),
            "first_photo_upload_id": as_text(completion.get("firstPhotoUploadId")),
            "unavailable_attachments_json": json_text(completion.get("unavailableAttachments")),
            "actions_json": json_text(completion.get("actions")),
            "distance": as_float(completion.get("distance"), "completionDetails.distance"),
            "result": as_text(completion.get("result")),
        },
        "feedback_json": json_text(record.get("feedback")),
        "metadata": normalize_metadata(record.get("metadata"), "metadata"),
        "overrides_json": json_text(record.get("overrides")),
        "quantity": as_int(record.get("quantity"), "quantity"),
        "service_time": as_int(record.get("serviceTime"), "serviceTime"),
        "identity": None if identity is None else {
            "failed_scan_count": as_int(identity.get("failedScanCount"), "identity.failedScanCount"),
            "checksum": as_text(identity.get("checksum")),
        },
        "appearance": None if appearance is None else {
            "triangle_color": as_text(appearance.get("triangleColor")),
        },
        "container": None if container is None else {
            "type": as_text(container.get("type")),
            "worker": as_text(container.get("worker")),
        },
        "recipients": recipients,
        "destination": normalize_destination(record.get("destination")),
        "estimated_arrival_time": as_int(record.get("estimatedArrivalTime"), "estimatedArrivalTime"),
        "estimated_completion_time": as_int(record.get("estimatedCompletionTime"), "estimatedCompletionTime"),
        "barcodes": None if barcodes is None else {
            "require": None if barcodes.get("require") is None else [as_text(x, "barcodes.require item") for x in array_or_none(barcodes.get("require"), "barcodes.require")],
            "capture": None if barcodes.get("capture") is None else [as_text(x, "barcodes.capture item") for x in array_or_none(barcodes.get("capture"), "barcodes.capture")],
            "capture_max_count": as_int(barcodes.get("captureMaxCount"), "barcodes.captureMaxCount"),
        },
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }

# ---------------------------------------------------------------------------
# Dynamic but structured schema inference for the workers roster
# ---------------------------------------------------------------------------


def snake_case(value: str) -> str:
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", str(value))
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9_]", "_", value).lower()
    value = re.sub(r"_+", "_", value)
    value = value.rstrip("_")
    if not value:
        value = "field"
    if value[0].isdigit():
        value = "field_" + value
    return value


def normalize_json_keys(value: Any, path: str = "worker") -> Any:
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for key, child in value.items():
            normalized_key = snake_case(str(key))
            if normalized_key in result:
                raise ValueError(
                    f"Worker record has keys that normalize to the same name at {path}: {normalized_key!r}"
                )
            result[normalized_key] = normalize_json_keys(child, f"{path}.{normalized_key}")
        return result
    if isinstance(value, list):
        return [normalize_json_keys(item, f"{path}[]") for item in value]
    return value


def infer_type(values: Sequence[Any]) -> Any:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return StringType()

    if all(isinstance(value, dict) for value in non_null):
        all_keys: List[str] = []
        for value in non_null:
            for key in value.keys():
                if key not in all_keys:
                    all_keys.append(key)
        return StructType(
            [
                StructField(key, infer_type([value.get(key) for value in non_null]), True)
                for key in all_keys
            ]
        )

    if all(isinstance(value, list) for value in non_null):
        elements = [element for value in non_null for element in value]
        return ArrayType(infer_type(elements) if elements else StringType(), True)

    if all(isinstance(value, bool) for value in non_null):
        return BooleanType()

    if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null):
        return LongType()

    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in non_null
    ):
        return DoubleType()

    if all(isinstance(value, str) for value in non_null):
        return StringType()

    # Mixed source types are retained as text (JSON text for nested values).
    return StringType()


def cast_inferred_value(value: Any, data_type: Any) -> Any:
    if value is None:
        return None
    if isinstance(data_type, StructType):
        if not isinstance(value, dict):
            return None
        return {field.name: cast_inferred_value(value.get(field.name), field.dataType) for field in data_type.fields}
    if isinstance(data_type, ArrayType):
        if not isinstance(value, list):
            return None
        return [cast_inferred_value(element, data_type.elementType) for element in value]
    if isinstance(data_type, StringType):
        if isinstance(value, (dict, list)):
            return json_text(value)
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)
    if isinstance(data_type, BooleanType):
        return as_bool(value, "worker field")
    if isinstance(data_type, LongType):
        return as_int(value, "worker field")
    if isinstance(data_type, DoubleType):
        return as_float(value, "worker field")
    return value


def worker_records_and_schema(
    raw_records: List[Dict[str, Any]], source_file: str, ingested_at: datetime, run_id: str
) -> tuple[List[Dict[str, Any]], StructType]:
    normalized_records = [normalize_json_keys(record) for record in raw_records]
    source_fields: List[str] = []
    for record in normalized_records:
        for key in record:
            if key in {"source_file_path", "ingested_at", "ingestion_run_id"}:
                raise ValueError(f"Worker source field {key!r} conflicts with an ingestion audit column")
            if key not in source_fields:
                source_fields.append(key)

    inferred_fields = [
        StructField(
            key,
            infer_type([record.get(key) for record in normalized_records]),
            True,
        )
        for key in source_fields
    ]
    schema = StructType(
        inferred_fields
        + [
            StructField("source_file_path", StringType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("ingestion_run_id", StringType(), False),
        ]
    )

    rows: List[Dict[str, Any]] = []
    for record in normalized_records:
        row: Dict[str, Any] = {
            field.name: cast_inferred_value(record.get(field.name), field.dataType)
            for field in inferred_fields
        }
        row.update(
            {
                "source_file_path": source_file,
                "ingested_at": ingested_at,
                "ingestion_run_id": run_id,
            }
        )
        rows.append(row)
    return rows, schema

# ---------------------------------------------------------------------------
# Delta table management and shared file-state tracking
# ---------------------------------------------------------------------------


def ensure_target_table(spark: SparkSession, table_name: str, schema: StructType) -> None:
    """Create an empty Delta table only when absent (Spark Connect-compatible)."""
    if not spark.catalog.tableExists(table_name):
        spark.createDataFrame([], schema).write.format("delta").mode("overwrite").saveAsTable(table_name)


def ensure_pipeline_state_table(spark: SparkSession, catalog: str, bronze_schema: str) -> str:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = quote_table_name(catalog, bronze_schema, "pipeline_state")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table} (
            source_system STRING NOT NULL,
            source_file STRING NOT NULL,
            record_count INT NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            ingestion_run_id STRING NOT NULL
        ) USING DELTA
        """
    )
    return state_table


def get_processed_files(spark: SparkSession, state_table: str) -> set[str]:
    rows = (
        spark.table(state_table)
        .filter(f"source_system = {sql_string(SOURCE_SYSTEM)}")
        .select("source_file")
        .distinct()
        .collect()
    )
    return {row["source_file"] for row in rows}


def mark_file_processed(
    spark: SparkSession,
    state_table: str,
    source_file: str,
    record_count: int,
    run_id: str,
) -> None:
    state_df = spark.createDataFrame(
        [(SOURCE_SYSTEM, source_file, int(record_count), utc_now_naive(), run_id)],
        schema=PIPELINE_STATE_SCHEMA,
    )
    view_name = "_onfleet_pipeline_state_new"
    state_df.createOrReplaceTempView(view_name)
    try:
        spark.sql(
            f"""
            MERGE INTO {state_table} AS target
            USING {view_name} AS source
              ON target.source_system = source.source_system
             AND target.source_file = source.source_file
            WHEN NOT MATCHED THEN INSERT (
                source_system, source_file, record_count, processed_at, ingestion_run_id
            ) VALUES (
                source.source_system, source.source_file, source.record_count,
                source.processed_at, source.ingestion_run_id
            )
            """
        )
    finally:
        spark.catalog.dropTempView(view_name)


def replace_rows_for_file(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    rows: Sequence[Dict[str, Any]],
    source_file: str,
) -> None:
    ensure_target_table(spark, table_name, schema)
    spark.sql(f"DELETE FROM {table_name} WHERE source_file_path = {sql_string(source_file)}")
    if not rows:
        log.info("No rows to write for %s into %s", source_file, table_name)
        return
    df: DataFrame = spark.createDataFrame(list(rows), schema=schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    log.info("Wrote %d rows to %s from %s", len(rows), table_name, source_file)

# ---------------------------------------------------------------------------
# File processors
# ---------------------------------------------------------------------------


def process_task_file(
    spark: SparkSession,
    file_path: Path,
    relative_path: str,
    state_table: str,
    target_table: str,
    processed_files: set[str],
    run_id: str,
) -> bool:
    if relative_path in processed_files:
        log.info("Already processed; skipping: %s", relative_path)
        return False

    log.info("Processing new task file: %s", relative_path)
    ingested_at = utc_now_naive()
    try:
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            raise ValueError("Task page must be a top-level JSON object")
        tasks = payload.get("tasks")
        if not isinstance(tasks, list):
            raise ValueError("Task page must contain a top-level 'tasks' array")
        last_id = payload.get("lastId")
        log.info("Task page %s contains %d tasks; lastId=%s", relative_path, len(tasks), last_id)

        rows: List[Dict[str, Any]] = []
        for index, task in enumerate(tasks):
            if not isinstance(task, dict):
                raise ValueError(f"tasks[{index}] must be a JSON object")
            rows.append(flatten_task(task, relative_path, ingested_at, run_id))

        replace_rows_for_file(spark, target_table, TASK_SCHEMA, rows, relative_path)
        mark_file_processed(spark, state_table, relative_path, len(rows), run_id)
        processed_files.add(relative_path)
        log.info("Successfully processed task file %s: rows=%d", relative_path, len(rows))
        return True
    except Exception:
        log.exception("Failed to process task file %s; it remains unregistered and can be retried", relative_path)
        raise


def process_workers_file(
    spark: SparkSession,
    file_path: Path,
    relative_path: str,
    state_table: str,
    target_table: str,
    processed_files: set[str],
    run_id: str,
) -> bool:
    if relative_path in processed_files:
        log.info("Already processed; skipping: %s", relative_path)
        return False

    log.info("Processing new workers file: %s", relative_path)
    ingested_at = utc_now_naive()
    try:
        payload = read_json_file(file_path)
        if isinstance(payload, list):
            raw_records = payload
        elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
            raw_records = payload["data"]
        else:
            raise ValueError("Workers file must be a JSON array or an object containing a 'data' array")
        if not all(isinstance(record, dict) for record in raw_records):
            raise ValueError("Every worker record must be a JSON object")

        rows, schema = worker_records_and_schema(raw_records, relative_path, ingested_at, run_id)
        replace_rows_for_file(spark, target_table, schema, rows, relative_path)
        mark_file_processed(spark, state_table, relative_path, len(rows), run_id)
        processed_files.add(relative_path)
        log.info("Successfully processed workers file %s: rows=%d", relative_path, len(rows))
        return True
    except Exception:
        log.exception("Failed to process workers file %s; it remains unregistered and can be retried", relative_path)
        raise

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run() -> None:
    args = parse_args()
    if not args.source_folder:
        raise ValueError(
            "Set --source-folder or ONFLEET_SOURCE_FOLDER to the Onfleet source root "
            "accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/onfleet"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Source folder does not exist or is not a directory: {source_root}")

    workers_folder = source_root / "workers"
    workers_file = workers_folder / "workers.json"
    if not workers_folder.is_dir():
        raise FileNotFoundError(f"Expected Onfleet workers folder not found: {workers_folder}")
    if not workers_file.is_file():
        raise FileNotFoundError(f"Expected Onfleet workers file not found: {workers_file}")

    # Match the original loader: immediate child folders other than workers are
    # treated as month folders; JSON pages are read one directory level below.
    month_folders = sorted(
        path for path in source_root.iterdir()
        if path.is_dir() and path.name != "workers"
    )
    task_files: List[Path] = []
    for month_folder in month_folders:
        task_files.extend(sorted(path for path in month_folder.glob("*.json") if path.is_file()))
    files: List[Path] = task_files + [workers_file]
    if not task_files:
        log.warning("No task-page JSON files found directly inside month folders under %s", source_root)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)
    delivery_table = quote_table_name(catalog, bronze_schema, "onfleet_deliveries")
    workers_table = quote_table_name(catalog, bronze_schema, "onfleet_workers")

    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)
    log.info("Starting Onfleet ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered Onfleet files: %d", len(processed_files))
    log.info("Discovered Onfleet task files: %d", len(task_files))
    log.info("Discovered Onfleet source files (including workers): %d", len(files))

    loaded_count = 0
    skipped_count = 0
    failed_files: List[str] = []
    for file_path in files:
        relative_path = source_relative_path(source_root, file_path)
        try:
            if file_path == workers_file:
                was_loaded = process_workers_file(
                    spark, file_path, relative_path, state_table, workers_table, processed_files, run_id
                )
            else:
                was_loaded = process_task_file(
                    spark, file_path, relative_path, state_table, delivery_table, processed_files, run_id
                )
            if was_loaded:
                loaded_count += 1
            else:
                skipped_count += 1
        except Exception:
            failed_files.append(relative_path)

    log.info(
        "Onfleet ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count, skipped_count, len(failed_files), run_id,
    )
    if failed_files:
        raise RuntimeError("Onfleet ingestion failed for the following file(s): " + ", ".join(failed_files))


if __name__ == "__main__":
    run()
