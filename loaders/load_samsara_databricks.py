"""Ingest Samsara JSON exports into source-aligned Bronze Delta tables on Databricks.

Expected source layout (matching the existing load_samsara.py)::

    <source-folder>/
      vehicles/vehicles.json                 # top-level ``data`` array
      driver_summary/driver_summary.json      # top-level ``data`` array
      trips/<month>/trips_<vehicle_id>.json   # top-level ``trips`` array

The loader writes one structured row per vehicle, driver summary, or trip. Known
nested values (vehicle tags and coordinates) are structured; ``hosViolations``
and ``safetyEvents`` are preserved as JSON strings because their element
schemas are not shown in the supplied examples. Trip file context (vehicle ID,
vehicle name, and month) is retained on each trip row when present in the file.

Target tables in the configured Unity Catalog catalog/schema:
    samsara_vehicles
    samsara_driver_summary
    samsara_trips
    pipeline_state  # shared with the other source loaders

Incremental policy: each relative file path is processed once per source system.
Previously registered paths are skipped even if their contents change at the
same path. Before a new file is registered, rows for that file are deleted and
rewritten so a failed, unregistered file can be retried without duplicates.
Run only one ingestion task at a time while sharing pipeline_state.
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
from typing import Any, Dict, List, Sequence, Tuple

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
log = logging.getLogger("load_samsara")

SOURCE_SYSTEM = "samsara"

# ---------------------------------------------------------------------------
# Source-aligned schemas
# ---------------------------------------------------------------------------

VEHICLE_TAG_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

VEHICLES_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("vin", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("license_plate", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("tags", ArrayType(VEHICLE_TAG_SCHEMA, True), True),
        StructField("current_odometer_meters", LongType(), True),
        StructField("last_known_location_latitude", DoubleType(), True),
        StructField("last_known_location_longitude", DoubleType(), True),
        StructField("operational_status", StringType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

DRIVER_SUMMARY_SCHEMA = StructType(
    [
        StructField("driver_id", StringType(), True),
        StructField("driver_name", StringType(), True),
        StructField("total_distance_meters", LongType(), True),
        StructField("total_drive_time_ms", LongType(), True),
        StructField("total_idle_time_ms", LongType(), True),
        StructField("total_trips", IntegerType(), True),
        StructField("speeding_count", IntegerType(), True),
        StructField("harsh_braking_count", IntegerType(), True),
        StructField("harsh_accel_count", IntegerType(), True),
        StructField("safety_score", DoubleType(), True),
        StructField("hos_violations_json", StringType(), True),
        StructField("city", StringType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

TRIPS_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("start_ms", LongType(), True),
        StructField("end_ms", LongType(), True),
        StructField("start_address", StringType(), True),
        StructField("end_address", StringType(), True),
        StructField("start_coords_latitude", DoubleType(), True),
        StructField("start_coords_longitude", DoubleType(), True),
        StructField("end_coords_latitude", DoubleType(), True),
        StructField("end_coords_longitude", DoubleType(), True),
        StructField("distance_meters", DoubleType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("driving_duration_ms", LongType(), True),
        StructField("idling_duration_ms", LongType(), True),
        StructField("fuel_consumed_ml", LongType(), True),
        StructField("fuel_consumed_gallons", DoubleType(), True),
        StructField("max_speed_mph", DoubleType(), True),
        StructField("average_speed_mph", DoubleType(), True),
        StructField("safety_events_json", StringType(), True),
        StructField("source_vehicle_id", StringType(), True),
        StructField("source_vehicle_name", StringType(), True),
        StructField("source_month", StringType(), True),
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
# Configuration and shared helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("SAMSARA_SOURCE_FOLDER"),
        help=(
            "Absolute Samsara source path accessible to Databricks, e.g. "
            "/Volumes/swiftroute/bronze/source_files/samsara"
        ),
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("UC_CATALOG", "swiftroute"),
        help="Unity Catalog catalog containing the Bronze schema (default: swiftroute)",
    )
    parser.add_argument(
        "--bronze-schema",
        default=os.getenv("BRONZE_SCHEMA", "bronze"),
        help="Target Bronze schema (default: bronze)",
    )
    return parser.parse_args()


def validate_identifier(value: str, label: str) -> str:
    """Allow a simple Unity Catalog identifier and prevent SQL identifier injection."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(
            f"Invalid {label} {value!r}. Use letters, numbers and underscores, "
            "starting with a letter or underscore."
        )
    return value


def quote_table_name(catalog: str, schema: str, table: str) -> str:
    for value, label in ((catalog, "catalog"), (schema, "schema"), (table, "table")):
        validate_identifier(value, label)
    return f"`{catalog}`.`{schema}`.`{table}`"


def sql_string(value: str) -> str:
    """Escape a value used as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def source_relative_path(source_root: Path, file_path: Path) -> str:
    return file_path.relative_to(source_root).as_posix()


def read_json_file(file_path: Path) -> Any:
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def as_text(value: Any) -> str | None:
    """Represent IDs and source attributes as strings without dropping values."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        raise TypeError(f"Expected a scalar source value, received {type(value).__name__}")
    return str(value)


def as_float(value: Any, field_name: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Expected numeric value for {field_name}, got boolean {value!r}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected numeric value for {field_name}, got {value!r}") from exc


def as_int(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"Expected integer value for {field_name}, got boolean {value!r}")
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}")
    if isinstance(value, str) and str(number) != value.strip():
        # Accept simple numeric strings (including a leading plus sign) only if
        # they represent an integer exactly; reject decimal strings like "3.5".
        try:
            numeric = float(value)
        except ValueError:
            numeric = None
        if numeric is None or not numeric.is_integer() or int(numeric) != number:
            raise ValueError(f"Expected integer value for {field_name}, got {value!r}")
    return number


def as_json_text(value: Any, field_name: str) -> str | None:
    """Preserve a variable-schema JSON value when its element shape is unknown."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"Expected JSON array for {field_name}, got {type(value).__name__}")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def require_object(value: Any, field_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object for {field_name}, got {type(value).__name__}")
    return value


def ensure_target_table(spark: SparkSession, table_name: str, schema: StructType) -> None:
    """Create an empty Delta table only when it does not already exist.

    The errorifexists writer mode is not supported by the Spark Connect runtime
    used in the current Databricks Free Edition environment.
    """
    if not spark.catalog.tableExists(table_name):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def ensure_pipeline_state_table(spark: SparkSession, catalog: str, bronze_schema: str) -> str:
    schema_name = f"`{catalog}`.`{bronze_schema}`"
    state_table = quote_table_name(catalog, bronze_schema, "pipeline_state")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
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
    processed_at: datetime,
) -> None:
    """Register a source file only after its Bronze output has succeeded."""
    source_df = spark.createDataFrame(
        [(SOURCE_SYSTEM, source_file, int(record_count), processed_at, run_id)],
        PIPELINE_STATE_SCHEMA,
    )
    temp_view = "_samsara_pipeline_state_new"
    source_df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {state_table} AS target
            USING {temp_view} AS source
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
        spark.catalog.dropTempView(temp_view)


def replace_rows_for_file(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    records: Sequence[Dict[str, Any]],
    source_file: str,
) -> None:
    """Delete this file's previous rows, then append its current records."""
    ensure_target_table(spark, table_name, schema)
    spark.sql(f"DELETE FROM {table_name} WHERE source_file_path = {sql_string(source_file)}")
    if not records:
        log.info("No records to write for %s into %s", source_file, table_name)
        return
    df: DataFrame = spark.createDataFrame(list(records), schema=schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    log.info("Wrote %d rows to %s from %s", len(records), table_name, source_file)


# ---------------------------------------------------------------------------
# Normalize one JSON record into a typed Bronze row
# ---------------------------------------------------------------------------


def normalize_vehicle_tag(tag: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": as_text(tag.get("id")),
        "name": as_text(tag.get("name")),
    }


def flatten_vehicle(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    raw_tags = record.get("tags")
    if raw_tags is not None and not isinstance(raw_tags, list):
        raise ValueError("Vehicle field 'tags' must be a JSON array or null")
    tags = None if raw_tags is None else [normalize_vehicle_tag(require_object(tag, "tags[]")) for tag in raw_tags]
    location = require_object(record.get("lastKnownLocation"), "lastKnownLocation")
    return {
        "vehicle_id": as_text(record.get("id")),
        "name": as_text(record.get("name")),
        "vin": as_text(record.get("vin")),
        "make": as_text(record.get("make")),
        "model": as_text(record.get("model")),
        "year": as_int(record.get("year"), "year"),
        "vehicle_type": as_text(record.get("vehicleType")),
        "license_plate": as_text(record.get("licensePlate")),
        "fuel_type": as_text(record.get("fuelType")),
        "tags": tags,
        "current_odometer_meters": as_int(record.get("currentOdometerMeters"), "currentOdometerMeters"),
        "last_known_location_latitude": as_float(location.get("latitude"), "lastKnownLocation.latitude"),
        "last_known_location_longitude": as_float(location.get("longitude"), "lastKnownLocation.longitude"),
        "operational_status": as_text(record.get("operationalStatus")),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_driver_summary(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    return {
        "driver_id": as_text(record.get("driverId")),
        "driver_name": as_text(record.get("driverName")),
        "total_distance_meters": as_int(record.get("totalDistanceMeters"), "totalDistanceMeters"),
        "total_drive_time_ms": as_int(record.get("totalDriveTimeMs"), "totalDriveTimeMs"),
        "total_idle_time_ms": as_int(record.get("totalIdleTimeMs"), "totalIdleTimeMs"),
        "total_trips": as_int(record.get("totalTrips"), "totalTrips"),
        "speeding_count": as_int(record.get("speedingCount"), "speedingCount"),
        "harsh_braking_count": as_int(record.get("harshBrakingCount"), "harshBrakingCount"),
        "harsh_accel_count": as_int(record.get("harshAccelCount"), "harshAccelCount"),
        "safety_score": as_float(record.get("safetyScore"), "safetyScore"),
        "hos_violations_json": as_json_text(record.get("hosViolations"), "hosViolations"),
        "city": as_text(record.get("_city")),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_trip(
    record: Dict[str, Any],
    source_file: str,
    ingested_at: datetime,
    run_id: str,
    source_vehicle_id: str | None,
    source_vehicle_name: str | None,
    source_month: str | None,
) -> Dict[str, Any]:
    start_coords = require_object(record.get("startCoords"), "startCoords")
    end_coords = require_object(record.get("endCoords"), "endCoords")
    return {
        "trip_id": as_text(record.get("id")),
        "vehicle_id": as_text(record.get("vehicleId")),
        "driver_id": as_text(record.get("driverId")),
        "start_ms": as_int(record.get("startMs"), "startMs"),
        "end_ms": as_int(record.get("endMs"), "endMs"),
        "start_address": as_text(record.get("startAddress")),
        "end_address": as_text(record.get("endAddress")),
        "start_coords_latitude": as_float(start_coords.get("latitude"), "startCoords.latitude"),
        "start_coords_longitude": as_float(start_coords.get("longitude"), "startCoords.longitude"),
        "end_coords_latitude": as_float(end_coords.get("latitude"), "endCoords.latitude"),
        "end_coords_longitude": as_float(end_coords.get("longitude"), "endCoords.longitude"),
        "distance_meters": as_float(record.get("distanceMeters"), "distanceMeters"),
        "duration_ms": as_int(record.get("durationMs"), "durationMs"),
        "driving_duration_ms": as_int(record.get("drivingDurationMs"), "drivingDurationMs"),
        "idling_duration_ms": as_int(record.get("idlingDurationMs"), "idlingDurationMs"),
        "fuel_consumed_ml": as_int(record.get("fuelConsumedMl"), "fuelConsumedMl"),
        "fuel_consumed_gallons": as_float(record.get("fuelConsumedGallons"), "fuelConsumedGallons"),
        "max_speed_mph": as_float(record.get("maxSpeedMph"), "maxSpeedMph"),
        "average_speed_mph": as_float(record.get("averageSpeedMph"), "averageSpeedMph"),
        "safety_events_json": as_json_text(record.get("safetyEvents"), "safetyEvents"),
        "source_vehicle_id": source_vehicle_id,
        "source_vehicle_name": source_vehicle_name,
        "source_month": source_month,
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


# ---------------------------------------------------------------------------
# File discovery, shape validation, and incremental ingestion
# ---------------------------------------------------------------------------


def table_and_schema_for_file(
    relative_path: str, tables: Dict[str, str]
) -> Tuple[str, StructType, str]:
    parts = Path(relative_path).parts
    if parts == ("vehicles", "vehicles.json"):
        return tables["vehicles"], VEHICLES_SCHEMA, "vehicles"
    if parts == ("driver_summary", "driver_summary.json"):
        return tables["driver_summary"], DRIVER_SUMMARY_SCHEMA, "driver_summary"
    if len(parts) >= 3 and parts[0] == "trips":
        return tables["trips"], TRIPS_SCHEMA, "trips"
    raise ValueError(f"File is outside recognized Samsara source folders: {relative_path}")


def process_file(
    spark: SparkSession,
    source_root: Path,
    file_path: Path,
    processed_files: set[str],
    state_table: str,
    tables: Dict[str, str],
    run_id: str,
) -> bool:
    relative_path = source_relative_path(source_root, file_path)
    if relative_path in processed_files:
        log.info("Already processed; skipping: %s", relative_path)
        return False

    table_name, schema, entity = table_and_schema_for_file(relative_path, tables)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("Processing new file: %s", relative_path)

    try:
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            raise ValueError("Samsara JSON file must contain a top-level JSON object")

        if entity in {"vehicles", "driver_summary"}:
            records = payload.get("data")
            if not isinstance(records, list):
                raise ValueError("Samsara vehicle and driver summary files must contain a top-level 'data' array")
        else:
            records = payload.get("trips")
            if not isinstance(records, list):
                raise ValueError("Samsara trip files must contain a top-level 'trips' array")

        source_vehicle_id = as_text(payload.get("vehicleId")) if entity == "trips" else None
        source_vehicle_name = as_text(payload.get("vehicleName")) if entity == "trips" else None
        source_month = as_text(payload.get("month")) if entity == "trips" else None

        normalized_rows: List[Dict[str, Any]] = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"{entity}[{index}] must be a JSON object")
            if entity == "vehicles":
                normalized = flatten_vehicle(record, relative_path, ingested_at, run_id)
            elif entity == "driver_summary":
                normalized = flatten_driver_summary(record, relative_path, ingested_at, run_id)
            else:
                normalized = flatten_trip(
                    record,
                    relative_path,
                    ingested_at,
                    run_id,
                    source_vehicle_id,
                    source_vehicle_name,
                    source_month,
                )
            normalized_rows.append(normalized)

        replace_rows_for_file(
            spark=spark,
            table_name=table_name,
            schema=schema,
            records=normalized_rows,
            source_file=relative_path,
        )
        mark_file_processed(
            spark=spark,
            state_table=state_table,
            source_file=relative_path,
            record_count=len(normalized_rows),
            run_id=run_id,
            processed_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        processed_files.add(relative_path)
        log.info(
            "Successfully processed %s: rows=%d target=%s",
            relative_path,
            len(normalized_rows),
            table_name,
        )
        return True
    except Exception:
        log.exception("Failed to process %s; it remains unregistered and can be retried", relative_path)
        raise


def run() -> None:
    args = parse_args()
    if not args.source_folder:
        raise ValueError(
            "Set --source-folder or SAMSARA_SOURCE_FOLDER to the Samsara folder "
            "accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/samsara"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Source folder does not exist or is not a directory: {source_root}")

    vehicles_file = source_root / "vehicles" / "vehicles.json"
    driver_summary_file = source_root / "driver_summary" / "driver_summary.json"
    trips_folder = source_root / "trips"
    if not vehicles_file.is_file():
        raise FileNotFoundError(f"Expected Samsara vehicles file not found: {vehicles_file}")
    if not driver_summary_file.is_file():
        raise FileNotFoundError(f"Expected Samsara driver summary file not found: {driver_summary_file}")
    if not trips_folder.is_dir():
        raise FileNotFoundError(f"Expected Samsara trips folder not found: {trips_folder}")

    # Process static snapshots first, followed by trip files in stable path order.
    files = [vehicles_file, driver_summary_file] + sorted(
        path for path in trips_folder.rglob("*.json") if path.is_file()
    )

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)

    tables = {
        "vehicles": quote_table_name(catalog, bronze_schema, "samsara_vehicles"),
        "driver_summary": quote_table_name(catalog, bronze_schema, "samsara_driver_summary"),
        "trips": quote_table_name(catalog, bronze_schema, "samsara_trips"),
    }
    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)
    log.info("Starting Samsara ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered Samsara files: %d", len(processed_files))
    log.info("Discovered Samsara source files: %d", len(files))

    loaded_count = 0
    skipped_count = 0
    failed_files: List[str] = []
    for file_path in files:
        try:
            was_loaded = process_file(
                spark=spark,
                source_root=source_root,
                file_path=file_path,
                processed_files=processed_files,
                state_table=state_table,
                tables=tables,
                run_id=run_id,
            )
            if was_loaded:
                loaded_count += 1
            else:
                skipped_count += 1
        except Exception:
            failed_files.append(source_relative_path(source_root, file_path))

    log.info(
        "Samsara ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count,
        skipped_count,
        len(failed_files),
        run_id,
    )
    if failed_files:
        raise RuntimeError(
            "Samsara ingestion failed for the following file(s): " + ", ".join(failed_files)
        )


if __name__ == "__main__":
    run()
