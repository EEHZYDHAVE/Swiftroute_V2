"""Ingest Linnworks JSON files into source-aligned Bronze Delta tables on Databricks.

Expected source layout::

    <source-folder>/
      processed_orders/<month-folder>/*.json
      stock_items/stock_items.json
      stock_transactions/<month-folder>/*.json

Each JSON file is expected to contain a top-level ``Data`` array. The page-level
pagination metadata is intentionally not repeated on every business row; the
source file path, actual row count, ingestion timestamp, and run ID are recorded
for auditability.

Tables created in the configured Unity Catalog catalog/schema:
    linnworks_orders
    linnworks_inventory
    linnworks_stock_transactions
    pipeline_state  # shared with the other source loaders

Incremental policy:
    Each source file is processed once per (source_system, relative_file_path).
    Files already registered in pipeline_state are skipped, even if their
    contents later change at the same path. New file paths are ingested.

Important:
    - Place source files in persistent storage accessible to Databricks, e.g. a
      Unity Catalog Volume at /Volumes/<catalog>/<schema>/<volume>/linnworks.
    - Configure a single concurrent run for this loader to avoid races in the
      shared pipeline_state table.
    - Before a new file is registered as processed, its target rows are deleted
      and rewritten. If a write fails, the file remains unregistered and a retry
      can safely replace any partial rows.
    - Order Items are preserved as a typed array of structs. Address,
      GeneralInfo, and ShippingInfo are flattened to prefixed columns. Stock
      transactions remain at transaction grain; running balances belong in a
      downstream transformation that replays the transactions in order.
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
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("load_linnworks")

SOURCE_SYSTEM = "linnworks"


# ---------------------------------------------------------------------------
# Source-aligned Bronze schemas
# ---------------------------------------------------------------------------

ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("stock_item_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("item_title", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("price_per_unit", DoubleType(), True),
        StructField("line_total", DoubleType(), True),
        StructField("weight", DoubleType(), True),
        StructField("is_composite", BooleanType(), True),
        StructField("bin_rack", StringType(), True),
    ]
)

ORDER_SCHEMA = StructType(
    [
        StructField("pk_order_id", StringType(), True),
        StructField("num_order_id", StringType(), True),
        StructField("reference_num", StringType(), True),
        StructField("external_reference", StringType(), True),
        StructField("secondary_reference", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("source", StringType(), True),
        StructField("sub_source", StringType(), True),
        StructField("site_code", StringType(), True),
        StructField("fulfilment_location_name", StringType(), True),
        StructField("received_date", StringType(), True),
        StructField("processed_on", StringType(), True),
        StructField("dispatched_on", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("address_full_name", StringType(), True),
        StructField("address_company", StringType(), True),
        StructField("address_address1", StringType(), True),
        StructField("address_address2", StringType(), True),
        StructField("address_city", StringType(), True),
        StructField("address_region", StringType(), True),
        StructField("address_post_code", StringType(), True),
        StructField("address_country", StringType(), True),
        StructField("address_country_code", StringType(), True),
        StructField("address_phone", StringType(), True),
        StructField("sub_total", DoubleType(), True),
        StructField("postal_service_cost", DoubleType(), True),
        StructField("total_charge", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("general_status", StringType(), True),
        StructField("general_lock_for_shipping", BooleanType(), True),
        StructField("general_marker", StringType(), True),
        StructField("general_notes", StringType(), True),
        StructField("shipping_postal_service_name", StringType(), True),
        StructField("shipping_tracking_number", StringType(), True),
        StructField("shipping_vendor", StringType(), True),
        StructField("shipping_postal_service_code", StringType(), True),
        StructField("items", ArrayType(ORDER_ITEM_SCHEMA, True), True),
        StructField("_swiftroute_client_id", StringType(), True),
        StructField("_swiftroute_client_name", StringType(), True),
        StructField("_swiftroute_pick_duration_mins", DoubleType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

INVENTORY_SCHEMA = StructType(
    [
        StructField("stock_item_id", StringType(), True),
        StructField("item_number", StringType(), True),
        StructField("item_title", StringType(), True),
        StructField("barcode_number", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("package_group_name", StringType(), True),
        StructField("tax_cost_inclusive", BooleanType(), True),
        StructField("postal_service_name", StringType(), True),
        StructField("cost_price", DoubleType(), True),
        StructField("retail_price", DoubleType(), True),
        StructField("weight", DoubleType(), True),
        StructField("width", DoubleType(), True),
        StructField("height", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("is_composite_parent", BooleanType(), True),
        StructField("is_variation_parent", BooleanType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("minimum_level", DoubleType(), True),
        StructField("in_order_book", DoubleType(), True),
        StructField("due", DoubleType(), True),
        StructField("jit", BooleanType(), True),
        StructField("creation_date", StringType(), True),
        StructField("modified_date", StringType(), True),
        StructField("source", StringType(), True),
        StructField("is_deleted", BooleanType(), True),
        StructField("_swiftroute_client_id", StringType(), True),
        StructField("_swiftroute_client_name", StringType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

STOCK_TRANSACTION_SCHEMA = StructType(
    [
        StructField("pk_stock_transaction_id", StringType(), True),
        StructField("fk_stock_item_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("item_title", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("transaction_note", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("fk_order_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("bin_rack", StringType(), True),
        StructField("date", StringType(), True),
        StructField("stock_value", DoubleType(), True),
        StructField("_note", StringType(), True),
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
# Configuration and general helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("LINNWORKS_SOURCE_FOLDER"),
        help="Absolute Linnworks source path accessible to Databricks, e.g. /Volumes/.../linnworks",
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
    raise ValueError(f"Expected boolean value for {field_name}, got {value!r}")


def ensure_target_table(
    spark: SparkSession, table_name: str, schema: StructType
) -> None:
    """Create an empty Delta table if it does not yet exist.

    The ``errorifexists`` writer mode is not supported in the user's Databricks
    Free Edition Spark Connect runtime, so table existence is checked first.
    """
    if not spark.catalog.tableExists(table_name):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def ensure_pipeline_state_table(
    spark: SparkSession, catalog: str, bronze_schema: str
) -> str:
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
    source_df.createOrReplaceTempView("_linnworks_pipeline_state_new")
    try:
        spark.sql(
            f"""
            MERGE INTO {state_table} AS target
            USING _linnworks_pipeline_state_new AS source
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
        spark.catalog.dropTempView("_linnworks_pipeline_state_new")


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
# Transform one JSON record into a typed Bronze row
# ---------------------------------------------------------------------------


def normalize_order_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "stock_item_id": as_text(item.get("StockItemId")),
        "sku": as_text(item.get("SKU")),
        "item_title": as_text(item.get("ItemTitle")),
        "quantity": as_float(item.get("Quantity"), "Items.Quantity"),
        "unit_cost": as_float(item.get("UnitCost"), "Items.UnitCost"),
        "price_per_unit": as_float(item.get("PricePerUnit"), "Items.PricePerUnit"),
        "line_total": as_float(item.get("LineTotal"), "Items.LineTotal"),
        "weight": as_float(item.get("Weight"), "Items.Weight"),
        "is_composite": as_bool(item.get("IsComposite"), "Items.IsComposite"),
        "bin_rack": as_text(item.get("BinRack")),
    }


def flatten_order(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    address = record.get("Address") or {}
    general_info = record.get("GeneralInfo") or {}
    shipping_info = record.get("ShippingInfo") or {}
    raw_items = record.get("Items")
    if raw_items is not None and not isinstance(raw_items, list):
        raise ValueError("Order field 'Items' must be a JSON array or null")
    if not isinstance(address, dict) or not isinstance(general_info, dict) or not isinstance(shipping_info, dict):
        raise ValueError("Order Address, GeneralInfo, and ShippingInfo must be JSON objects or null")

    items = None if raw_items is None else [normalize_order_item(item) for item in raw_items]
    return {
        "pk_order_id": as_text(record.get("pkOrderID")),
        "num_order_id": as_text(record.get("NumOrderId")),
        "reference_num": as_text(record.get("ReferenceNum")),
        "external_reference": as_text(record.get("ExternalReference")),
        "secondary_reference": as_text(record.get("SecondaryReference")),
        "channel": as_text(record.get("Channel")),
        "source": as_text(record.get("Source")),
        "sub_source": as_text(record.get("SubSource")),
        "site_code": as_text(record.get("SiteCode")),
        "fulfilment_location_name": as_text(record.get("FulfilmentLocationName")),
        "received_date": as_text(record.get("ReceivedDate")),
        "processed_on": as_text(record.get("ProcessedOn")),
        "dispatched_on": as_text(record.get("DispatchedOn")),
        "customer_name": as_text(record.get("CustomerName")),
        "customer_email": as_text(record.get("CustomerEmail")),
        "address_full_name": as_text(address.get("FullName")),
        "address_company": as_text(address.get("Company")),
        "address_address1": as_text(address.get("Address1")),
        "address_address2": as_text(address.get("Address2")),
        "address_city": as_text(address.get("City")),
        "address_region": as_text(address.get("Region")),
        "address_post_code": as_text(address.get("PostCode")),
        "address_country": as_text(address.get("Country")),
        "address_country_code": as_text(address.get("CountryCode")),
        "address_phone": as_text(address.get("Phone")),
        "sub_total": as_float(record.get("SubTotal"), "SubTotal"),
        "postal_service_cost": as_float(record.get("PostalServiceCost"), "PostalServiceCost"),
        "total_charge": as_float(record.get("TotalCharge"), "TotalCharge"),
        "tax_amount": as_float(record.get("TaxAmount"), "TaxAmount"),
        "currency": as_text(record.get("Currency")),
        "payment_method": as_text(record.get("PaymentMethod")),
        "payment_status": as_text(record.get("PaymentStatus")),
        "general_status": as_text(general_info.get("Status")),
        "general_lock_for_shipping": as_bool(general_info.get("LockForShipping"), "GeneralInfo.LockForShipping"),
        "general_marker": as_text(general_info.get("Marker")),
        "general_notes": as_text(general_info.get("Notes")),
        "shipping_postal_service_name": as_text(shipping_info.get("PostalServiceName")),
        "shipping_tracking_number": as_text(shipping_info.get("TrackingNumber")),
        "shipping_vendor": as_text(shipping_info.get("Vendor")),
        "shipping_postal_service_code": as_text(shipping_info.get("PostalServiceCode")),
        "items": items,
        "_swiftroute_client_id": as_text(record.get("_swiftroute_client_id")),
        "_swiftroute_client_name": as_text(record.get("_swiftroute_client_name")),
        "_swiftroute_pick_duration_mins": as_float(record.get("_swiftroute_pick_duration_mins"), "_swiftroute_pick_duration_mins"),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_inventory_item(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    return {
        "stock_item_id": as_text(record.get("StockItemId")),
        "item_number": as_text(record.get("ItemNumber")),
        "item_title": as_text(record.get("ItemTitle")),
        "barcode_number": as_text(record.get("BarcodeNumber")),
        "category_name": as_text(record.get("CategoryName")),
        "package_group_name": as_text(record.get("PackageGroupName")),
        "tax_cost_inclusive": as_bool(record.get("TaxCostInclusive"), "TaxCostInclusive"),
        "postal_service_name": as_text(record.get("PostalServiceName")),
        "cost_price": as_float(record.get("CostPrice"), "CostPrice"),
        "retail_price": as_float(record.get("RetailPrice"), "RetailPrice"),
        "weight": as_float(record.get("Weight"), "Weight"),
        "width": as_float(record.get("Width"), "Width"),
        "height": as_float(record.get("Height"), "Height"),
        "depth": as_float(record.get("Depth"), "Depth"),
        "is_composite_parent": as_bool(record.get("IsCompositeParent"), "IsCompositeParent"),
        "is_variation_parent": as_bool(record.get("IsVariationParent"), "IsVariationParent"),
        "quantity": as_float(record.get("Quantity"), "Quantity"),
        "minimum_level": as_float(record.get("MinimumLevel"), "MinimumLevel"),
        "in_order_book": as_float(record.get("InOrderBook"), "InOrderBook"),
        "due": as_float(record.get("Due"), "Due"),
        "jit": as_bool(record.get("JIT"), "JIT"),
        "creation_date": as_text(record.get("CreationDate")),
        "modified_date": as_text(record.get("ModifiedDate")),
        "source": as_text(record.get("Source")),
        "is_deleted": as_bool(record.get("IsDeleted"), "IsDeleted"),
        "_swiftroute_client_id": as_text(record.get("_swiftroute_client_id")),
        "_swiftroute_client_name": as_text(record.get("_swiftroute_client_name")),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_stock_transaction(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    return {
        "pk_stock_transaction_id": as_text(record.get("pkStockTransactionId")),
        "fk_stock_item_id": as_text(record.get("fkStockItemId")),
        "sku": as_text(record.get("SKU")),
        "item_title": as_text(record.get("ItemTitle")),
        "transaction_type": as_text(record.get("TransactionType")),
        "transaction_note": as_text(record.get("TransactionNote")),
        "quantity": as_float(record.get("Quantity"), "Quantity"),
        "fk_order_id": as_text(record.get("fkOrderId")),
        "location": as_text(record.get("Location")),
        "bin_rack": as_text(record.get("BinRack")),
        "date": as_text(record.get("Date")),
        "stock_value": as_float(record.get("StockValue"), "StockValue"),
        "_note": as_text(record.get("_note")),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


# ---------------------------------------------------------------------------
# Incremental ingestion
# ---------------------------------------------------------------------------


def table_and_schema_for_file(
    relative_path: str, tables: Dict[str, str]
) -> tuple[str, StructType, Any]:
    """Return target table, schema, and record flattener based on the file path."""
    parts = Path(relative_path).parts
    if parts and parts[0] == "processed_orders":
        return tables["orders"], ORDER_SCHEMA, flatten_order
    if parts == ("stock_items", "stock_items.json"):
        return tables["inventory"], INVENTORY_SCHEMA, flatten_inventory_item
    if parts and parts[0] == "stock_transactions":
        return tables["stock_transactions"], STOCK_TRANSACTION_SCHEMA, flatten_stock_transaction
    raise ValueError(f"File is outside the recognized Linnworks source folders: {relative_path}")


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

    table_name, schema, flattener = table_and_schema_for_file(relative_path, tables)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("Processing new file: %s", relative_path)

    try:
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            raise ValueError("Linnworks JSON file must contain a top-level JSON object")
        records = payload.get("Data")
        if not isinstance(records, list):
            raise ValueError("Linnworks JSON file must contain a top-level 'Data' array")

        normalized_rows: List[Dict[str, Any]] = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"Data[{index}] must be a JSON object")
            normalized_rows.append(flattener(record, relative_path, ingested_at, run_id))

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
        log.exception(
            "Failed to process %s; it remains unregistered and can be retried",
            relative_path,
        )
        raise


def run() -> None:
    args = parse_args()
    if not args.source_folder:
        raise ValueError(
            "Set --source-folder or LINNWORKS_SOURCE_FOLDER to the Linnworks folder "
            "accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/linnworks"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Source folder does not exist or is not a directory: {source_root}")

    orders_folder = source_root / "processed_orders"
    inventory_file = source_root / "stock_items" / "stock_items.json"
    transactions_folder = source_root / "stock_transactions"
    for expected_folder in (orders_folder, transactions_folder):
        if not expected_folder.is_dir():
            raise FileNotFoundError(f"Expected Linnworks folder not found: {expected_folder}")
    if not inventory_file.is_file():
        raise FileNotFoundError(f"Expected Linnworks stock item file not found: {inventory_file}")

    # Discover records in a stable order so reruns and job logs are reproducible.
    files = (
        sorted(path for path in orders_folder.rglob("*.json") if path.is_file())
        + [inventory_file]
        + sorted(path for path in transactions_folder.rglob("*.json") if path.is_file())
    )

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)

    tables = {
        "orders": quote_table_name(catalog, bronze_schema, "linnworks_orders"),
        "inventory": quote_table_name(catalog, bronze_schema, "linnworks_inventory"),
        "stock_transactions": quote_table_name(
            catalog, bronze_schema, "linnworks_stock_transactions"
        ),
    }
    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)
    log.info("Starting Linnworks ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered Linnworks files: %d", len(processed_files))
    log.info("Discovered Linnworks source files: %d", len(files))

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
        "Linnworks ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count,
        skipped_count,
        len(failed_files),
        run_id,
    )
    if failed_files:
        raise RuntimeError(
            "Linnworks ingestion failed for the following file(s): " + ", ".join(failed_files)
        )


if __name__ == "__main__":
    run()
