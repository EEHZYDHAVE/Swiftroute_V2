"""Ingest QuickBooks JSON exports into source-aligned Bronze Delta tables.

Expected source layout (matching the existing loader's discovery logic)::

    <source-folder>/
      invoices/*.json   # QueryResponse.Invoice array
      payments/*.json   # QueryResponse.Payment array
      expenses/*.json   # QueryResponse.Purchase array

Target tables in Unity Catalog:
    quickbooks_invoices
    quickbooks_payments
    quickbooks_expenses
    pipeline_state (shared with the other source loaders)

Each entity is converted into structured columns; repeated Line, LinkedTxn,
CustomField and reference/detail objects are represented as typed structs and
arrays where the supplied payload examples establish their shape. Source date
and timestamp values remain strings in Bronze for later standardization in
Silver Technical.

Incremental policy: each relative file path is processed once per source
system. Existing registered paths are skipped even if overwritten, by design
for this project. Rows associated with a not-yet-registered file are replaced
before the file is marked successful so a failed write can be retried safely.
Run only one instance concurrently when sharing pipeline_state.
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
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("load_quickbooks")

SOURCE_SYSTEM = "quickbooks"

# ---------------------------------------------------------------------------
# Source-aligned nested schemas
# ---------------------------------------------------------------------------

REFERENCE_VALUE_NAME_SCHEMA = StructType(
    [
        StructField("value", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

LINKED_TXN_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), True),
        StructField("txn_type", StringType(), True),
    ]
)

CUSTOM_FIELD_SCHEMA = StructType(
    [
        StructField("definition_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("string_value", StringType(), True),
    ]
)

INVOICE_SALES_ITEM_DETAIL_SCHEMA = StructType(
    [
        StructField("item_ref", REFERENCE_VALUE_NAME_SCHEMA, True),
        StructField("unit_price", DoubleType(), True),
        StructField("qty", DoubleType(), True),
        StructField("service_date", StringType(), True),
    ]
)

INVOICE_LINE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("line_num", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("detail_type", StringType(), True),
        StructField("sales_item_line_detail", INVOICE_SALES_ITEM_DETAIL_SCHEMA, True),
    ]
)

EXPENSE_ACCOUNT_BASED_DETAIL_SCHEMA = StructType(
    [
        StructField("account_ref", REFERENCE_VALUE_NAME_SCHEMA, True),
        StructField("billable_status", StringType(), True),
        # QuickBooks examples contain null ClassRef; keep it as a typed ref.
        StructField("class_ref", REFERENCE_VALUE_NAME_SCHEMA, True),
    ]
)

EXPENSE_LINE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("detail_type", StringType(), True),
        StructField(
            "account_based_expense_line_detail",
            EXPENSE_ACCOUNT_BASED_DETAIL_SCHEMA,
            True,
        ),
        StructField("description", StringType(), True),
    ]
)

PAYMENT_LINE_LINKED_TXN_SCHEMA = LINKED_TXN_SCHEMA
PAYMENT_LINE_SCHEMA = StructType(
    [
        StructField("amount", DoubleType(), True),
        StructField("linked_txn", ArrayType(PAYMENT_LINE_LINKED_TXN_SCHEMA, True), True),
    ]
)

AUDIT_FIELDS = [
    StructField("source_file_path", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("ingestion_run_id", StringType(), False),
]

INVOICE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sync_token", StringType(), True),
        StructField("metadata_create_time", StringType(), True),
        StructField("metadata_last_updated_time", StringType(), True),
        StructField("doc_number", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("customer_ref_value", StringType(), True),
        StructField("customer_ref_name", StringType(), True),
        StructField("bill_email_address", StringType(), True),
        StructField("email_status", StringType(), True),
        StructField("line", ArrayType(INVOICE_LINE_SCHEMA, True), True),
        StructField("total_amt", DoubleType(), True),
        StructField("balance", DoubleType(), True),
        StructField("currency_ref_value", StringType(), True),
        StructField("currency_ref_name", StringType(), True),
        StructField("payment_method_ref_value", StringType(), True),
        StructField("payment_method_ref_name", StringType(), True),
        StructField("linked_txn", ArrayType(LINKED_TXN_SCHEMA, True), True),
        StructField("custom_field", ArrayType(CUSTOM_FIELD_SCHEMA, True), True),
        *AUDIT_FIELDS,
    ]
)

PAYMENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sync_token", StringType(), True),
        StructField("metadata_create_time", StringType(), True),
        StructField("metadata_last_updated_time", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("customer_ref_value", StringType(), True),
        StructField("customer_ref_name", StringType(), True),
        StructField("total_amt", DoubleType(), True),
        StructField("unapplied_amt", DoubleType(), True),
        StructField("process_payment", BooleanType(), True),
        StructField("payment_method_ref_value", StringType(), True),
        StructField("payment_method_ref_name", StringType(), True),
        StructField("deposit_to_account_ref_value", StringType(), True),
        StructField("deposit_to_account_ref_name", StringType(), True),
        StructField("line", ArrayType(PAYMENT_LINE_SCHEMA, True), True),
        StructField("custom_field", ArrayType(CUSTOM_FIELD_SCHEMA, True), True),
        *AUDIT_FIELDS,
    ]
)

EXPENSE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sync_token", StringType(), True),
        StructField("metadata_create_time", StringType(), True),
        StructField("metadata_last_updated_time", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("account_ref_value", StringType(), True),
        StructField("account_ref_name", StringType(), True),
        StructField("entity_ref_value", StringType(), True),
        StructField("entity_ref_name", StringType(), True),
        StructField("entity_ref_type", StringType(), True),
        StructField("line", ArrayType(EXPENSE_LINE_SCHEMA, True), True),
        StructField("total_amt", DoubleType(), True),
        StructField("private_note", StringType(), True),
        StructField("currency_ref_value", StringType(), True),
        StructField("currency_ref_name", StringType(), True),
        *AUDIT_FIELDS,
    ]
)

SCHEMAS_BY_FOLDER: Dict[str, Tuple[str, str, StructType]] = {
    "invoices": ("Invoice", "quickbooks_invoices", INVOICE_SCHEMA),
    "payments": ("Payment", "quickbooks_payments", PAYMENT_SCHEMA),
    "expenses": ("Purchase", "quickbooks_expenses", EXPENSE_SCHEMA),
}

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
# Configuration and reusable helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("QUICKBOOKS_SOURCE_FOLDER"),
        help="Absolute QuickBooks source path accessible to Databricks, e.g. /Volumes/swiftroute/bronze/source_files/quickbooks",
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
    """Allow simple Unity Catalog identifiers and prevent SQL injection."""
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
    """Represent scalar source attributes and identifiers without losing IDs."""
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
        int_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}")
    return int_value


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


def normalize_reference(value: Any, field_name: str) -> Dict[str, Any] | None:
    ref = object_or_none(value, field_name)
    if ref is None:
        return None
    return {"value": as_text(ref.get("value")), "name": as_text(ref.get("name"))}


def normalize_linked_txn(value: Any, field_name: str) -> List[Dict[str, Any]] | None:
    entries = array_or_none(value, field_name)
    if entries is None:
        return None
    normalized = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{field_name}[{index}] must be a JSON object")
        normalized.append(
            {"txn_id": as_text(entry.get("TxnId")), "txn_type": as_text(entry.get("TxnType"))}
        )
    return normalized


def normalize_custom_fields(value: Any, field_name: str) -> List[Dict[str, Any]] | None:
    entries = array_or_none(value, field_name)
    if entries is None:
        return None
    normalized = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{field_name}[{index}] must be a JSON object")
        normalized.append(
            {
                "definition_id": as_text(entry.get("DefinitionId")),
                "name": as_text(entry.get("Name")),
                "type": as_text(entry.get("Type")),
                "string_value": as_text(entry.get("StringValue")),
            }
        )
    return normalized


def normalize_metadata(record: Dict[str, Any]) -> Tuple[str | None, str | None]:
    metadata = object_or_none(record.get("MetaData"), "MetaData") or {}
    return as_text(metadata.get("CreateTime")), as_text(metadata.get("LastUpdatedTime"))


# ---------------------------------------------------------------------------
# Entity normalizers
# ---------------------------------------------------------------------------


def normalize_invoice_line(line: Dict[str, Any], index: int) -> Dict[str, Any]:
    detail = object_or_none(line.get("SalesItemLineDetail"), f"Line[{index}].SalesItemLineDetail")
    normalized_detail = None
    if detail is not None:
        normalized_detail = {
            "item_ref": normalize_reference(detail.get("ItemRef"), f"Line[{index}].SalesItemLineDetail.ItemRef"),
            "unit_price": as_float(detail.get("UnitPrice"), f"Line[{index}].SalesItemLineDetail.UnitPrice"),
            "qty": as_float(detail.get("Qty"), f"Line[{index}].SalesItemLineDetail.Qty"),
            "service_date": as_text(detail.get("ServiceDate")),
        }
    return {
        "id": as_text(line.get("Id")),
        "line_num": as_int(line.get("LineNum"), f"Line[{index}].LineNum"),
        "description": as_text(line.get("Description")),
        "amount": as_float(line.get("Amount"), f"Line[{index}].Amount"),
        "detail_type": as_text(line.get("DetailType")),
        "sales_item_line_detail": normalized_detail,
    }


def normalize_expense_line(line: Dict[str, Any], index: int) -> Dict[str, Any]:
    detail = object_or_none(
        line.get("AccountBasedExpenseLineDetail"),
        f"Line[{index}].AccountBasedExpenseLineDetail",
    )
    normalized_detail = None
    if detail is not None:
        class_ref = detail.get("ClassRef")
        normalized_detail = {
            "account_ref": normalize_reference(
                detail.get("AccountRef"), f"Line[{index}].AccountBasedExpenseLineDetail.AccountRef"
            ),
            "billable_status": as_text(detail.get("BillableStatus")),
            "class_ref": normalize_reference(
                class_ref, f"Line[{index}].AccountBasedExpenseLineDetail.ClassRef"
            ),
        }
    return {
        "id": as_text(line.get("Id")),
        "amount": as_float(line.get("Amount"), f"Line[{index}].Amount"),
        "detail_type": as_text(line.get("DetailType")),
        "account_based_expense_line_detail": normalized_detail,
        "description": as_text(line.get("Description")),
    }


def normalize_payment_line(line: Dict[str, Any], index: int) -> Dict[str, Any]:
    return {
        "amount": as_float(line.get("Amount"), f"Line[{index}].Amount"),
        "linked_txn": normalize_linked_txn(line.get("LinkedTxn"), f"Line[{index}].LinkedTxn"),
    }


def audit_fields(source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    return {
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_invoice(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    meta_create, meta_updated = normalize_metadata(record)
    customer = normalize_reference(record.get("CustomerRef"), "CustomerRef")
    bill_email = object_or_none(record.get("BillEmail"), "BillEmail")
    currency = normalize_reference(record.get("CurrencyRef"), "CurrencyRef")
    payment_method = normalize_reference(record.get("PaymentMethodRef"), "PaymentMethodRef")
    line_values = array_or_none(record.get("Line"), "Line")
    lines = None if line_values is None else [
        normalize_invoice_line(line, index)
        for index, line in enumerate(line_values)
        if isinstance(line, dict)
    ]
    if line_values is not None and len(lines) != len(line_values):
        raise ValueError("Every Invoice Line entry must be a JSON object")
    return {
        "id": as_text(record.get("Id")),
        "sync_token": as_text(record.get("SyncToken")),
        "metadata_create_time": meta_create,
        "metadata_last_updated_time": meta_updated,
        "doc_number": as_text(record.get("DocNumber")),
        "txn_date": as_text(record.get("TxnDate")),
        "due_date": as_text(record.get("DueDate")),
        "customer_ref_value": customer.get("value") if customer else None,
        "customer_ref_name": customer.get("name") if customer else None,
        "bill_email_address": as_text(bill_email.get("Address")) if bill_email else None,
        "email_status": as_text(record.get("EmailStatus")),
        "line": lines,
        "total_amt": as_float(record.get("TotalAmt"), "TotalAmt"),
        "balance": as_float(record.get("Balance"), "Balance"),
        "currency_ref_value": currency.get("value") if currency else None,
        "currency_ref_name": currency.get("name") if currency else None,
        "payment_method_ref_value": payment_method.get("value") if payment_method else None,
        "payment_method_ref_name": payment_method.get("name") if payment_method else None,
        "linked_txn": normalize_linked_txn(record.get("LinkedTxn"), "LinkedTxn"),
        "custom_field": normalize_custom_fields(record.get("CustomField"), "CustomField"),
        **audit_fields(source_file, ingested_at, run_id),
    }


def flatten_payment(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    meta_create, meta_updated = normalize_metadata(record)
    customer = normalize_reference(record.get("CustomerRef"), "CustomerRef")
    payment_method = normalize_reference(record.get("PaymentMethodRef"), "PaymentMethodRef")
    deposit_account = normalize_reference(record.get("DepositToAccountRef"), "DepositToAccountRef")
    line_values = array_or_none(record.get("Line"), "Line")
    lines = None if line_values is None else [
        normalize_payment_line(line, index)
        for index, line in enumerate(line_values)
        if isinstance(line, dict)
    ]
    if line_values is not None and len(lines) != len(line_values):
        raise ValueError("Every Payment Line entry must be a JSON object")
    return {
        "id": as_text(record.get("Id")),
        "sync_token": as_text(record.get("SyncToken")),
        "metadata_create_time": meta_create,
        "metadata_last_updated_time": meta_updated,
        "txn_date": as_text(record.get("TxnDate")),
        "customer_ref_value": customer.get("value") if customer else None,
        "customer_ref_name": customer.get("name") if customer else None,
        "total_amt": as_float(record.get("TotalAmt"), "TotalAmt"),
        "unapplied_amt": as_float(record.get("UnappliedAmt"), "UnappliedAmt"),
        "process_payment": as_bool(record.get("ProcessPayment"), "ProcessPayment"),
        "payment_method_ref_value": payment_method.get("value") if payment_method else None,
        "payment_method_ref_name": payment_method.get("name") if payment_method else None,
        "deposit_to_account_ref_value": deposit_account.get("value") if deposit_account else None,
        "deposit_to_account_ref_name": deposit_account.get("name") if deposit_account else None,
        "line": lines,
        "custom_field": normalize_custom_fields(record.get("CustomField"), "CustomField"),
        **audit_fields(source_file, ingested_at, run_id),
    }


def flatten_expense(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    meta_create, meta_updated = normalize_metadata(record)
    account = normalize_reference(record.get("AccountRef"), "AccountRef")
    entity = object_or_none(record.get("EntityRef"), "EntityRef")
    currency = normalize_reference(record.get("CurrencyRef"), "CurrencyRef")
    line_values = array_or_none(record.get("Line"), "Line")
    lines = None if line_values is None else [
        normalize_expense_line(line, index)
        for index, line in enumerate(line_values)
        if isinstance(line, dict)
    ]
    if line_values is not None and len(lines) != len(line_values):
        raise ValueError("Every Purchase Line entry must be a JSON object")
    return {
        "id": as_text(record.get("Id")),
        "sync_token": as_text(record.get("SyncToken")),
        "metadata_create_time": meta_create,
        "metadata_last_updated_time": meta_updated,
        "txn_date": as_text(record.get("TxnDate")),
        "payment_type": as_text(record.get("PaymentType")),
        "account_ref_value": account.get("value") if account else None,
        "account_ref_name": account.get("name") if account else None,
        "entity_ref_value": as_text(entity.get("value")) if entity else None,
        "entity_ref_name": as_text(entity.get("name")) if entity else None,
        "entity_ref_type": as_text(entity.get("type")) if entity else None,
        "line": lines,
        "total_amt": as_float(record.get("TotalAmt"), "TotalAmt"),
        "private_note": as_text(record.get("PrivateNote")),
        "currency_ref_value": currency.get("value") if currency else None,
        "currency_ref_name": currency.get("name") if currency else None,
        **audit_fields(source_file, ingested_at, run_id),
    }


FLATTENERS = {
    "invoices": flatten_invoice,
    "payments": flatten_payment,
    "expenses": flatten_expense,
}


def ensure_target_table(spark: SparkSession, table_name: str, schema: StructType) -> None:
    """Create an empty Delta table when absent; avoids unsupported errorifexists."""
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
    source_df = spark.createDataFrame(
        [(SOURCE_SYSTEM, source_file, int(record_count), processed_at, run_id)],
        PIPELINE_STATE_SCHEMA,
    )
    view_name = "_quickbooks_pipeline_state_new"
    source_df.createOrReplaceTempView(view_name)
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
    records: Sequence[Dict[str, Any]],
    source_file: str,
) -> None:
    """Replace rows from one unregistered source file before marking success."""
    ensure_target_table(spark, table_name, schema)
    spark.sql(f"DELETE FROM {table_name} WHERE source_file_path = {sql_string(source_file)}")
    if not records:
        log.info("No records to write for %s into %s", source_file, table_name)
        return
    df: DataFrame = spark.createDataFrame(list(records), schema=schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    log.info("Wrote %d rows to %s from %s", len(records), table_name, source_file)


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

    parts = Path(relative_path).parts
    if not parts or parts[0] not in SCHEMAS_BY_FOLDER:
        raise ValueError(f"File is outside recognized QuickBooks folders: {relative_path}")
    entity_key, table_suffix, schema = SCHEMAS_BY_FOLDER[parts[0]]
    table_name = tables[parts[0]]
    flattener = FLATTENERS[parts[0]]
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("Processing new file: %s (entity=%s)", relative_path, entity_key)

    try:
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            raise ValueError("QuickBooks JSON file must contain a top-level JSON object")
        query_response = payload.get("QueryResponse")
        if not isinstance(query_response, dict):
            raise ValueError("QuickBooks JSON file must contain a 'QueryResponse' object")
        records = query_response.get(entity_key, [])
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError(f"QueryResponse.{entity_key} must be a JSON array")

        normalized_rows: List[Dict[str, Any]] = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"QueryResponse.{entity_key}[{index}] must be a JSON object")
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
        log.exception("Failed to process %s; it remains unregistered and can be retried", relative_path)
        raise


def run() -> None:
    args = parse_args()
    if not args.source_folder:
        raise ValueError(
            "Set --source-folder or QUICKBOOKS_SOURCE_FOLDER to the QuickBooks folder "
            "accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/quickbooks"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Source folder does not exist or is not a directory: {source_root}")

    expected_folders = [source_root / name for name in SCHEMAS_BY_FOLDER]
    for folder in expected_folders:
        if not folder.is_dir():
            raise FileNotFoundError(f"Expected QuickBooks folder not found: {folder}")

    # Preserve the initial loader's layout: JSON files directly inside each
    # entity folder (not recursively grouped into month subfolders).
    files_by_folder = {
        folder_name: sorted(
            path for path in (source_root / folder_name).glob("*.json") if path.is_file()
        )
        for folder_name in SCHEMAS_BY_FOLDER
    }
    files = [path for folder_name in SCHEMAS_BY_FOLDER for path in files_by_folder[folder_name]]
    if not files:
        log.warning("No QuickBooks JSON files were found in the three entity folders.")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)
    tables = {
        folder_name: quote_table_name(catalog, bronze_schema, table_name)
        for folder_name, (_, table_name, _) in SCHEMAS_BY_FOLDER.items()
    }
    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)

    log.info("Starting QuickBooks ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered QuickBooks files: %d", len(processed_files))
    log.info("Discovered QuickBooks source files: %d", len(files))
    for folder_name, folder_files in files_by_folder.items():
        log.info("Discovered %d files under %s/", len(folder_files), folder_name)

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
        "QuickBooks ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count,
        skipped_count,
        len(failed_files),
        run_id,
    )
    if failed_files:
        raise RuntimeError(
            "QuickBooks ingestion failed for the following files (they remain unregistered and "
            f"can be retried): {', '.join(failed_files)}"
        )


if __name__ == "__main__":
    run()
