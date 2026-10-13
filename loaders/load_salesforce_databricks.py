"""Ingest Salesforce JSON exports into structured Bronze Delta tables on Databricks.

Expected source layout (matching the existing load_salesforce.py)::

    <source-folder>/
      accounts/*.json         # top-level records array
      contracts/*.json        # top-level records array
      contract_rates/*.json   # top-level records array
      opportunities/*.json    # top-level records array

Each file contains a Salesforce query-response envelope with a ``records`` array.
Records are normalized into source-aligned structured columns; useful related
objects (BillingAddress, Owner, and Account) are retained as typed structs.
Source dates/timestamps remain strings for standardization in Silver Technical.

Target tables (in the configured Unity Catalog catalog/schema):
    salesforce_accounts
    salesforce_contracts
    salesforce_contract_rates
    salesforce_opportunities
    pipeline_state  # shared with other source loaders

Incremental policy: each relative file path is processed once per source system.
Previously registered paths are skipped even if their contents change. Rows for
an unregistered file are deleted and rewritten before the file is registered,
so retries after a partial write do not duplicate rows. Run one ingestion task
at a time while sharing pipeline_state.
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
log = logging.getLogger("load_salesforce")

SOURCE_SYSTEM = "salesforce"

# ---------------------------------------------------------------------------
# Nested source-aligned schemas
# ---------------------------------------------------------------------------

SALESFORCE_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("type", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

BILLING_ADDRESS_SCHEMA = StructType(
    [
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("country_code", StringType(), True),
    ]
)

OWNER_SCHEMA = StructType(
    [
        StructField("attributes", SALESFORCE_ATTRIBUTES_SCHEMA, True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

RELATED_ACCOUNT_SCHEMA = StructType(
    [
        StructField("attributes", SALESFORCE_ATTRIBUTES_SCHEMA, True),
        StructField("name", StringType(), True),
        StructField("swiftroute_client_id", StringType(), True),
    ]
)

AUDIT_FIELDS = [
    StructField("source_file_path", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
    StructField("ingestion_run_id", StringType(), False),
]

ACCOUNTS_SCHEMA = StructType(
    [
        StructField("attributes_type", StringType(), True),
        StructField("attributes_url", StringType(), True),
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website", StringType(), True),
        StructField("billing_address", BILLING_ADDRESS_SCHEMA, True),
        StructField("annual_revenue", DoubleType(), True),
        StructField("number_of_employees", IntegerType(), True),
        StructField("last_activity_date", StringType(), True),
        StructField("last_modified_date", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("owner", OWNER_SCHEMA, True),
        StructField("swiftroute_client_id", StringType(), True),
        StructField("account_tier", StringType(), True),
        StructField("primary_city", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("contracted_monthly_volume", IntegerType(), True),
        StructField("discount_rate", DoubleType(), True),
        StructField("is_fulfillment_client", BooleanType(), True),
        StructField("net_payment_terms", IntegerType(), True),
        *AUDIT_FIELDS,
    ]
)

CONTRACTS_SCHEMA = StructType(
    [
        StructField("attributes_type", StringType(), True),
        StructField("attributes_url", StringType(), True),
        StructField("id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account", RELATED_ACCOUNT_SCHEMA, True),
        StructField("status", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("contract_term", IntegerType(), True),
        StructField("signed_date", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("committed_monthly_volume", IntegerType(), True),
        StructField("discount_rate", DoubleType(), True),
        StructField("net_payment_terms", IntegerType(), True),
        StructField("auto_renewal", BooleanType(), True),
        StructField("termination_notice_days", IntegerType(), True),
        StructField("primary_city", StringType(), True),
        StructField("account_tier", StringType(), True),
        StructField("rate_note", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("last_modified_date", StringType(), True),
        StructField("owner_id", StringType(), True),
        *AUDIT_FIELDS,
    ]
)

CONTRACT_RATES_SCHEMA = StructType(
    [
        StructField("attributes_type", StringType(), True),
        StructField("attributes_url", StringType(), True),
        StructField("id", StringType(), True),
        StructField("contract_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("swiftroute_client_id", StringType(), True),
        StructField("service_type", StringType(), True),
        StructField("zone_id", StringType(), True),
        StructField("zone_name", StringType(), True),
        StructField("base_rate", DoubleType(), True),
        StructField("discount_rate", DoubleType(), True),
        StructField("net_rate", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("effective_date", StringType(), True),
        StructField("created_date", StringType(), True),
        *AUDIT_FIELDS,
    ]
)

OPPORTUNITIES_SCHEMA = StructType(
    [
        StructField("attributes_type", StringType(), True),
        StructField("attributes_url", StringType(), True),
        StructField("id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("account", RELATED_ACCOUNT_SCHEMA, True),
        StructField("opportunity_name", StringType(), True),
        StructField("stage_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("close_date", StringType(), True),
        StructField("probability", DoubleType(), True),
        StructField("opportunity_type", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("owner_id", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("last_activity_date", StringType(), True),
        StructField("contract_type", StringType(), True),
        StructField("primary_city", StringType(), True),
        StructField("account_tier", StringType(), True),
        *AUDIT_FIELDS,
    ]
)

# Folder -> table name, table schema, and entity normalizer.
ENTITY_SPECS: Dict[str, Tuple[str, StructType, str]] = {
    "accounts": ("salesforce_accounts", ACCOUNTS_SCHEMA, "accounts"),
    "contracts": ("salesforce_contracts", CONTRACTS_SCHEMA, "contracts"),
    "contract_rates": ("salesforce_contract_rates", CONTRACT_RATES_SCHEMA, "contract_rates"),
    "opportunities": ("salesforce_opportunities", OPPORTUNITIES_SCHEMA, "opportunities"),
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
# Configuration and shared helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("SALESFORCE_SOURCE_FOLDER"),
        help=(
            "Absolute Salesforce source path accessible to Databricks, e.g. "
            "/Volumes/swiftroute/bronze/source_files/salesforce"
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
    """Represent scalar source attributes and Salesforce IDs without coercing IDs to numbers."""
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
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"Expected integer value for {field_name}, got {value!r}")
    return parsed


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


def normalize_attributes(value: Any, field_name: str) -> Dict[str, Any] | None:
    attributes = object_or_none(value, field_name)
    if attributes is None:
        return None
    return {
        "type": as_text(attributes.get("type")),
        "url": as_text(attributes.get("url")),
    }


def normalize_top_attributes(record: Dict[str, Any]) -> Dict[str, str | None]:
    attributes = normalize_attributes(record.get("attributes"), "attributes") or {}
    return {
        "attributes_type": attributes.get("type"),
        "attributes_url": attributes.get("url"),
    }


def normalize_billing_address(value: Any) -> Dict[str, Any] | None:
    address = object_or_none(value, "BillingAddress")
    if address is None:
        return None
    return {
        "street": as_text(address.get("street")),
        "city": as_text(address.get("city")),
        "state": as_text(address.get("state")),
        "postal_code": as_text(address.get("postalCode")),
        "country": as_text(address.get("country")),
        "state_code": as_text(address.get("stateCode")),
        "country_code": as_text(address.get("countryCode")),
    }


def normalize_owner(value: Any) -> Dict[str, Any] | None:
    owner = object_or_none(value, "Owner")
    if owner is None:
        return None
    return {
        "attributes": normalize_attributes(owner.get("attributes"), "Owner.attributes"),
        "name": as_text(owner.get("Name")),
        "email": as_text(owner.get("Email")),
    }


def normalize_related_account(value: Any, field_name: str = "Account") -> Dict[str, Any] | None:
    account = object_or_none(value, field_name)
    if account is None:
        return None
    return {
        "attributes": normalize_attributes(account.get("attributes"), f"{field_name}.attributes"),
        "name": as_text(account.get("Name")),
        "swiftroute_client_id": as_text(account.get("SwiftRoute_Client_ID__c")),
    }


def audit_fields(source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    return {
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


# ---------------------------------------------------------------------------
# Entity normalizers
# ---------------------------------------------------------------------------


def normalize_account(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    row = normalize_top_attributes(record)
    row.update(
        {
            "id": as_text(record.get("Id")),
            "name": as_text(record.get("Name")),
            "account_type": as_text(record.get("Type")),
            "industry": as_text(record.get("Industry")),
            "phone": as_text(record.get("Phone")),
            "website": as_text(record.get("Website")),
            "billing_address": normalize_billing_address(record.get("BillingAddress")),
            "annual_revenue": as_float(record.get("AnnualRevenue"), "AnnualRevenue"),
            "number_of_employees": as_int(record.get("NumberOfEmployees"), "NumberOfEmployees"),
            "last_activity_date": as_text(record.get("LastActivityDate")),
            "last_modified_date": as_text(record.get("LastModifiedDate")),
            "created_date": as_text(record.get("CreatedDate")),
            "owner_id": as_text(record.get("OwnerId")),
            "owner": normalize_owner(record.get("Owner")),
            "swiftroute_client_id": as_text(record.get("SwiftRoute_Client_ID__c")),
            "account_tier": as_text(record.get("Account_Tier__c")),
            "primary_city": as_text(record.get("Primary_City__c")),
            "contract_type": as_text(record.get("Contract_Type__c")),
            "contracted_monthly_volume": as_int(
                record.get("Contracted_Monthly_Volume__c"), "Contracted_Monthly_Volume__c"
            ),
            "discount_rate": as_float(record.get("Discount_Rate__c"), "Discount_Rate__c"),
            "is_fulfillment_client": as_bool(
                record.get("Is_Fulfillment_Client__c"), "Is_Fulfillment_Client__c"
            ),
            "net_payment_terms": as_int(record.get("Net_Payment_Terms__c"), "Net_Payment_Terms__c"),
            **audit_fields(source_file, ingested_at, run_id),
        }
    )
    return row


def normalize_contract(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    row = normalize_top_attributes(record)
    row.update(
        {
            "id": as_text(record.get("Id")),
            "account_id": as_text(record.get("AccountId")),
            "account": normalize_related_account(record.get("Account")),
            "status": as_text(record.get("Status")),
            "start_date": as_text(record.get("StartDate")),
            "end_date": as_text(record.get("EndDate")),
            "contract_term": as_int(record.get("ContractTerm"), "ContractTerm"),
            "signed_date": as_text(record.get("SignedDate__c")),
            "contract_type": as_text(record.get("Contract_Type__c")),
            "committed_monthly_volume": as_int(
                record.get("Committed_Monthly_Volume__c"), "Committed_Monthly_Volume__c"
            ),
            "discount_rate": as_float(record.get("Discount_Rate__c"), "Discount_Rate__c"),
            "net_payment_terms": as_int(record.get("Net_Payment_Terms__c"), "Net_Payment_Terms__c"),
            "auto_renewal": as_bool(record.get("Auto_Renewal__c"), "Auto_Renewal__c"),
            "termination_notice_days": as_int(
                record.get("Termination_Notice_Days__c"), "Termination_Notice_Days__c"
            ),
            "primary_city": as_text(record.get("Primary_City__c")),
            "account_tier": as_text(record.get("Account_Tier__c")),
            "rate_note": as_text(record.get("_rate_note")),
            "created_date": as_text(record.get("CreatedDate")),
            "last_modified_date": as_text(record.get("LastModifiedDate")),
            "owner_id": as_text(record.get("OwnerId")),
            **audit_fields(source_file, ingested_at, run_id),
        }
    )
    return row


def normalize_contract_rate(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    row = normalize_top_attributes(record)
    row.update(
        {
            "id": as_text(record.get("Id")),
            "contract_id": as_text(record.get("Contract__c")),
            "account_id": as_text(record.get("Account__c")),
            "swiftroute_client_id": as_text(record.get("SwiftRoute_Client_ID__c")),
            "service_type": as_text(record.get("Service_Type__c")),
            "zone_id": as_text(record.get("Zone_ID__c")),
            "zone_name": as_text(record.get("Zone_Name__c")),
            "base_rate": as_float(record.get("Base_Rate__c"), "Base_Rate__c"),
            "discount_rate": as_float(record.get("Discount_Rate__c"), "Discount_Rate__c"),
            "net_rate": as_float(record.get("Net_Rate__c"), "Net_Rate__c"),
            "unit": as_text(record.get("Unit__c")),
            "effective_date": as_text(record.get("Effective_Date__c")),
            "created_date": as_text(record.get("CreatedDate")),
            **audit_fields(source_file, ingested_at, run_id),
        }
    )
    return row


def normalize_opportunity(record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str) -> Dict[str, Any]:
    row = normalize_top_attributes(record)
    row.update(
        {
            "id": as_text(record.get("Id")),
            "account_id": as_text(record.get("AccountId")),
            "account": normalize_related_account(record.get("Account")),
            "opportunity_name": as_text(record.get("Name")),
            "stage_name": as_text(record.get("StageName")),
            "amount": as_float(record.get("Amount"), "Amount"),
            "close_date": as_text(record.get("CloseDate")),
            "probability": as_float(record.get("Probability"), "Probability"),
            "opportunity_type": as_text(record.get("Type")),
            "lead_source": as_text(record.get("LeadSource")),
            "owner_id": as_text(record.get("OwnerId")),
            "created_date": as_text(record.get("CreatedDate")),
            "last_activity_date": as_text(record.get("LastActivityDate")),
            "contract_type": as_text(record.get("Contract_Type__c")),
            "primary_city": as_text(record.get("Primary_City__c")),
            "account_tier": as_text(record.get("Account_Tier__c")),
            **audit_fields(source_file, ingested_at, run_id),
        }
    )
    return row


NORMALIZERS = {
    "accounts": normalize_account,
    "contracts": normalize_contract,
    "contract_rates": normalize_contract_rate,
    "opportunities": normalize_opportunity,
}


# ---------------------------------------------------------------------------
# Delta/Unity Catalog helpers
# ---------------------------------------------------------------------------


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
    view_name = "_salesforce_pipeline_state_new"
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

    path_parts = Path(relative_path).parts
    if not path_parts or path_parts[0] not in ENTITY_SPECS:
        raise ValueError(f"File is outside recognized Salesforce entity folders: {relative_path}")

    folder_name = path_parts[0]
    table_suffix, schema, _ = ENTITY_SPECS[folder_name]
    table_name = tables[folder_name]
    normalizer = NORMALIZERS[folder_name]
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    log.info("Processing new file: %s (entity=%s)", relative_path, folder_name)
    try:
        payload = read_json_file(file_path)
        if not isinstance(payload, dict):
            raise ValueError("Salesforce JSON file must contain a top-level JSON object")
        records = payload.get("records", [])
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("Salesforce payload 'records' must be a JSON array")

        normalized_rows: List[Dict[str, Any]] = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"records[{index}] must be a JSON object")
            normalized_rows.append(normalizer(record, relative_path, ingested_at, run_id))

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
            "Set --source-folder or SALESFORCE_SOURCE_FOLDER to the Salesforce source folder "
            "accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/salesforce"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Salesforce source folder does not exist or is not a directory: {source_root}")

    expected_folders = [source_root / name for name in ENTITY_SPECS]
    for folder in expected_folders:
        if not folder.is_dir():
            raise FileNotFoundError(f"Expected Salesforce entity folder not found: {folder}")

    # Preserve the original loader's layout: JSON files directly in each entity folder.
    files_by_folder = {
        folder_name: sorted(
            path for path in (source_root / folder_name).glob("*.json") if path.is_file()
        )
        for folder_name in ENTITY_SPECS
    }
    files = [path for folder_name in ENTITY_SPECS for path in files_by_folder[folder_name]]
    if not files:
        log.warning("No Salesforce JSON files were found in the four entity folders.")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)
    tables = {
        folder_name: quote_table_name(catalog, bronze_schema, table_name)
        for folder_name, (table_name, _, _) in ENTITY_SPECS.items()
    }
    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)

    log.info("Starting Salesforce ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered Salesforce files: %d", len(processed_files))
    log.info("Discovered Salesforce source files: %d", len(files))
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
        "Salesforce ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count,
        skipped_count,
        len(failed_files),
        run_id,
    )
    if failed_files:
        raise RuntimeError(
            "Salesforce ingestion failed for the following files (they remain unregistered and "
            f"can be retried): {', '.join(failed_files)}"
        )


if __name__ == "__main__":
    run()
