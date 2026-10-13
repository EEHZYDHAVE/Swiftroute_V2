"""Ingest Gusto JSON files into source-aligned Bronze Delta tables on Databricks.

Expected source layout::

    <source-folder>/
      employees/employees.json                 # JSON array of employee records
      payrolls/payroll_YYYY-MM-DD.json         # one payroll object per file

Tables created in the configured Unity Catalog catalog/schema:
    gusto_employees
    gusto_payroll_runs
    gusto_payroll_compensations
    pipeline_state

Incremental policy for this practice project:
    A source file is processed once per (source_system, relative_file_path).
    Re-running the job skips files already marked successful, even if a source
    file is later overwritten at the same path. This is intentional per the
    project requirements. New files with new paths are processed.

Important:
    - Keep source files in persistent storage accessible to Databricks, e.g.
      a Unity Catalog Volume at /Volumes/<catalog>/<schema>/<volume>/....
    - Configure the Databricks Job to allow only one concurrent run of this
      loader so two runs cannot race while checking/updating pipeline_state.
    - The loader replaces rows for a source_file_path before writing them. This
      makes retries safe if a previous run wrote target rows but failed before
      recording the file as processed. It does not reprocess registered files.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

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
log = logging.getLogger("load_gusto")

SOURCE_SYSTEM = "gusto"

# ---------------------------------------------------------------------------
# Source-aligned schemas
# ---------------------------------------------------------------------------

CUSTOM_FIELD_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("label", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

EMPLOYEE_SCHEMA = StructType(
    [
        StructField("uuid", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("company_uuid", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("job_title", StringType(), True),
        # Keep the source value as text in Bronze; normalize/cast in Silver.
        StructField("job_rate", StringType(), True),
        StructField("job_payment_unit", StringType(), True),
        StructField("job_hire_date", StringType(), True),
        StructField("employment_status", StringType(), True),
        StructField("termination_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("custom_fields", ArrayType(CUSTOM_FIELD_SCHEMA, True), True),
        StructField("_annual_salary", DoubleType(), True),
        StructField("_bi_weekly_gross", DoubleType(), True),
        StructField("_dept", StringType(), True),
        StructField("_is_driver", BooleanType(), True),
        StructField("_bonus_max", DoubleType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

PAYROLL_RUN_SCHEMA = StructType(
    [
        StructField("uuid", StringType(), True),
        StructField("company_uuid", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("version", StringType(), True),
        StructField("payroll_deadline", StringType(), True),
        StructField("check_date", StringType(), True),
        StructField("processed", BooleanType(), True),
        StructField("pay_period_start_date", StringType(), True),
        StructField("pay_period_end_date", StringType(), True),
        StructField("totals_company_debit", StringType(), True),
        StructField("totals_gross_pay", StringType(), True),
        StructField("totals_net_pay", StringType(), True),
        StructField("totals_employee_taxes", StringType(), True),
        StructField("totals_employer_taxes", StringType(), True),
        StructField("totals_employee_deductions", StringType(), True),
        StructField("source_file_path", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_run_id", StringType(), False),
    ]
)

FIXED_COMPENSATION_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("job_uuid", StringType(), True),
    ]
)

BENEFIT_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("employee_deduction", StringType(), True),
        StructField("company_contribution", StringType(), True),
        StructField("imputed", BooleanType(), True),
    ]
)

EMPLOYEE_DEDUCTION_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("pre_tax", BooleanType(), True),
    ]
)

TAX_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("employer", BooleanType(), True),
        StructField("amount", StringType(), True),
    ]
)

PAYROLL_COMPENSATION_SCHEMA = StructType(
    [
        StructField("payroll_run_uuid", StringType(), True),
        StructField("company_uuid", StringType(), True),
        StructField("company_id", StringType(), True),
        StructField("payroll_version", StringType(), True),
        StructField("payroll_deadline", StringType(), True),
        StructField("check_date", StringType(), True),
        StructField("pay_period_start_date", StringType(), True),
        StructField("pay_period_end_date", StringType(), True),
        StructField("compensation_ordinal", IntegerType(), False),
        StructField("employee_uuid", StringType(), True),
        StructField("employee_first_name", StringType(), True),
        StructField("employee_last_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField(
            "fixed_compensations", ArrayType(FIXED_COMPENSATION_SCHEMA, True), True
        ),
        # These two arrays are empty in the supplied examples, so their actual
        # populated schemas are not yet known. Preserve them as JSON text until
        # a populated example is available rather than silently losing fields.
        StructField("variable_compensations_json", StringType(), True),
        StructField("paid_time_off_json", StringType(), True),
        StructField("benefits", ArrayType(BENEFIT_SCHEMA, True), True),
        StructField(
            "employee_deductions", ArrayType(EMPLOYEE_DEDUCTION_SCHEMA, True), True
        ),
        StructField("taxes", ArrayType(TAX_SCHEMA, True), True),
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
# Configuration and helpers
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-folder",
        default=os.getenv("GUSTO_SOURCE_FOLDER"),
        help="Absolute path accessible to Databricks, e.g. /Volumes/.../source_data/gusto",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("UC_CATALOG", "swiftroute_dev"),
        help="Unity Catalog catalog containing the Bronze schema (default: swiftroute_dev)",
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


def json_compact(value: Any) -> str:
    return json.dumps(value if value is not None else [], ensure_ascii=False, separators=(",", ":"))


def source_relative_path(source_root: Path, file_path: Path) -> str:
    return file_path.relative_to(source_root).as_posix()


def read_json_file(file_path: Path) -> Any:
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_target_table(
    spark: SparkSession, table_name: str, schema: StructType
) -> None:
    """Create an empty Delta table with the requested schema when absent.

    Databricks Free Edition uses Spark Connect for serverless execution, where
    the ``errorifexists`` writer mode is not supported. We check existence
    first and use ``overwrite`` only for the initial empty-table creation.
    The job should be configured for a single concurrent run of this loader.
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
    """Register a file only after all output tables have been written successfully."""
    source_row = [
        (SOURCE_SYSTEM, source_file, int(record_count), processed_at, run_id)
    ]
    source_df = spark.createDataFrame(source_row, PIPELINE_STATE_SCHEMA)
    source_df.createOrReplaceTempView("_gusto_pipeline_state_new")
    try:
        spark.sql(
            f"""
            MERGE INTO {state_table} AS target
            USING _gusto_pipeline_state_new AS source
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
        spark.catalog.dropTempView("_gusto_pipeline_state_new")


def replace_rows_for_file(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    records: Sequence[Dict[str, Any]],
    source_file: str,
) -> None:
    """Idempotently replace rows attributed to this file before appending its records.

    This handles retries after partial output writes. It does NOT cause an already
    registered source file to be re-read; registered files are skipped earlier.
    """
    ensure_target_table(spark, table_name, schema)
    escaped_file = sql_string(source_file)
    spark.sql(
        f"DELETE FROM {table_name} WHERE source_file_path = {escaped_file}"
    )

    if not records:
        log.info("No records to write for %s into %s", source_file, table_name)
        return

    df = spark.createDataFrame(list(records), schema=schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)
    log.info("Wrote %d records to %s from %s", len(records), table_name, source_file)


# ---------------------------------------------------------------------------
# Flatten source JSON into source-aligned Bronze rows
# ---------------------------------------------------------------------------


def flatten_employee(
    record: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    job = record.get("job") or {}
    custom_fields = []
    for item in record.get("custom_fields") or []:
        custom_fields.append(
            {
                "id": item.get("id"),
                "label": item.get("label"),
                "value": item.get("value"),
            }
        )

    # _note is a dataset annotation rather than an employee business attribute.
    # The original source file remains available in the source-data location.
    return {
        "uuid": record.get("uuid"),
        "first_name": record.get("first_name"),
        "last_name": record.get("last_name"),
        "email": record.get("email"),
        "company_uuid": record.get("company_uuid"),
        "company_id": record.get("company_id"),
        "department": record.get("department"),
        "job_title": job.get("title"),
        "job_rate": None if job.get("rate") is None else str(job.get("rate")),
        "job_payment_unit": job.get("payment_unit"),
        "job_hire_date": job.get("hire_date"),
        "employment_status": record.get("employment_status"),
        "termination_date": record.get("termination_date"),
        "phone": record.get("phone"),
        "date_of_birth": record.get("date_of_birth"),
        "custom_fields": custom_fields,
        "_annual_salary": record.get("_annual_salary"),
        "_bi_weekly_gross": record.get("_bi_weekly_gross"),
        "_dept": record.get("_dept"),
        "_is_driver": record.get("_is_driver"),
        "_bonus_max": record.get("_bonus_max"),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def flatten_payroll_run(
    payload: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> Dict[str, Any]:
    pay_period = payload.get("pay_period") or {}
    totals = payload.get("totals") or {}

    return {
        "uuid": payload.get("uuid"),
        "company_uuid": payload.get("company_uuid"),
        "company_id": payload.get("company_id"),
        "version": payload.get("version"),
        "payroll_deadline": payload.get("payroll_deadline"),
        "check_date": payload.get("check_date"),
        "processed": payload.get("processed"),
        "pay_period_start_date": pay_period.get("start_date"),
        "pay_period_end_date": pay_period.get("end_date"),
        "totals_company_debit": None if totals.get("company_debit") is None else str(totals.get("company_debit")),
        "totals_gross_pay": None if totals.get("gross_pay") is None else str(totals.get("gross_pay")),
        "totals_net_pay": None if totals.get("net_pay") is None else str(totals.get("net_pay")),
        "totals_employee_taxes": None if totals.get("employee_taxes") is None else str(totals.get("employee_taxes")),
        "totals_employer_taxes": None if totals.get("employer_taxes") is None else str(totals.get("employer_taxes")),
        "totals_employee_deductions": None if totals.get("employee_deductions") is None else str(totals.get("employee_deductions")),
        "source_file_path": source_file,
        "ingested_at": ingested_at,
        "ingestion_run_id": run_id,
    }


def normalize_fixed_compensations(items: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in items or []:
        output.append(
            {
                "name": item.get("name"),
                "amount": None if item.get("amount") is None else str(item.get("amount")),
                "job_uuid": item.get("job_uuid"),
            }
        )
    return output


def normalize_benefits(items: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in items or []:
        output.append(
            {
                "name": item.get("name"),
                "employee_deduction": None if item.get("employee_deduction") is None else str(item.get("employee_deduction")),
                "company_contribution": None if item.get("company_contribution") is None else str(item.get("company_contribution")),
                "imputed": item.get("imputed"),
            }
        )
    return output


def normalize_employee_deductions(items: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in items or []:
        output.append(
            {
                "name": item.get("name"),
                "amount": None if item.get("amount") is None else str(item.get("amount")),
                "pre_tax": item.get("pre_tax"),
            }
        )
    return output


def normalize_taxes(items: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in items or []:
        output.append(
            {
                "name": item.get("name"),
                "employer": item.get("employer"),
                "amount": None if item.get("amount") is None else str(item.get("amount")),
            }
        )
    return output


def flatten_payroll_compensations(
    payload: Dict[str, Any], source_file: str, ingested_at: datetime, run_id: str
) -> List[Dict[str, Any]]:
    pay_period = payload.get("pay_period") or {}
    compensations = payload.get("employee_compensations") or []
    output: List[Dict[str, Any]] = []

    for ordinal, comp in enumerate(compensations):
        output.append(
            {
                "payroll_run_uuid": payload.get("uuid"),
                "company_uuid": payload.get("company_uuid"),
                "company_id": payload.get("company_id"),
                "payroll_version": payload.get("version"),
                "payroll_deadline": payload.get("payroll_deadline"),
                "check_date": payload.get("check_date"),
                "pay_period_start_date": pay_period.get("start_date"),
                "pay_period_end_date": pay_period.get("end_date"),
                "compensation_ordinal": ordinal,
                "employee_uuid": comp.get("employee_uuid"),
                "employee_first_name": comp.get("employee_first_name"),
                "employee_last_name": comp.get("employee_last_name"),
                "department": comp.get("department"),
                "fixed_compensations": normalize_fixed_compensations(comp.get("fixed_compensations")),
                # Empty in the provided samples; preserve the JSON until populated examples
                # allow us to define stable typed schemas without guessing.
                "variable_compensations_json": json_compact(comp.get("variable_compensations")),
                "paid_time_off_json": json_compact(comp.get("paid_time_off")),
                "benefits": normalize_benefits(comp.get("benefits")),
                "employee_deductions": normalize_employee_deductions(comp.get("employee_deductions")),
                "taxes": normalize_taxes(comp.get("taxes")),
                "source_file_path": source_file,
                "ingested_at": ingested_at,
                "ingestion_run_id": run_id,
            }
        )
    return output


# ---------------------------------------------------------------------------
# Incremental ingestion
# ---------------------------------------------------------------------------


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

    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("Processing new file: %s", relative_path)

    try:
        payload = read_json_file(file_path)

        if relative_path == "employees/employees.json":
            if not isinstance(payload, list):
                raise ValueError("employees/employees.json must contain a top-level JSON array")
            employee_rows = [
                flatten_employee(record, relative_path, ingested_at, run_id)
                for record in payload
            ]
            replace_rows_for_file(
                spark,
                tables["employees"],
                EMPLOYEE_SCHEMA,
                employee_rows,
                relative_path,
            )
            total_count = len(employee_rows)
        else:
            if not isinstance(payload, dict):
                raise ValueError("Each payroll JSON file must contain a top-level JSON object")
            payroll_uuid = payload.get("uuid")
            if not payroll_uuid:
                raise ValueError("Payroll file is missing its required 'uuid' field")

            payroll_row = flatten_payroll_run(payload, relative_path, ingested_at, run_id)
            compensation_rows = flatten_payroll_compensations(
                payload, relative_path, ingested_at, run_id
            )

            replace_rows_for_file(
                spark,
                tables["payroll_runs"],
                PAYROLL_RUN_SCHEMA,
                [payroll_row],
                relative_path,
            )
            replace_rows_for_file(
                spark,
                tables["payroll_compensations"],
                PAYROLL_COMPENSATION_SCHEMA,
                compensation_rows,
                relative_path,
            )
            total_count = 1 + len(compensation_rows)

        mark_file_processed(
            spark=spark,
            state_table=state_table,
            source_file=relative_path,
            record_count=total_count,
            run_id=run_id,
            processed_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        processed_files.add(relative_path)
        log.info("Successfully processed %s (%d records)", relative_path, total_count)
        return True

    except Exception:
        log.exception("Failed to process %s; it will remain unregistered and can be retried", relative_path)
        raise


def run() -> None:
    args = parse_args()
    if not args.source_folder:
        raise ValueError(
            "Set --source-folder or the GUSTO_SOURCE_FOLDER environment variable to a "
            "path accessible to Databricks, e.g. /Volumes/<catalog>/<schema>/<volume>/source_data/gusto"
        )

    catalog = validate_identifier(args.catalog, "catalog")
    bronze_schema = validate_identifier(args.bronze_schema, "Bronze schema")
    source_root = Path(args.source_folder)
    if not source_root.exists() or not source_root.is_dir():
        raise FileNotFoundError(f"Source folder does not exist or is not a directory: {source_root}")

    employees_file = source_root / "employees" / "employees.json"
    payrolls_folder = source_root / "payrolls"
    if not employees_file.is_file():
        raise FileNotFoundError(f"Expected Gusto employee file not found: {employees_file}")
    if not payrolls_folder.is_dir():
        raise FileNotFoundError(f"Expected Gusto payroll folder not found: {payrolls_folder}")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    schema_quoted = f"`{catalog}`.`{bronze_schema}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_quoted}")

    state_table = ensure_pipeline_state_table(spark, catalog, bronze_schema)
    tables = {
        "employees": quote_table_name(catalog, bronze_schema, "gusto_employees"),
        "payroll_runs": quote_table_name(catalog, bronze_schema, "gusto_payroll_runs"),
        "payroll_compensations": quote_table_name(
            catalog, bronze_schema, "gusto_payroll_compensations"
        ),
    }

    run_id = str(uuid.uuid4())
    processed_files = get_processed_files(spark, state_table)
    log.info("Starting Gusto ingestion; run_id=%s", run_id)
    log.info("Source root: %s", source_root)
    log.info("Bronze schema: %s.%s", catalog, bronze_schema)
    log.info("Previously registered Gusto files: %d", len(processed_files))

    # Process the stable employee snapshot first, then discover payroll JSON files
    # recursively so this loader also works if files are later placed in date folders.
    files = [employees_file] + sorted(
        path for path in payrolls_folder.rglob("*.json") if path.is_file()
    )

    failed_files: List[str] = []
    loaded_count = 0
    skipped_count = 0

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
        "Gusto ingestion summary: loaded_files=%d skipped_files=%d failed_files=%d run_id=%s",
        loaded_count,
        skipped_count,
        len(failed_files),
        run_id,
    )
    if failed_files:
        raise RuntimeError(
            "Gusto ingestion failed for the following file(s): " + ", ".join(failed_files)
        )


if __name__ == "__main__":
    run()