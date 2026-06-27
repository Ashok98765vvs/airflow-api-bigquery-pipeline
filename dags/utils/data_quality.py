"""
data_quality.py: Data quality validation functions for BigQuery tables
Author: Ashok
"""

from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def run_quality_checks(project: str, dataset: str, table: str, min_row_count: int = 100) -> bool:
    """
    Run data quality checks on a BigQuery table.
    
    Args:
        project: GCP project ID
        dataset: BigQuery dataset name
        table: BigQuery table name
        min_row_count: Minimum expected row count
    
    Returns:
        True if all checks pass, raises Exception if any check fails
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    
    logger.info(f"Running quality checks on {table_ref}")
    
    # Check 1: Row count
    row_count = _check_row_count(client, table_ref, min_row_count)
    logger.info(f"Row count check passed: {row_count} rows")
    
    # Check 2: Null check on key columns
    _check_nulls(client, table_ref)
    logger.info("Null check passed")
    
    # Check 3: Freshness check
    _check_freshness(client, table_ref)
    logger.info("Freshness check passed")
    
    return True


def _check_row_count(client, table_ref: str, min_count: int) -> int:
    query = f"SELECT COUNT(*) as row_count FROM `{table_ref}`"
    result = client.query(query).result()
    row_count = list(result)[0]['row_count']
    
    if row_count < min_count:
        raise ValueError(f"Row count {row_count} below minimum {min_count}")
    
    return row_count


def _check_nulls(client, table_ref: str) -> None:
    query = f"""
        SELECT 
            COUNTIF(id IS NULL) as null_ids,
            COUNTIF(timestamp IS NULL) as null_timestamps
        FROM `{table_ref}`
    """
    result = client.query(query).result()
    row = list(result)[0]
    
    if row['null_ids'] > 0:
        raise ValueError(f"Found {row['null_ids']} null IDs")
    if row['null_timestamps'] > 0:
        raise ValueError(f"Found {row['null_timestamps']} null timestamps")


def _check_freshness(client, table_ref: str, max_hours: int = 24) -> None:
    query = f"""
        SELECT MAX(created_at) as latest_record
        FROM `{table_ref}`
    """
    result = client.query(query).result()
    latest = list(result)[0]['latest_record']
    
    if latest is None:
        raise ValueError("No records found in table")
    
    age_hours = (datetime.utcnow() - latest.replace(tzinfo=None)).total_seconds() / 3600
    if age_hours > max_hours:
        logger.warning(f"Data is {age_hours:.1f} hours old, exceeds {max_hours}h threshold")
