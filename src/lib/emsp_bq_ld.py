from datetime import datetime, timezone
from typing import Dict, List, Optional

from google.cloud import bigquery


def ensure_dataset(project_id: str, dataset_id: str, location: str) -> None:
    client = bigquery.Client(project=project_id)
    ds_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)


def load_prices_to_bigquery_from_list(
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: List[Dict],
    location: str,
) -> int:
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("type_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("average_price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("adjusted_price", "FLOAT64", mode="NULLABLE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    # Convert list-of-dicts to JSONL bytes in-memory (es relativamente pequeño para este endpoint)
    # Si quisieras 0-mem: se podría escribir a /tmp y load_table_from_file.
    import json
    payload = ("\n".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) for r in rows) + "\n").encode("utf-8")

    from io import BytesIO
    bio = BytesIO(payload)

    job = client.load_table_from_file(bio, table_ref, job_config=job_config, location=location)
    job.result()

    # Si la tabla no existía, ahora existe; devolvemos filas pretendidas (aprox)
    return len(rows)


def write_prices_dt_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    expires_dt: datetime,
    location: str,
) -> None:
    client = bigquery.Client(project=project_id)

    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    expires_dt = expires_dt.astimezone(timezone.utc)

    sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
    SELECT @expires_utc AS expires_utc
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("expires_utc", "TIMESTAMP", expires_dt),
        ]
    )

    job = client.query(sql, job_config=job_config, location=location)
    job.result()
