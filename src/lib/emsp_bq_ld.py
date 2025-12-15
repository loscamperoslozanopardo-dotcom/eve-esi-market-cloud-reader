from google.cloud import bigquery

# Función para cargar datos en BigQuery
def load_data_to_bq(client, dataset_id, table_id, data, schema=None, write_disposition="WRITE_TRUNCATE"):
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition
    )
    
    load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
    load_job.result()  # Espera hasta que el trabajo esté completo
    print(f"Loaded data to BigQuery table: {table_id}")
