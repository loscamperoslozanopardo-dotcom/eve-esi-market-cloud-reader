import json
import os
import requests
from google.cloud import bigquery
from datetime import datetime, timezone
import time

# Cargar las configuraciones necesarias
PROJECT_ID = 'eve-market-sandbox'
DATASET_ID = 'eve_market'
TABLE_ID = 'markets_prices'
TABLE_DT_ID = 'markets_prices_dt'
STATE_FILE = 'state/emsp_hdrs.json'

# Variables de autenticación de BigQuery
client = bigquery.Client(project=PROJECT_ID)

# Función para obtener el estado de headers desde el archivo persistente
def load_headers():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

# Función para guardar el estado de headers en el archivo persistente
def save_headers(headers):
    with open(STATE_FILE, 'w') as f:
        json.dump(headers, f)

# Función para obtener la fecha actual en UTC
def current_time_utc():
    return datetime.now(timezone.utc).isoformat()

# Función para hacer la llamada a la API de ESI
def fetch_market_prices():
    url = 'https://esi.evetech.net/latest/markets/prices/?datasource=tranquility'
    headers = load_headers()
    etag = headers.get('etag', '')

    response = requests.get(url, headers={'If-None-Match': etag})
    
    if response.status_code == 200:
        return response.json(), response.headers
    elif response.status_code == 304:
        print("No new data, skipping load.")
        return None, response.headers
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Función para cargar datos en BigQuery
def load_to_bigquery(data, table_id):
    table_ref = client.dataset(DATASET_ID).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("type_id", "INT64"),
            bigquery.SchemaField("average_price", "FLOAT64"),
            bigquery.SchemaField("adjusted_price", "FLOAT64"),
        ],
        write_disposition="WRITE_TRUNCATE",  # Sobrescribe la tabla
    )
    
    load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
    load_job.result()  # Espera hasta que el trabajo esté completo
    print(f"Loaded {len(data)} rows to {table_id}")

# Función para actualizar el timestamp de expiration en markets_prices_dt
def update_expiration_dt(expires_utc):
    table_ref = client.dataset(DATASET_ID).table(TABLE_DT_ID)
    rows_to_insert = [{"expires_utc": expires_utc}]
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
    load_job.result()  # Espera hasta que el trabajo esté completo
    print("Updated expiration timestamp in markets_prices_dt")

def main():
    # 1. Intentar obtener los datos de ESI
    data, headers = fetch_market_prices()
    
    if data:
        # 2. Cargar los datos en BigQuery
        load_to_bigquery(data, TABLE_ID)
        
        # 3. Actualizar expiration timestamp en BigQuery
        expires_utc = headers.get('Expires')
        update_expiration_dt(expires_utc)
        
        # 4. Guardar headers persistentes (ETag, Last-Modified, Expires)
        save_headers({
            'etag': headers.get('ETag', ''),
            'last_modified': headers.get('Last-Modified', ''),
            'expires': expires_utc,
            'last_success_request_utc': current_time_utc()
        })
        print("Run completed successfully.")
    else:
        print("No data to load, exiting.")

if __name__ == "__main__":
    main()
