import requests

# Función para realizar la llamada a la API de ESI con manejo de errores
def make_esi_request(url, headers={}):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"HTTP request failed: {e}")
        return None
