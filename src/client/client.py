import openmeteo_requests
import requests
from config.settings import AEMET_API_KEY, AEMET_BASE_URL
import requests_cache
from retry_requests import retry
url = "https://archive-api.open-meteo.com/v1/archive"

class AEMETClient:
    def get_estaciones(self):
        #url = f"{AEMET_BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones/"
        url=f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/fechaini/2024-01-01T00:00:00UTC/fechafin/2024-01-10T23:59:59UTC/todasestaciones/"
        url=f"{AEMET_BASE_URL}/observacion/convencional/datos/estacion/3195"
        querystring = {"api_key": AEMET_API_KEY}
        headers = {'cache-control': "no-cache"}
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()

        data = response.json()
        datos_url = data["datos"]

        response_datos = requests.get(datos_url)
        response_datos.raise_for_status()

        return response_datos.json()
        
class OpenMeteoClient:
    def get_data(self,lat,lon,start_date,end_date,values):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": values,
        }
        responses = openmeteo.weather_api(url, params=params)
        response=responses[0]
        return response