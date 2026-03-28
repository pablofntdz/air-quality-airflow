from datetime import datetime
from include.src.download_data import download_data, get_timestamp, setup_bucket,build_raw_object_path
from include.config.paths import RAW_DIR 
from include.config.urls import URLS 

from airflow.sdk import dag, task, Asset,Variable

air_raw_data_asset = Asset("air_raw_data_ready")
meteo_raw_data_asset = Asset("meteo_raw_data_ready")
traffic_raw_data_asset = Asset("traffic_raw_data_ready")

@dag(schedule="@hourly", start_date=datetime(2024, 1, 1), catchup=False,tags=["download", "raw"], default_args={"retries": 2})
def download_pipeline():
    @task
    def task_setup():
        setup_bucket()
        timestamp = str(get_timestamp())
        Variable.set("last_download_timestamp", timestamp)
        return timestamp

    @task(outlets=[air_raw_data_asset])
    def task_download_air(timestamp): 
        download_data(URLS["air"], build_raw_object_path("air", f"measure_air_{timestamp}.csv",timestamp))
        download_data(URLS["air_est"], build_raw_object_path("air", f"stations_air_{timestamp}.csv",timestamp))

    @task(outlets=[meteo_raw_data_asset])
    def task_download_meteo(timestamp):
        download_data(URLS["meteo"],  build_raw_object_path("meteo", f"measure_meteo_{timestamp}.json",timestamp))
        download_data(URLS["meteo_est"], build_raw_object_path("meteo", f"stations_meteo_{timestamp}.csv",timestamp))

    @task(outlets=[traffic_raw_data_asset])    
    def task_download_traffic(timestamp):
        download_data(URLS["traffic"], build_raw_object_path("traffic", f"measure_traffic_{timestamp}.xml",timestamp))
        download_data(URLS["traffic_est"], build_raw_object_path("traffic", f"stations_traffic_{timestamp}.csv",timestamp))

    t_setup = task_setup()
    task_download_air(t_setup)
    task_download_meteo(t_setup)
    task_download_traffic(t_setup)
    

download_pipeline()
    
