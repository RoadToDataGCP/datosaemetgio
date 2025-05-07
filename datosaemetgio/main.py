import json
import pandas as pd
from datetime import datetime
import requests as rq
import time
from google.cloud import storage


api_key="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJndWVycmVyb2FudGhvbnk5NzA3QGdtYWlsLmNvbSIsImp0aSI6IjlhOGQxMzE4LTgxZjYtNDIzZC05NTVmLWQwMWVlNDI1YzgwMCIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzQzMDc3Mzg5LCJ1c2VySWQiOiI5YThkMTMxOC04MWY2LTQyM2QtOTU1Zi1kMDFlZTQyNWM4MDAiLCJyb2xlIjoiIn0.YmW6rvOu7CwfegjjNHdiow6Dz_zwGx1ljdaoVpadelA"


def predicciones():
    df = pd.read_csv("municipios.csv")
    df['CODIGO'] = df['CODIGO'].apply(lambda x: f'{x:05d}')
    listaCod = list(df["CODIGO"])
    headers = {'cache-control': "no-cache"}
    querystring = {"api_key":api_key}
    dfMuni = pd.DataFrame()
    log = open("logFalta.txt","a")
    for codMuni in listaCod[:20]:
        url=f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/{codMuni}"
            


        # Configuración de reintentos
        max_retries = 3
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
                try:
                    response = rq.get(url,headers=headers,params=querystring)
                    response.raise_for_status()
                    data = response.json()
                    #dataframe con el link en datos y el metadatos
                    df = pd.DataFrame([data])
                    
                    response2 = rq.get(df["datos"].item())
                    response2.raise_for_status()
                    data2 = response2.json()
                    df2 = pd.DataFrame(data2)

                    dfMuni = pd.concat([dfMuni,df2])
                    dfMuni.reset_index(drop=True, inplace=True)
                    print(f"Se proceso el municipio: {codMuni}")
                    retry_count = 0
                    success = True
                    dfMuni.to_json("predicciones.json",orient="index")

                except rq.exceptions.HTTPError as http_err:
                    
                    if response.status_code == 429:
                        wait_time = 60 
                        print(f"Error 429 (Too Many Requests). Reintentando...")
                        time.sleep(wait_time)
                    elif response.status_code==404:
                        print(f"HTTP error: {http_err}. No se encontraron datos, pasando al siguiente...")
                        success=True 
                    else:
                        retry_count += 1
                        print(f"HTTP error: {http_err}. Reintento {retry_count}/{max_retries}")
                        if retry_count < max_retries:
                            time.sleep(30) 
                    
                except rq.exceptions.ConnectionError as conn_err:
                    retry_count += 1
                    print(f"Error de conexión: {conn_err}. Reintento {retry_count}/{max_retries}. Esperando 60 segundos...")
                    if retry_count < max_retries:
                        time.sleep(60)
                        
                except rq.exceptions.Timeout as timeout_err:
                    retry_count += 1
                    print(f"Timeout : {timeout_err}. Reintento {retry_count}/{max_retries}. Esperando 60 segundos...")
                    if retry_count < max_retries:
                        time.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    print(f"Ocurrio otro error: {e}. Reintento {retry_count}/{max_retries}. Esperando 30 segundos...")
                    if retry_count < max_retries:
                        time.sleep(30)
        if not success:
            print(f"No se pudo procesar el municipio {codMuni} después de {max_retries} intentos. Continuando con el siguiente municipio...")
            
            log.write(f"No se pudo procesar el municipio {codMuni} \n")
            

    print("Proceso completado.")


def tiempopre():
    with open("predicciones.json", "r", encoding="utf-8") as file:
        data = json.load(file)

    # Obtener la fecha actual en formato 'YYYY-MM-DDT00:00:00'
    fecha_actual = datetime.now().strftime("%Y-%m-%dT00:00:00")

    # Preparar la lista para el DataFrame
    predicciones = []

    for municipio in data.values():
        for dia in municipio["prediccion"]["dia"]:
            if dia["fecha"] == fecha_actual:
                for periodo in dia["probPrecipitacion"]:
                    predicciones.append({
                        "id": municipio["id"],
                        "municipio": municipio["nombre"],
                        "provincia": municipio["provincia"],
                        "periodo": periodo["periodo"],
                        "prob_precipitacion": periodo["value"],
                        "estado_cielo": next((e["descripcion"] for e in dia["estadoCielo"] if e["periodo"] == periodo["periodo"]), ""),
                        "temperatura_min_max": f"{dia['temperatura']['minima']} - {dia['temperatura']['maxima']}"
                    })

    # Convertir a DataFrame y guardar en CSV
    df = pd.DataFrame(predicciones)
    df["id"] = df["id"].apply(lambda x: f'{x:05d}')
    df.to_csv("prediccion_2.csv", index=False, encoding="utf-8")
         
    #preparacion para subir al bucket de gcloud
    bucket_name="datosaemetgio"
    destination_blob_name =f"output/{fecha_actual}/prediccion_{fecha_actual}.csv"
    #inicializa el cliente de gCloudStorage
    storage_client= storage.Client()
    #obtencion del bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    #nombre archivo
    local_file=f"prediccion_2.csv"
    blob.upload_from_filename(local_file)
    print(f"Archivo {local_file} subido a gs://{bucket_name}/{destination_blob_name}")

if __name__ =="__main__":
    predicciones()
    time.sleep(4)
    tiempopre()