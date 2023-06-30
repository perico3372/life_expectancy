#!/usr/bin/env python
# coding: utf-8

# author: Pablo Perez
# email: perico3372@gmail.com
import os
import tempfile
import zipfile
import requests
import gitlab
import base64
from pyspark.sql import SparkSession
import xml.etree.ElementTree as ET
from datetime import datetime

# Configurar la conexión al servidor de GitLab
gl = gitlab.Gitlab("https://gitlab.com", private_token="glpat-BKvQKBcwpD2pxuQURP8_")

# Nombre de usuario o grupo y nombre del repositorio en GitLab
namespace = "perico3372"
repo_name = "life_expectancy"

# ID del proyecto en GitLab
project_id = f"{namespace}/{repo_name}"

# Obtener el proyecto de GitLab
project = gl.projects.get(project_id)

# Define las URLs y nombre de la carpeta de descarga del archivo .zip "data" como una lista
base_urls = [
    ("https://api.worldbank.org/v2/en/indicator/SP.DYN.LE00.IN?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SP.DYN.LE00.MA.IN?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SP.DYN.LE00.FE.IN?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.DYN.NMRT?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SP.DYN.IMRT.IN?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.DYN.MORT?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.STA.MMRT?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.STA.MMRT?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SP.DYN.CDRT.IN?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SP.POP.GROW?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.DYN.NCOM.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.DTH.COMM.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.MED.PHYS.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.MED.BEDS.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.H2O.BASW.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/SH.STA.BASS.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/EG.ELC.ACCS.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/IT.NET.USER.ZS?downloadformat=xml", "data"),
    ("https://api.worldbank.org/v2/en/indicator/NY.GDP.PCAP.KD.ZG?downloadformat=xml", "data")
]

# Crea un bucle para iterar sobre las URLs y los nombres de las carpetas
for counter, (base_url, folder_name) in enumerate(base_urls):
    # Genera la URL actual
    url = base_url

    # Envía una solicitud GET para descargar el archivo ZIP
    response = requests.get(url)

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Crea un archivo temporal para guardar el contenido
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(response.content)

        # Extrae el contenido del archivo ZIP en memoria
        with zipfile.ZipFile(tmp_file.name) as zip_ref:
            # Encuentra los archivos XML en los archivos extraídos
            xml_files = [file for file in zip_ref.namelist() if file.endswith(".xml")]

            # Verifica si se encontraron archivos XML
            if xml_files:
                # Codifica y crea los archivos XML en el repositorio
                for i, xml_file in enumerate(xml_files):
                    # Codifica el contenido del archivo XML como base64
                    file_content = base64.b64encode(zip_ref.read(xml_file)).decode()

                    # Crea el nombre del archivo en el repositorio
                    file_name = f"data_{str(counter).zfill(2)}.xml"

                    # Crea una ruta de carpeta en el repositorio
                    folder_path = f"{folder_name}/"
                    file_path = f"{folder_path}{file_name}"

                    # Verifica si el archivo ya existe en el repositorio
                    try:
                        existing_file = project.files.get(file_path, ref="main")
                        print(f"The file {file_path} already exists in GitLab. Deleting...")
                        existing_file.delete(branch="main", commit_message="Delete file")
                    except gitlab.exceptions.GitlabGetError:
                        pass

                    # Crea el archivo en el repositorio
                    file = project.files.create(
                        {
                            "file_path": file_path,
                            "branch": "main",
                            "content": file_content,
                            "encoding": "base64",
                            "commit_message": f"Create file {file_path}",
                        }
                    )
                    print(f"The file {file_path} has been successfully created in GitLab.")
            else:
                print(f"No XML files found in the {folder_name} folder.")
    else:
        print(f"Error downloading data from {url}: {response.status_code}")


# Crea una sesión de Spark
spark = SparkSession.builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# Lista de URLs y nombres de carpetas
base_urls = [
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_00.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_01.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_02.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_03.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_04.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_05.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_06.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_07.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_08.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_09.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_10.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_11.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_12.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_13.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_14.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_15.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_16.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_17.xml", "data"),
    ("https://gitlab.com/perico3372/life_expectancy/-/raw/main/data/data_18.xml", "data")
]

# Lista para almacenar los dataframe generados
list_dataframe = []

# Recorre las URLs
for url, folder_name in base_urls:
    # Realiza la solicitud GET para obtener el contenido del archivo
    response = requests.get(url)

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Accede al contenido del archivo
        xml_content = response.text

        # Parsea el contenido del archivo XML
        tree = ET.ElementTree(ET.fromstring(xml_content))
        root = tree.getroot()

        # Obtén los datos relevantes del árbol XML y conviértelos en una lista de diccionarios
        data = []
        for record in root.findall('.//record'):
            record_data = {}
            for field in record.findall('field'):
                name = field.get('name')
                value = field.text
                record_data[name] = value
            data.append(record_data)

        # Crea un DataFrame a partir de la lista de diccionarios
        dataframe = spark.createDataFrame(data)

        # Agrega el DataFrame a la lista de list_dataframe
        list_dataframe.append(dataframe)

        # Puedes realizar operaciones y análisis de datos con el DataFrame aquí

    else:
        print(f"Error al abrir el archivo {url}: {response.status_code}")

list_dataframe[0] = list_dataframe[0].withColumnRenamed("Value", 'life_expectancy')
list_dataframe[0] = list_dataframe[0].drop("Item")

list_dataframe[1] = list_dataframe[1].withColumnRenamed("Value", 'life_expectancy_male')
list_dataframe[1] = list_dataframe[1].drop("Item")

list_dataframe[2] = list_dataframe[2].withColumnRenamed("Value", 'life_expectancy_female')
list_dataframe[2] = list_dataframe[2].drop("Item")

list_dataframe[3] = list_dataframe[3].withColumnRenamed("Value", 'Mortality_rate_neonatal_per_1,000')
list_dataframe[3] = list_dataframe[3].drop("Item")

list_dataframe[4] = list_dataframe[4].withColumnRenamed("Value", 'mort_infant_1000')
list_dataframe[4] = list_dataframe[4].drop("Item")

list_dataframe[5] = list_dataframe[5].withColumnRenamed("Value", 'mort_under_5_1000')
list_dataframe[5] = list_dataframe[5].drop("Item")

list_dataframe[6] = list_dataframe[6].withColumnRenamed("Value", 'maternal_mort_1000')
list_dataframe[6] = list_dataframe[6].drop("Item")

list_dataframe[7] = list_dataframe[7].withColumnRenamed("Value", 'xxxxxx')
list_dataframe[7] = list_dataframe[7].drop("Item")

list_dataframe[8] = list_dataframe[8].withColumnRenamed("Value", 'Death rate, crude (per 1,000 people)')
list_dataframe[8] = list_dataframe[8].drop("Item")

list_dataframe[9] = list_dataframe[9].withColumnRenamed("Value", 'Population growth (annual %)')
list_dataframe[9] = list_dataframe[9].drop("Item")

list_dataframe[10] = list_dataframe[10].withColumnRenamed("Value", 'Mortality from CVD, cancer, diabetes or CRD be...')
list_dataframe[10] = list_dataframe[10].drop("Item")

list_dataframe[11] = list_dataframe[11].withColumnRenamed("Value", 'Cause of death, by communicable diseases and m...')
list_dataframe[11] = list_dataframe[11].drop("Item")

list_dataframe[12] = list_dataframe[12].withColumnRenamed("Value", 'Physicians (per 1,000 people)')
list_dataframe[12] = list_dataframe[12].drop("Item")

list_dataframe[13] = list_dataframe[13].withColumnRenamed("Value", 'Hospital beds (per 1,000 people)')
list_dataframe[13] = list_dataframe[13].drop("Item")

list_dataframe[14] = list_dataframe[14].withColumnRenamed("Value", 'People using at least basic drinking water ser...')
list_dataframe[14] = list_dataframe[14].drop("Item")

list_dataframe[15] = list_dataframe[15].withColumnRenamed("Value", 'People using at least basic sanitation service...')
list_dataframe[15] = list_dataframe[15].drop("Item")

list_dataframe[16] = list_dataframe[16].withColumnRenamed("Value", 'Electricity (% of population)')
list_dataframe[16] = list_dataframe[16].drop("Item")

list_dataframe[17] = list_dataframe[17].withColumnRenamed("Value", 'Internet (% of population)')
list_dataframe[17] = list_dataframe[17].drop("Item")

list_dataframe[18] = list_dataframe[18].withColumnRenamed("Value", 'PBI')
list_dataframe[18] = list_dataframe[18].drop("Item")

# Inicializar el DataFrame final con el primer DataFrame en la lista
dataframe_final = list_dataframe[0]

# Realizar el join interno con el resto de los DataFrames en la lista
for i in range(1, len(list_dataframe)):
    dataframe_final = dataframe_final.join(list_dataframe[i], ["Country or Area", "Year"], "outer")

# Configurar la conexión al servidor de GitLab
#gl = gitlab.Gitlab("https://gitlab.com", private_token="glpat-BKvQKBcwpD2pxuQURP8_")

# Nombre de usuario o grupo y nombre del repositorio en GitLab
#namespace = "perico3372"
#repo_name = "life_expectancy"

# ID del proyecto en GitLab
#project_id = f"{namespace}/{repo_name}"

# Obtener el proyecto de GitLab
#project = gl.projects.get(project_id)

# Generar el nombre del archivo con formato table_fecha_hora.csv
#now = datetime.now()
#timestamp = now.strftime("%Y%m%d_%H%M%S")
#file_name = f"table_{timestamp}.csv"


# Generar el nombre del archivo
file_name = "table.txt"

# Ruta del archivo en GitLab
file_path = file_name
commit_message = "Tabla final"

# Verificar si el archivo existe en el repositorio
try:
    # Intentar obtener el archivo existente
    file = project.files.get(file_path=file_path, ref='main')

    # Actualizar el contenido del archivo
    file.content = dataframe_final.toPandas().to_csv(index=False)
    file.save(branch='main', commit_message='Actualizar archivo existente')

except gitlab.exceptions.GitlabGetError as e:
    if e.error_message == "404 File Not Found":
        # El archivo no existe, crearlo
        project.files.create({'file_path': file_path, 'branch': "main", "content": dataframe_final.toPandas().to_csv(index=False), 'commit_message': commit_message})
    else:
        raise e



