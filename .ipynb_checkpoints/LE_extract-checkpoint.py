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
