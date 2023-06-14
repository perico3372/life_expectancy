#!/usr/bin/env python
# coding: utf-8

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

def main():
    # Crear una sesión de Spark
    spark = SparkSession.builder.appName("LifeExpectancyPrediction").getOrCreate()

    # Crear un DataFrame de Spark con tus datos
    df = spark.createDataFrame([(70, 10, 5, 0.1),
                                (75, 8, 4, 0.08),
                                (80, 6, 3, 0.06),
                                (85, 5, 2, 0.04),
                                (90, 4, 1, 0.02)],
                               ["EsperanzaVida", "MortalidadInfantil", "MortalidadNeonatal", "MortalidadMaterna"])

    # Crear un ensamblador de características
    assembler = VectorAssembler(inputCols=["EsperanzaVida", "MortalidadInfantil", "MortalidadNeonatal", "MortalidadMaterna"],
                                outputCol="features")

    # Transformar los datos utilizando el ensamblador de características
    data = assembler.transform(df)

    # Entrenar el modelo de k-means
    kmeans = KMeans(k=2, seed=0)
    model = kmeans.fit(data)

    # Obtener los centros de los clusters
    centers = model.clusterCenters()

    # Preparar los datos para hacer la predicción para el año 2030
    new_data = spark.createDataFrame([(0, 3, 1, 0.01)], ["EsperanzaVida", "MortalidadInfantil", "MortalidadNeonatal", "MortalidadMaterna"])
    new_data = assembler.transform(new_data)

    # Realizar la predicción para el año 2030
    prediction = model.transform(new_data)

    # Imprimir los resultados en Streamlit
    for i, center in enumerate(centers):
        st.write(f"Cluster {i+1} - Centroid: {center}")
    st.write(f"Predicción para el año 2030: {prediction.select('prediction').collect()[0][0]}")

# Ejecutar la función principal
if __name__ == "__main__":
    main()



