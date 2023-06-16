# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 12:42:05 2023

@author: Pablo Perez
"""

import pandas as pd
from sklearn.neighbors import NearestNeighbors
from sklearn.impute import SimpleImputer
import streamlit as st
from collections import Counter

# Cargar los datos en un DataFrame de pandas
data = pd.read_csv('DATA_PF_IV.csv')

# Seleccionar los indicadores como características
features = ['Birth rate, crude (per 1,000 people)',
            'Death rate, crude (per 1,000 people)',
            'Fertility rate, total (births per woman)',
            'Life expectancy at birth, female (years)',
            'Life expectancy at birth, male (years)',
            'Life expectancy at birth, total (years)',
            'Mortality rate, adult, female (per 1,000 female adults)',
            'Mortality rate, adult, male (per 1,000 male adults)',
            'Mortality rate, infant (per 1,000 live births)',
            'Mortality rate, infant, female (per 1,000 live births)',
            'Mortality rate, infant, male (per 1,000 live births)',
            'Mortality rate, neonatal (per 1,000 live births)',
            'Population growth (annual %)', 'Number of infant deaths',
            'Number of neonatal deaths', 'Net migration', 'Population, female',
            'Population, female (% of total population)', 'Population, male',
            'Population, male (% of total population)', 'Population, total',
            'Rural population', 'Rural population (% of total population)',
            'Rural population growth (annual %)', 'Urban population',
            'Urban population (% of total population)',
            'Urban population growth (annual %)',
            'Sex ratio at birth (male births per female births)',
            'Adolescent fertility rate (births per 1,000 women ages 15-19)',
            'GDP (current US$)', 'GDP growth (annual %)',
            'Immunization, DPT (% of children ages 12-23 months)',
            'Immunization, measles (% of children ages 12-23 months)',
            'Esperanza de vida saludable, total',
            'Esperanza de vida saludable, hombres',
            'Esperanza de vida saludable, mujeres']

# Crear una matriz de características
X = data[features].values

# Aplicar imputación para manejar los valores faltantes
imputer = SimpleImputer(strategy='mean')
X_imputed = imputer.fit_transform(X)

# Crear un modelo KNN y ajustarlo a los datos
model = NearestNeighbors(metric='cosine')
model.fit(X_imputed)


def get_recommendations(country_index, n_recommendations=3):
    distances, indices = model.kneighbors(X_imputed[country_index].reshape(1, -1), n_neighbors=n_recommendations+1)
    indicator_indices = indices[0][1:]
    valid_indices = [i for i in indicator_indices if i < len(features)]
    return valid_indices

# Crear una lista para almacenar todas las recomendaciones de indicadores
all_recommended_indicators = []

for country_index in range(len(data)):
    recommendations = get_recommendations(country_index)
    recommended_indicators = [features[i] for i in recommendations]
    all_recommended_indicators.extend(recommended_indicators)

# Contar la frecuencia de cada indicador recomendado
counter = Counter(all_recommended_indicators)

# Obtener los indicadores más influyentes
top_indicators = counter.most_common(7)

# Crear la aplicación Streamlit
st.title("Indicadores más influyentes para la esperanza de vida")
st.write("Los siete indicadores más influyentes para la esperanza de vida:")

for indicator, count in top_indicators:
    st.write(f"- {indicator}")
