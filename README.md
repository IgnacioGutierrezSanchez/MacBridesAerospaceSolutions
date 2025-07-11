# MacBrides Aerospace Solutions
## Proyecto de Datos II: Visualización del Conjunto de Datos

## Descripción

Este proyecto forma parte de la asignatura **Proyecto de Datos II** de la **Universidad Complutense de Madrid (Curso 2024/25)**. El objetivo principal es la visualización y el análisis de un conjunto de datos de tráfico aéreo para predecir el tiempo que los aviones permanecen en tierra antes de despegar.

## Estructura del Repositorio

- `PRIMERA ENTREGA/` - Contiene los archivos de la primera entrega del proyecto
    - `EJERCICIO 1/` - Incluye los notebooks de Jupyter para el ejercicio 1.
    - `EJERCICIO 2/` - Incluye los notebooks de Jupyter para el ejercicio 2.
    - `Preprocesamiento.ipynb` - Este cuaderno realiza el procesamiento de los datos ADS-B desde un archivo comprimido
    - `README.md` - Archivo dentro de PRIMERA ENTREGA con información sobre la misma.
- `README.md` - Este archivo, con información sobre el proyecto.

## Instalación y Requisitos

Para ejecutar el proyecto, es necesario instalar las siguientes dependencias:

Las bibliotecas utilizadas incluyen:
- **matplotlib**
- **seaborn**
- **geopandas**
- **movingpandas**
- **dask**
- **plotly.express**
- **pyModedS**
- **datetime**
- **base64**
- **Dash**


## Estructura y Contenido

### Entrega 1: Análisis Exploratorio y Preprocesamiento

- **EJERCICIO 1/A/Ejercicio_1_a.ipynb**  
  Análisis visual de tráfico aéreo y tiempos de espera. Incluye visualizaciones como histogramas, boxplots y mapas de calor para entender la distribución de tiempos y patrones horarios.

- **EJERCICIO 1/B/Ejercicio_1_b.ipynb**  
  Complementa el análisis previo con un estudio adicional de dinámicas del tráfico aéreo en puntos de espera.

- **EJERCICIO 2/Ejercicio_2.ipynb**  
  Visualización geográfica de pistas y aeronaves, mostrando su ubicación y estado (en tierra o en aire).

- **preprocesamiento.ipynb**  
  Procesa los datos ADS-B originales desde archivos comprimidos, extrayendo variables clave y segmentando la información en archivos CSV por día para facilitar el análisis posterior.

**Importante:**  
Para ejecutar los ejercicios de análisis, es necesario contar con los archivos CSV segmentados generados por el preprocesamiento, por ejemplo:  
`data_2024-12-01.csv`, `data_2024-12-02.csv`, ..., `data_2024-12-07.csv`.

---

### Entrega 2: Modelado y Predicción

- **Procesamiento y filtrado avanzado** de mensajes ADS-B, con enriquecimiento mediante datos meteorológicos y selección de vuelos en puntos de espera.
- **Análisis exploratorio** para identificar patrones de tráfico y comportamiento de tiempos de espera.
- **Transformaciones previas al modelado**, como codificación cíclica, normalización y tratamiento de outliers.
- **Entrenamiento de modelos de machine learning**, incluyendo Random Forest, Gradient Boosting, SVR y KNN.  
  El mejor resultado se obtuvo con Gradient Boosting, alcanzando un MAE aproximado de 67.6 segundos.

---

## Resultados

| Métrica  | Valor       |
|----------|-------------|
| MAE      | 67.6 s      |
| RMSE     | 104.1 s     |
| R²       | 0.166       |

**Observaciones:**  
- Sesgo hacia ciertas pistas y tipos de aeronaves.  
- Subestimación de tiempos altos debido a la escasez de datos extremos.  
- El modelo es eficaz en condiciones normales, pero tiene limitaciones en escenarios atípicos.

---

## Equipo y Reparto de Tareas

| Nombre           | Rol                        |
|------------------|----------------------------|
| Carlos Mantilla   | Preprocesamiento           |
| Héctor García    | Preprocesamiento           |
| Ignacio Gutiérrez | Preprocesamiento           |
| Diego Alonso     | Entrenamiento              |
| Telmo Aracama    | Limpieza y Entrenamiento   |
| Mario López      | Entrenamiento y Evaluación |
| Pablo Rodríguez  | Entrenamiento y Evaluación |

---

## Tecnologías y Herramientas

- pandas, numpy  
- scikit-learn  
- matplotlib, seaborn  
- joblib, multiprocessing  
- Archivos en formato `.parquet`

---

## Reproducibilidad

Para clonar y ejecutar el proyecto, sigue los siguientes pasos:

```bash
git clone https://github.com/tu_usuario/macbrides-aerospace.git
cd macbrides-aerospace
pip install -r requirements.txt
python scripts/preprocess.py
python scripts/train_model.py

