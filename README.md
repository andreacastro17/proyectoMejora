# Proyecto de Mejora - Clasificación de Programas Académicos

## Descripción

Este proyecto automatiza la descarga, procesamiento y clasificación de programas académicos del Sistema Nacional de Información de la Educación Superior (SNIES) para identificar programas nuevos que son referentes o competencia directa de los programas ofrecidos por la Universidad EAFIT.

El sistema utiliza técnicas de Machine Learning (embeddings semánticos y Random Forest) para clasificar automáticamente los programas nuevos y determinar si son referentes de programas EAFIT existentes.

## Características Principales

- **Descarga Automatizada**: Descarga automática de datos de programas desde el portal SNIES usando Selenium
- **Normalización de Datos**: Normalización y limpieza de textos para facilitar el procesamiento
- **Detección de Programas Nuevos**: Comparación con archivos históricos para identificar programas nuevos
- **Clasificación con ML**: Modelo de Machine Learning que identifica si un programa nuevo es referente de algún programa EAFIT
- **Exportación para Power BI**: Preparación de datos para visualización en dashboards

## Estructura del Proyecto

```
proyectoMejora/
├── app/                    # Aplicación principal y orquestador del pipeline
│   ├── __init__.py
│   └── main.py            # Pipeline principal
├── etl/                    # Scripts de extracción, transformación y carga
│   ├── descargaSNIES.py   # Descarga de datos desde SNIES
│   ├── normalizacion.py   # Normalización de columnas de texto
│   ├── procesamientoSNIES.py  # Identificación de programas nuevos
│   ├── clasificacionProgramas.py  # Modelo ML de clasificación
│   └── exportacionPowerBI.py     # Exportación para Power BI
├── dashboards/            # Dashboards y visualizaciones
├── models/                 # Modelos de ML entrenados
│   ├── clasificador_referentes.pkl
│   ├── encoder_programas_eafit.pkl
│   └── modelo_embeddings.pkl
├── outputs/                # Archivos de salida
│   ├── Programas.xlsx     # Archivo principal con programas procesados
│   └── historico/         # Respaldo de archivos históricos
├── ref/                    # Archivos de referencia
│   ├── referentesUnificados.csv  # Datos de entrenamiento
│   └── catalogoOfertasEAFIT.csv  # Catálogo de programas EAFIT
├── docs/                   # Documentación y backups
├── logs/                   # Archivos de log
└── requirements.txt        # Dependencias del proyecto
```

## Requisitos Previos

- Python 3.8 o superior
- Google Chrome instalado (para la descarga automatizada)
- Entorno virtual de Python (recomendado)

## Instalación

1. **Clonar o descargar el proyecto**

2. **Crear y activar un entorno virtual** (recomendado):
   ```bash
   python -m venv env
   
   # En Windows:
   env\Scripts\activate
   
   # En Linux/Mac:
   source env/bin/activate
   ```

3. **Instalar las dependencias**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Verificar que Chrome esté instalado** en el sistema

## Configuración

### Archivos de Referencia

Asegúrate de tener los siguientes archivos en la carpeta `ref/`:

- `referentesUnificados.csv`: Archivo con programas referentes confirmados para entrenar el modelo
- `catalogoOfertasEAFIT.csv`: Catálogo completo de programas ofrecidos por EAFIT

### Rutas de Archivos

Los scripts utilizan rutas absolutas configuradas internamente. Si necesitas cambiar las rutas, modifica las constantes en cada script:

- `etl/descargaSNIES.py`: `DOWNLOAD_DIR` y `HISTORIC_DIR`
- `etl/normalizacion.py`: `ARCHIVO_PROGRAMAS`
- `etl/procesamientoSNIES.py`: `ARCHIVO_PROGRAMAS_ACTUAL` y `DIRECTORIO_HISTORICO`

## Uso

### Ejecución Completa del Pipeline

Para ejecutar el sistema (abre el **Menú Principal**):

cls```bash
python app/main.py
```

Desde el menú puedes:

- Ejecutar el **análisis SNIES** (pipeline)
- Hacer **ajustes manuales** (gestión de falsos positivos) sobre `outputs/Programas.xlsx`
- Editar el dataset de entrenamiento y **reentrenar el modelo**
- Hacer **consolidación (merge)** entre el archivo actual y un histórico

### Flujo de uso recomendado

1. **Entrar a la app** y abrir el menú principal (una sola ventana).
2. **Ejecutar el análisis SNIES (Pipeline)**: descarga datos del SNIES, normaliza, detecta programas nuevos y, con la oferta de programas EAFIT (`ref/catalogoOfertasEAFIT`), la IA clasifica cada programa nuevo: si es **referente** o no, qué **programa EAFIT** le corresponde y la **probabilidad**.
3. **Revisar el resultado** en **“Gestión de falsos positivos / ajuste manual”**: se muestra la combinación programa SNIES + es referente (sí/no) + nombre/código programa EAFIT + probabilidad. El usuario puede **confirmar o corregir** cuando la clasificación se equivoca y guardar los cambios.
4. **Reentrenar el modelo** (opcional): a partir de las correcciones o del archivo de referentes (`ref/referentesUnificados`), se puede reentrenar el modelo para mejorar futuras clasificaciones.
5. **Consolidación (Merge)** (cuando se necesite): unir `Programas.xlsx` con un histórico para generar el Excel final.

Cuando ejecutas el análisis SNIES, se ejecutan los siguientes pasos en orden:

1. **Resguardo de históricos (condicional)**: Solo si se logra obtener una versión nueva de `Programas.xlsx`, el archivo anterior se mueve a `outputs/historico/`
2. **Descarga de Programas SNIES**: Descarga el archivo más reciente desde el portal SNIES (Selenium). Si falla, el pipeline aborta **sin modificar archivos** para evitar usar información potencialmente desactualizada.
3. **Normalización de columnas**: Normaliza y limpia los textos de las columnas principales
4. **Procesamiento de programas nuevos**: Identifica programas nuevos comparando con archivos históricos (columna `PROGRAMA_NUEVO`)
5. **Clasificación de programas nuevos**: Compara cada programa nuevo con la oferta EAFIT (catálogo) y asigna, mediante el modelo ML, si es referente, el programa EAFIT correspondiente y la probabilidad (`ES_REFERENTE`, `PROGRAMA_EAFIT_CODIGO`, `PROGRAMA_EAFIT_NOMBRE`, `PROBABILIDAD`)
6. **Normalización final**: Aplica normalización de ortografía y formato
7. **Actualización de histórico de programas nuevos**: Agrega los programas nuevos detectados a `outputs/HistoricoProgramasNuevos.xlsx`
8. **Limpieza automática de archivos históricos**: Si hay más de 20 archivos en `outputs/historico/`, los consolida en `HistoricoProgramasNuevos.xlsx` y elimina los archivos individuales para evitar que la carpeta se llene

### Comportamiento cuando falla la descarga

La etapa de descarga (`etl/descargaSNIES.py`) está diseñada para **no modificar archivos existentes si no se puede obtener una versión nueva**:

- **SNIES OK (WEB_SNIES)**:
  - Se genera una nueva versión de `outputs/Programas.xlsx`
  - El archivo anterior (si existía) se mueve a `outputs/historico/`
  - Se registra `FUENTE_DATOS = WEB_SNIES`

- **SNIES falla**:
  - **No se realizan cambios** sobre `outputs/Programas.xlsx` ni se mueven archivos a histórico
  - El pipeline se detiene y deja el error registrado en `logs/pipeline.log`

### Limpieza automática de archivos históricos

Para evitar que la carpeta `outputs/historico/` se llene de muchos archivos `.xlsx`, el sistema incluye una **limpieza automática**:

- **Automática**: Al finalizar cada ejecución del pipeline, si hay más de **20 archivos** en `outputs/historico/`, se consolidan automáticamente en `outputs/HistoricoProgramasNuevos.xlsx` y se eliminan los archivos individuales.
- **Manual**: Desde el menú principal, botón **"Limpiar archivos históricos"** en la sección de utilidades. Esto consolida todos los archivos históricos (sin umbral mínimo) y los elimina después de consolidarlos.

La consolidación:
- Lee todos los archivos `.xlsx` de `outputs/historico/`
- Extrae todos los programas de cada archivo
- Los agrega a `HistoricoProgramasNuevos.xlsx` (eliminando duplicados)
- Elimina los archivos históricos individuales consolidados

### Ejecución de Componentes Individuales

#### 1. Descargar datos de SNIES

```bash
python etl/descargaSNIES.py
```

Descarga el archivo de programas desde el portal SNIES y lo guarda en `outputs/Programas.xlsx`.

#### 2. Normalizar datos

```bash
python etl/normalizacion.py
```

Normaliza las columnas de texto del archivo `outputs/Programas.xlsx`.

#### 3. Procesar programas nuevos

```bash
python etl/procesamientoSNIES.py
```

Compara el archivo actual con el histórico y marca los programas nuevos con `PROGRAMA_NUEVO = 'Sí'`.

#### 4. Entrenar el modelo de clasificación

```bash
python etl/clasificacionProgramas.py entrenar
```

Entrena el modelo de Machine Learning usando los referentes unificados. Este paso es necesario la primera vez o cuando se quiera reentrenar el modelo.

#### 5. Clasificar programas nuevos

```bash
python etl/clasificacionProgramas.py
```

Clasifica los programas nuevos identificados en el paso 3 y determina si son referentes de programas EAFIT.

## Modelo de Machine Learning

### Descripción del Modelo

El sistema utiliza un modelo híbrido que combina:

- **Embeddings Semánticos**: Utiliza `paraphrase-multilingual-MiniLM-L12-v2` de Sentence Transformers para generar representaciones vectoriales de los nombres de programas
- **Random Forest Classifier**: Clasificador que utiliza los embeddings y características adicionales (similitud de campo amplio, nivel de formación) para predecir si un programa es referente

### Features del Modelo

El modelo utiliza las siguientes características:

1. Embeddings del nombre del programa (384 dimensiones)
2. Similitud coseno entre embeddings del programa externo y programa EAFIT
3. Similitud de campo amplio (binaria: 1 si coinciden, 0 si no)
4. Similitud de nivel de formación (binaria: 1 si coinciden, 0 si no)

### Entrenamiento

El modelo se entrena con datos de `ref/referentesUnificados.csv` donde:
- `label = 1` indica que es un referente confirmado
- Se filtran solo los casos donde los niveles de formación coinciden

### Clasificación

Para clasificar un programa nuevo:

1. Se normaliza el nivel de formación del programa nuevo
2. Se filtra el catálogo EAFIT solo a programas con el mismo nivel
3. Se generan embeddings y se calculan similitudes
4. Se evalúan los top K candidatos con el modelo completo
5. Se determina si es referente basado en un umbral de probabilidad (por defecto: 0.70; configurable en `config.json` con `umbral_referente`)

## Archivos de Salida

### Programas.xlsx

El archivo principal `outputs/Programas.xlsx` contiene:

- Todas las columnas originales del SNIES
- `PROGRAMA_NUEVO`: Indica si el programa es nuevo ('Sí' o 'No')
- `ES_REFERENTE`: Indica si el programa es referente de EAFIT ('Sí' o 'No')
- `PROBABILIDAD`: Probabilidad de que sea referente (0.0 - 1.0)
- `PROGRAMA_EAFIT_CODIGO`: Código del programa EAFIT referente (si aplica)
- `PROGRAMA_EAFIT_NOMBRE`: Nombre del programa EAFIT referente (si aplica)
- `SIMILITUD_EMBEDDING`: Similitud semántica con el programa EAFIT
- `SIMILITUD_CAMPO`: Similitud de campo amplio
- `SIMILITUD_NIVEL`: Similitud de nivel de formación

## Solución de Problemas

### Error: "No se encontró el modelo"

Si obtienes este error, necesitas entrenar el modelo primero:

```bash
python etl/clasificacionProgramas.py entrenar
```

### Error: "ChromeDriver no encontrado"

El proyecto usa `webdriver-manager` que descarga automáticamente ChromeDriver. Si hay problemas:

1. Verifica que Chrome esté instalado
2. Asegúrate de tener conexión a internet
3. Revisa los logs en `logs/pipeline.log`

### Error: "No se encontró la columna 'PROGRAMA_NUEVO'"

Este error indica que no se ha ejecutado el paso de procesamiento de programas nuevos. Ejecuta:

```bash
python etl/procesamientoSNIES.py
```

### La descarga desde SNIES falla

- Verifica tu conexión a internet
- Asegúrate de que el portal SNIES esté accesible
- Revisa que Chrome esté actualizado
- Puedes ejecutar con `HEADLESS = False` en `descargaSNIES.py` para ver qué está pasando

## Dependencias Principales

- **pandas**: Manipulación de datos
- **numpy**: Operaciones numéricas
- **sentence-transformers**: Generación de embeddings semánticos
- **scikit-learn**: Modelos de Machine Learning
- **selenium**: Automatización del navegador para descarga
- **openpyxl**: Lectura/escritura de archivos Excel
- **rapidfuzz**: Comparación de strings (fuzzy matching)

Ver `requirements.txt` para la lista completa.

## Notas Importantes

- El modelo requiere que los niveles de formación coincidan para considerar un programa como referente
- Los archivos históricos se guardan automáticamente en `outputs/historico/` con timestamp
- El proceso de descarga puede tardar varios minutos dependiendo de la velocidad de conexión
- El entrenamiento del modelo puede tardar varios minutos dependiendo del tamaño del dataset



