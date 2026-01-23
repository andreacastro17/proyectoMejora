# Archivos Utilizados en el Proyecto

Este documento lista todos los archivos (CSV, XLSX, PKL, TXT, etc.) que se utilizan en la ejecución del proyecto, organizados por categoría y función.

---

## 📁 ARCHIVOS DE ENTRADA (INPUTS)

### Archivos de Referencia (`ref/`)

#### 1. **referentesUnificados.xlsx**
- **Ubicación**: `ref/referentesUnificados.xlsx`
- **Uso**: Archivo principal de entrenamiento del modelo de clasificación
- **Contenido**: Pares de programas (externos y EAFIT) con label=1 (referentes confirmados)
- **Columnas clave**:
  - `NOMBRE_DEL_PROGRAMA`: Nombre del programa externo
  - `NombrePrograma EAFIT`: Nombre del programa EAFIT correspondiente
  - `CAMPO_AMPLIO`: Campo amplio del programa externo
  - `CAMPO_AMPLIO_EAFIT`: Campo amplio del programa EAFIT
  - `NIVEL_DE_FORMACIÓN`: Nivel de formación del programa externo
  - `NIVEL_DE_FORMACIÓN EAFIT`: Nivel de formación del programa EAFIT
  - `label`: Etiqueta (1 = referente confirmado)
- **Utilizado en**:
  - `etl/clasificacionProgramas.py` (función `cargar_referentes()`)
  - `etl/calibracionUmbrales.py` (función `cargar_referentes()`)

#### 2. **catalogoOfertasEAFIT.xlsx**
- **Ubicación**: `ref/catalogoOfertasEAFIT.xlsx`
- **Uso**: Catálogo de programas ofrecidos por EAFIT para comparación
- **Contenido**: Lista completa de programas EAFIT con sus características
- **Columnas clave**:
  - `Codigo EAFIT`: Código único del programa EAFIT
  - `Nombre Programa EAFIT`: Nombre del programa
  - `CAMPO_AMPLIO`: Campo amplio del programa
  - `NIVEL_DE_FORMACIÓN` o `Nivel Programas`: Nivel de formación
- **Utilizado en**:
  - `etl/clasificacionProgramas.py` (función `cargar_catalogo_eafit()`)

#### 5. **posParesPositivos.csv**
- **Ubicación**: `ref/posParesPositivos.csv`
- **Uso**: Posibles pares positivos de posgrado (referencia)

#### 6. **preParesPositivos.csv**
- **Ubicación**: `ref/preParesPositivos.csv`
- **Uso**: Posibles pares positivos de pregrado (referencia)

---

## 📥 ARCHIVOS DE SALIDA (OUTPUTS)

### Archivos Principales

#### 1. **Programas.xlsx**
- **Ubicación**: `outputs/Programas.xlsx`
- **Uso**: Archivo principal de salida con todos los programas procesados
- **Hoja**: `Programas`
- **Proceso**:
  1. Se descarga desde SNIES (web scraping)
  2. Se normaliza (columnas de texto)
  3. Se marca `PROGRAMA_NUEVO` (Sí/No)
  4. Se clasifica y agrega columnas:
     - `ES_REFERENTE`: Sí/No
     - `PROBABILIDAD`: Probabilidad de ser referente
     - `PROGRAMA_EAFIT_CODIGO`: Código del programa EAFIT asignado
     - `PROGRAMA_EAFIT_NOMBRE`: Nombre del programa EAFIT asignado
     - `SIMILITUD_EMBEDDING`: Similitud de embeddings
     - `SIMILITUD_CAMPO`: Similitud de campo amplio
     - `SIMILITUD_NIVEL`: Similitud de nivel de formación
- **Utilizado en**:
  - `etl/descargaSNIES.py` (descarga y renombrado)
  - `etl/normalizacion.py` (normalización de columnas)
  - `etl/procesamientoSNIES.py` (marcado de programas nuevos)
  - `etl/clasificacionProgramas.py` (clasificación de programas nuevos)

#### 2. **Programas_YYYYMMDD_HHMMSS.xlsx** (Históricos)
- **Ubicación**: `outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx`
- **Uso**: Versiones históricas del archivo Programas.xlsx
- **Proceso**: Se crean automáticamente cuando se descarga un nuevo archivo
- **Ejemplos**:
  - `Programas_20251112_153924.xlsx`
  - `Programas_20251112_154101.xlsx`
  - `Programas_20251216_135106.xlsx`
- **Utilizado en**:
  - `etl/descargaSNIES.py` (función `_mover_archivo_existente()`)
  - `etl/procesamientoSNIES.py` (función `obtener_ultimo_archivo_historico()`)

#### 3. **calibracion_embeddings.csv**
- **Ubicación**: `outputs/calibracion_embeddings.csv`
- **Uso**: Resultados de calibración de umbrales con similitudes calculadas
- **Contenido**: Referentes con similitudes coseno y clasificación por umbral
- **Columnas adicionales**:
  - `SIMILITUD_COSENO`: Similitud coseno calculada
  - `NIVEL_AFINIDAD_CALIBRADO`: Clasificación (ALTO/MEDIO/BAJO/MUY BAJO)
- **Generado por**: `etl/calibracionUmbrales.py`

#### 4. **calibracion_resumen.txt**
- **Ubicación**: `outputs/calibracion_resumen.txt`
- **Uso**: Resumen en texto de la calibración de umbrales
- **Contenido**: Estadísticas, percentiles y umbrales sugeridos
- **Generado por**: `etl/calibracionUmbrales.py`

#### 5. **error_screenshot.png** (temporal)
- **Ubicación**: `outputs/error_screenshot.png`
- **Uso**: Captura de pantalla cuando hay errores en la descarga
- **Generado por**: `etl/descargaSNIES.py` (en caso de error)

---

## 🤖 ARCHIVOS DE MODELOS (MODELS)

### Modelos Entrenados (`models/`)

#### 1. **clasificador_referentes.pkl**
- **Ubicación**: `models/clasificador_referentes.pkl`
- **Uso**: Modelo RandomForest entrenado para clasificar programas
- **Contenido**: Modelo serializado con pickle
- **Generado por**: `etl/clasificacionProgramas.py` (función `guardar_modelos()`)
- **Cargado por**: `etl/clasificacionProgramas.py` (función `cargar_modelos()`)

#### 2. **modelo_embeddings.pkl**
- **Ubicación**: `models/modelo_embeddings.pkl`
- **Uso**: Modelo de embeddings (SentenceTransformer) serializado
- **Contenido**: Modelo `paraphrase-multilingual-MiniLM-L12-v2` serializado
- **Generado por**: `etl/clasificacionProgramas.py` (función `guardar_modelos()`)
- **Cargado por**: `etl/clasificacionProgramas.py` (función `cargar_modelos()`)

#### 3. **encoder_programas_eafit.pkl**
- **Ubicación**: `models/encoder_programas_eafit.pkl`
- **Uso**: LabelEncoder para mapear nombres de programas EAFIT a labels numéricos
- **Contenido**: Encoder serializado con pickle
- **Generado por**: `etl/clasificacionProgramas.py` (función `guardar_modelos()`)
- **Cargado por**: `etl/clasificacionProgramas.py` (función `cargar_modelos()`)

---

## 📝 ARCHIVOS DE LOGS

#### 1. **pipeline.log**
- **Ubicación**: `logs/pipeline.log`
- **Uso**: Registro de todas las operaciones del pipeline
- **Contenido**: Logs de inicio, etapas, errores, resultados
- **Generado por**: `etl/pipeline_logger.py`

---

## 🔄 FLUJO DE ARCHIVOS EN EL PIPELINE

### Orden de Ejecución:

1. **Descarga** (`etl/descargaSNIES.py`):
   - Lee: Ninguno (descarga desde web)
   - Escribe: `outputs/Programas.xlsx`
   - Mueve: `outputs/Programas.xlsx` → `outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx` (si existe)

2. **Normalización** (`etl/normalizacion.py`):
   - Lee: `outputs/Programas.xlsx`
   - Escribe: `outputs/Programas.xlsx` (actualizado)

3. **Procesamiento** (`etl/procesamientoSNIES.py`):
   - Lee: 
     - `outputs/Programas.xlsx` (actual)
     - `outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx` (último histórico)
   - Escribe: `outputs/Programas.xlsx` (con columna `PROGRAMA_NUEVO`)

4. **Clasificación** (`etl/clasificacionProgramas.py`):
   - Lee:
     - `ref/referentesUnificados.xlsx` (entrenamiento, solo si se entrena)
     - `ref/catalogoOfertasEAFIT.xlsx` (catálogo EAFIT)
     - `outputs/Programas.xlsx` (programas a clasificar)
     - `models/clasificador_referentes.pkl` (modelo entrenado)
     - `models/modelo_embeddings.pkl` (modelo embeddings)
     - `models/encoder_programas_eafit.pkl` (encoder)
   - Escribe: 
     - `outputs/Programas.xlsx` (con columnas de clasificación)
     - `models/*.pkl` (solo si se ejecuta entrenamiento)

5. **Calibración** (`etl/calibracionUmbrales.py`) - Opcional:
   - Lee: `ref/referentesUnificados.xlsx`
   - Escribe:
     - `outputs/calibracion_embeddings.csv`
     - `outputs/calibracion_resumen.txt`

---

## 📊 RESUMEN POR TIPO DE ARCHIVO

### Archivos CSV:
- `ref/posParesPositivos.csv` (referencia)
- `ref/preParesPositivos.csv` (referencia)
- `outputs/calibracion_embeddings.csv` (salida)

### Archivos XLSX:
- `ref/catalogoOfertasEAFIT.xlsx` (entrada)
- `ref/referentesUnificados.xlsx` (entrada)
- `outputs/Programas.xlsx` (principal, entrada/salida)
- `outputs/historico/Programas_*.xlsx` (históricos)

### Archivos PKL (Pickle):
- `models/clasificador_referentes.pkl` (modelo)
- `models/modelo_embeddings.pkl` (modelo embeddings)
- `models/encoder_programas_eafit.pkl` (encoder)

### Archivos TXT:
- `outputs/calibracion_resumen.txt` (salida)
- `logs/pipeline.log` (logs)

### Archivos PNG:
- `outputs/error_screenshot.png` (temporal, solo en errores)

---

## ⚠️ NOTAS IMPORTANTES

1. **Archivos Requeridos para Ejecución Normal**:
   - `ref/referentesUnificados.xlsx` (para entrenamiento inicial)
   - `ref/catalogoOfertasEAFIT.xlsx` (siempre requerido)
   - `outputs/Programas.xlsx` (generado por descarga)
   - `models/*.pkl` (requeridos para clasificación, excepto en primer entrenamiento)

2. **Archivos Opcionales**:
   - `outputs/historico/*.xlsx` (necesario para detectar programas nuevos)
   - `outputs/calibracion_embeddings.csv` (solo si se ejecuta calibración)

3. **Archivos Generados Automáticamente**:
   - Todos los archivos en `outputs/` (excepto si se crean manualmente)
   - Todos los archivos en `models/` (generados durante entrenamiento)
   - Archivos en `outputs/historico/` (generados automáticamente)

4. **Rutas Hardcodeadas**:
   - Algunas rutas están hardcodeadas en los archivos (ej: `C:\Users\andre\OneDrive...`)
   - Se recomienda usar rutas relativas o variables de entorno para portabilidad

---

## 🔍 BÚSQUEDA RÁPIDA

### Para encontrar dónde se usa un archivo específico:
- **referentesUnificados.xlsx**: `grep -r "referentesUnificados" etl/`
- **catalogoOfertasEAFIT.xlsx**: `grep -r "catalogoOfertasEAFIT" etl/`
- **Programas.xlsx**: `grep -r "Programas.xlsx" etl/`
- **clasificador_referentes.pkl**: `grep -r "clasificador_referentes" etl/`

