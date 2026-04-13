# Diagnóstico del sistema SniesManager

**Fecha del informe original:** 10 de febrero de 2026  
**Nota:** Este archivo es un **informe puntual** (no se actualiza en cada commit). La descripción vigente del producto, rutas y pipelines está en **`README.md`**, **`ARCHIVOS_PROYECTO.md`** y **`etl/config.py`**.

## Resumen ejecutivo

El sistema **SniesManager** combina el pipeline **SNIES / referentes EAFIT** con el **estudio de mercado** (Fases 1–6 y reportes segmentados). El diagnóstico histórico indicaba el código **funcionalmente correcto** salvo advertencias de dependencias en el entorno donde se ejecutó la herramienta de diagnóstico.

## RESULTADOS DEL DIAGNÓSTICO

### ✅ COMPONENTES FUNCIONALES

1. **Estructura de Directorios**
   - Todos los directorios requeridos existen (`app/`, `etl/`, `outputs/`, `ref/`, `models/`, `docs/`, `logs/`)

2. **Archivos Principales**
   - Todos los archivos Python principales están presentes y accesibles
   - `app/main.py` - Aplicación principal GUI
   - `etl/config.py` - Configuración centralizada
   - `etl/historicoProgramasNuevos.py` - Gestión de históricos
   - Todos los módulos ETL están presentes

3. **Imports Críticos**
   - ✅ `etl.config` - Importa correctamente
   - ✅ `etl.historicoProgramasNuevos` - Importa correctamente
   - ✅ `etl.pipeline_logger` - Importa correctamente
   - ✅ `etl.exceptions_helpers` - Importa correctamente

4. **Configuración del Archivo Histórico**
   - ✅ `ARCHIVO_HISTORICO` está configurado correctamente: `HistoricoProgramasNuevos .xlsx` (con espacio)
   - ✅ El archivo histórico existe y es accesible
   - ✅ Solo existe un archivo histórico (no hay duplicados)
   - ✅ La función de consolidación está implementada para manejar duplicados automáticamente

5. **Archivos de referencia**
   - ✅ Directorio `ref/` existe
   - Los archivos **referentesUnificados** y **catalogoOfertasEAFIT** suelen estar en **`ref/`** (p. ej. `.csv`)
   - **`ref/backup/`** agrupa insumos **locales** del estudio de mercado (matrículas, inscritos, OLE, etc.); no sustituye a los CSV/XLSX de referentes en `ref/`

6. **Sintaxis de Archivos Python**
   - ✅ Todos los archivos Python críticos tienen sintaxis correcta:
     - `etl/config.py`
     - `etl/historicoProgramasNuevos.py`
     - `etl/pipeline_logger.py`
     - `app/main.py`

### ⚠️ ADVERTENCIAS Y RECOMENDACIONES

1. **Dependencias No Instaladas**
   Algunas dependencias pueden no estar instaladas en el entorno actual:
   - `unidecode` - Necesario para normalización de texto
   - `sentence_transformers` - Necesario para embeddings semánticos
   - `sklearn` (scikit-learn) - Necesario para clasificación ML
   - `selenium` - Necesario para descarga automatizada
   - `webdriver_manager` - Necesario para gestión de drivers

   **Recomendación:** Ejecutar `pip install -r requirements.txt` para instalar todas las dependencias.

2. **Archivo Histórico**
   - El archivo histórico existe y está configurado correctamente
   - La hoja se llama "ProgramasNuevos" (configurado en `HOJA_HISTORICO`)

### Configuración de rutas (referencia)

- **ARCHIVO_HISTORICO:** `outputs/HistoricoProgramasNuevos .xlsx` (con espacio al final)
- **ARCHIVO_PROGRAMAS:** `outputs/Programas.xlsx`
- **OUTPUTS_DIR / REF_DIR / TEMP_DIR / ESTUDIO_MERCADO_DIR:** ver `etl/config.py`
- **Mercado:** salidas bajo `outputs/estudio_de_mercado/` y parquets en `outputs/temp/`

### 📋 FUNCIONALIDADES VERIFICADAS

1. ✅ **Gestión de Archivos Históricos**
   - Configuración correcta del archivo principal
   - Función de consolidación de duplicados implementada
   - Manejo automático de variaciones de nombre (con/sin espacio)

2. ✅ **Configuración Centralizada**
   - Sistema de configuración dinámico funcionando
   - Detección automática de entorno (script vs .exe)
   - Soporte para configuración personalizada vía `config.json`

3. ✅ **Estructura del Proyecto**
   - Organización modular correcta
   - Separación clara entre `app/` (GUI) y `etl/` (procesamiento)
   - Archivos de configuración y documentación presentes

## CONCLUSIÓN

El sistema está **listo para usar** después de instalar las dependencias faltantes. Todos los componentes críticos están presentes y funcionando correctamente. La configuración del archivo histórico está correcta y el sistema manejará automáticamente cualquier duplicación de archivos históricos.

### Próximos pasos recomendados

1. Instalar dependencias: `pip install -r requirements.txt`
2. Verificar el archivo histórico y `outputs/Programas.xlsx` según el uso
3. Probar el **pipeline SNIES** y, si aplica, el **estudio de mercado** desde la GUI
4. Comprobar insumos en **`ref/backup/`** para el mercado (ver `README.md`)

---

**Nota:** Este diagnóstico fue generado automáticamente. Para ejecutarlo nuevamente, ejecuta:
```bash
python diagnostico_sistema.py
```
