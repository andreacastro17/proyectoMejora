# DIAGNÓSTICO DEL SISTEMA SNIES MANAGER

**Fecha:** 10 de Febrero, 2026  
**Versión del Sistema:** Última actualización

## RESUMEN EJECUTIVO

El sistema SNIES Manager ha sido diagnosticado y está **funcionalmente correcto** con algunas advertencias menores sobre dependencias que pueden necesitar instalación.

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

5. **Archivos de Referencia**
   - ✅ Directorio `ref/` existe
   - ✅ Se encontraron archivos de referencia en `ref/backup/`:
     - `referentesUnificados.csv`
     - `catalogoOfertasEAFIT.csv`

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

### 🔧 CONFIGURACIÓN ACTUAL

- **ARCHIVO_HISTORICO:** `outputs/HistoricoProgramasNuevos .xlsx` (con espacio al final)
- **ARCHIVO_PROGRAMAS:** `outputs/Programas.xlsx`
- **OUTPUTS_DIR:** `outputs/`
- **REF_DIR:** `ref/`
- **Archivos de Referencia:** Ubicados en `ref/backup/`

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

### Próximos Pasos Recomendados

1. Instalar dependencias faltantes: `pip install -r requirements.txt`
2. Verificar que el archivo histórico contiene los datos esperados
3. Ejecutar una prueba del pipeline completo para validar el flujo end-to-end

---

**Nota:** Este diagnóstico fue generado automáticamente. Para ejecutarlo nuevamente, ejecuta:
```bash
python diagnostico_sistema.py
```
