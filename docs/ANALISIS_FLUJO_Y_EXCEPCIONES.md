# Análisis del Flujo del Sistema y Mejoras de Manejo de Excepciones

## Flujo Principal del Sistema

### 1. Pipeline Completo (`run_pipeline` en `app/main.py`)

```
1. Pre-checks (ARCHIVO_NORMALIZACION, modelos ML)
2. Descarga SNIES (Selenium)
3. Validación schema
4. Backup de Programas.xlsx
5. Normalización
6. Procesamiento programas nuevos
7. Clasificación ML (opcional)
8. Normalización final
9. Histórico programas nuevos
10. Limpieza históricos (opcional)
```

### 2. Ajuste Manual (`ManualReviewPage`)

```
1. Cargar Programas.xlsx
2. Validar schema
3. Mostrar tabla editable
4. Usuario edita → cambios en memoria
5. Guardar → escribir Excel
```

### 3. Reentrenamiento (`RetrainPage`)

```
1. Cargar referentesUnificados
2. Editar tabla
3. Guardar cambios
4. Entrenar modelo ML
```

### 4. Pipeline Estudio de Mercado (`etl/mercado_pipeline.py` + páginas en `app/main.py`)

Flujo de alto nivel (desde la GUI: Fases 1 a 6 y, aparte, reportes segmentados):

```
1. Validación de archivos de entrada (validar_archivos_entrada) — errores bloqueantes antes de Fase 2
2. Fase 1: Programas.xlsx + Referente_Categorias → base_maestra.parquet
3. Fase 2: lectura ref/backup/ + scrapers → CSV en outputs/historico/raw/
4. Fase 3: sábana consolidada → sabana_consolidada.parquet; limpieza de CSV intermedios
5. Fase 4: agregación por categoría + métricas + scoring → agregado (parquet)
6. Fase 5: Excel nacional (Estudio_Mercado_Colombia.xlsx) + hoja cambios_vs_anterior vs snapshot
7. Fase 6 (opcional): hoja eafit_vs_mercado; segmentos regionales → Excels independientes + contexto_nacional
```

**Puntos sensibles a errores (alineados con el resto del documento):**

- **Excel abierto** al escribir `Estudio_Mercado_*.xlsx` o segmentos: `PermissionError` — cerrar archivo; en hilos de fondo no usar Tkinter para diálogos (evitar cuelgues en Windows).
- **Insumos SNIES faltantes** en `ref/backup/`: no deben tumbar todo el proceso; warnings y métricas con ceros/NaN.
- **Excels SNIES con hoja ÍNDICE**: la lectura debe apuntar a la hoja de datos (ya contemplado en `scraper_matriculas.py` con detección de hoja).
- **Parquets / caché desactualizados**: si la sábana cambia y persisten `agregado_*.parquet` viejos, puede haber resultados incoherentes; la UI ofrece forzar recálculo en segmentos.

---

## Puntos Críticos de Manejo de Excepciones

### 🔴 CRÍTICO: Descarga SNIES

**Problemas identificados:**
- `ChromeDriverManager.install()` puede fallar (red, permisos, espacio en disco)
- `driver.get(SNIES_URL)` puede fallar con `TimeoutException` vs otros errores
- `driver.quit()` en `finally` puede fallar si `driver` nunca se inicializó
- No hay manejo específico de `WebDriverException` vs `TimeoutException`

**Mejoras necesarias:**
- Validar que ChromeDriverManager funcione antes de crear driver
- Manejar `TimeoutException` específicamente (reintentar o fallar rápido)
- Verificar que `driver` existe antes de `quit()`
- Capturar tipo específico de excepción para mensajes más claros

---

### 🔴 CRÍTICO: Lectura/Escritura de Excel

**Problemas identificados:**
- `pd.read_excel()` puede fallar con `PermissionError` si Excel está abierto
- `ExcelWriter` puede fallar si el archivo está abierto
- No hay validación de que el archivo sea un Excel válido antes de leer
- Archivos corruptos pueden causar errores genéricos poco informativos

**Mejoras necesarias:**
- Detectar `PermissionError` específicamente y sugerir cerrar Excel
- Validar formato del archivo antes de leer (extensión, headers mínimos)
- Manejar `BadZipFile` si el Excel está corrupto
- Retry con backoff si el archivo está temporalmente bloqueado

---

### 🟡 MEDIO: Clasificación ML

**Problemas identificados:**
- `cargar_modelos()` puede fallar si los archivos están corruptos (`pickle.UnpicklingError`)
- `modelo_embeddings.encode()` puede fallar con OOM si hay muchos programas
- No hay manejo específico de errores de memoria
- Si falla la carga del catálogo EAFIT, el error es genérico

**Mejoras necesarias:**
- Validar integridad de modelos antes de usar (checksum o validación básica)
- Manejar `MemoryError` específicamente y sugerir procesar en lotes
- Validar que el catálogo EAFIT tenga las columnas requeridas antes de procesar
- Manejar `pickle.UnpicklingError` con mensaje claro

---

### 🟡 MEDIO: Procesamiento de Programas Nuevos

**Problemas identificados:**
- Si el histórico está corrupto, el error es genérico
- No hay validación de que el histórico tenga el schema esperado
- La comparación con `rapidfuzz` puede fallar silenciosamente si hay datos inválidos

**Mejoras necesarias:**
- Validar schema del histórico antes de comparar
- Manejar errores de `rapidfuzz` específicamente
- Validar que los códigos SNIES sean válidos antes de comparar

---

### 🟢 BAJO: Normalización

**Problemas identificados:**
- `unidecode()` puede fallar con caracteres especiales raros
- No hay manejo de errores en la normalización vectorizada

**Mejoras necesarias:**
- Try/except alrededor de `unidecode()` con fallback
- Validar que las columnas existan antes de normalizar

---

## Mejoras Propuestas por Prioridad

### Prioridad ALTA

1. **Manejo robusto de archivos abiertos**
   - Detectar `PermissionError` específicamente
   - Mensaje claro: "Cierra Excel y vuelve a intentar"
   - Retry con backoff (3 intentos, 2 segundos entre intentos)

2. **Validación de integridad de archivos**
   - Validar que Excel no esté corrupto antes de leer
   - Validar schema mínimo antes de procesar
   - Checksum básico de modelos ML

3. **Manejo específico de errores de Selenium**
   - `TimeoutException` → mensaje claro + opción de reintentar
   - `WebDriverException` → verificar Chrome/ChromeDriver
   - `ChromeDriverManager` falla → mensaje con instrucciones

### Prioridad MEDIA

4. **Manejo de errores de memoria**
   - Detectar `MemoryError` en clasificación
   - Procesar en lotes si es necesario
   - Mensaje claro al usuario

5. **Validación de datos antes de procesar**
   - Validar que catálogo EAFIT tenga columnas requeridas
   - Validar que histórico tenga schema esperado
   - Validar que modelos sean compatibles con el catálogo

6. **Mejores mensajes de error**
   - Contexto específico (qué archivo, qué operación)
   - Sugerencias de solución
   - Logs detallados para debugging

### Prioridad BAJA

7. **Fallbacks para operaciones no críticas**
   - Si falla normalización de una columna, continuar con las demás
   - Si falla histórico, marcar todos como nuevos
   - Si falla limpieza de históricos, continuar

8. **Validación de configuración**
   - Verificar que todas las rutas sean válidas al inicio
   - Verificar permisos de escritura antes de ejecutar
   - Validar que Chrome esté instalado antes de descargar

---

## Implementación

Se implementaron las mejoras de Prioridad ALTA y MEDIA de forma conservadora, manteniendo compatibilidad hacia atrás.

### ✅ Mejoras Implementadas

#### 1. Módulo `etl/exceptions_helpers.py` (NUEVO)

Funciones auxiliares para manejo robusto de excepciones:

- **`leer_excel_con_reintentos()`**: Lee Excel con manejo de `PermissionError`, validación de integridad, y reintentos automáticos
- **`escribir_excel_con_reintentos()`**: Escribe Excel con manejo de `PermissionError` y reintentos
- **`validar_excel_basico()`**: Valida que un archivo sea un Excel válido sin leerlo completamente
- **`explicar_error_archivo_abierto()`**: Genera mensajes claros cuando un archivo está abierto

**Características:**
- Reintentos automáticos (3 intentos, 2 segundos entre intentos)
- Validación de integridad con `openpyxl` antes de leer
- Manejo específico de `BadZipFile` y `InvalidFileException`
- Mensajes de error claros con instrucciones de solución

#### 2. `etl/normalizacion.py`

**Mejoras:**
- ✅ Usa `leer_excel_con_reintentos()` para lectura robusta
- ✅ Usa `escribir_excel_con_reintentos()` para escritura robusta
- ✅ Valida integridad del Excel antes de leer
- ✅ Manejo específico de `PermissionError` con mensajes claros
- ✅ Try/except alrededor de normalización de columnas individuales (continúa si una falla)

#### 3. `etl/procesamientoSNIES.py`

**Mejoras:**
- ✅ Usa `leer_excel_con_reintentos()` para archivo actual e histórico
- ✅ Usa `escribir_excel_con_reintentos()` para todas las escrituras
- ✅ Valida integridad del archivo actual antes de leer
- ✅ Valida integridad del histórico antes de leer
- ✅ Fallback robusto: si el histórico es inválido o está bloqueado, marca todos como nuevos con código específico (`HISTORICO_INVALIDO`, `HISTORICO_BLOQUEADO`)
- ✅ Manejo específico de `PermissionError` con mensajes claros

#### 4. `etl/descargaSNIES.py`

**Mejoras:**
- ✅ Manejo específico de `TimeoutException` con mensaje claro y sugerencias
- ✅ Manejo específico de `WebDriverException` con instrucciones de solución
- ✅ Validación de inicialización de ChromeDriver antes de usar
- ✅ Verificación de que `driver` existe antes de `quit()` en `finally`
- ✅ Mensajes de error contextualizados con posibles causas y soluciones

**Mensajes mejorados:**
- Timeout: explica posibles causas (red lenta, sitio no disponible) y sugiere verificaciones
- WebDriverException: verifica Chrome instalado, conexión a internet, permisos

#### 5. `etl/clasificacionProgramas.py`

**Mejoras:**
- ✅ Validación de existencia de todos los archivos de modelos antes de cargar
- ✅ Manejo específico de `pickle.UnpicklingError` con mensaje claro (archivo corrupto)
- ✅ Manejo específico de `MemoryError` en carga de modelos y en `encode()` de embeddings
- ✅ Validación de columnas requeridas en catálogo EAFIT antes de procesar
- ✅ Manejo de errores de lectura del catálogo con contexto

**Mensajes mejorados:**
- Modelos corruptos: sugiere reentrenar
- MemoryError: sugiere cerrar otras aplicaciones
- Catálogo inválido: lista columnas requeridas vs encontradas

### 📊 Resumen de Cobertura

| Módulo | PermissionError | Validación Integridad | Reintentos | Mensajes Claros | Errores Específicos |
|--------|----------------|----------------------|------------|-----------------|-------------------|
| `normalizacion.py` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `procesamientoSNIES.py` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `descargaSNIES.py` | N/A | N/A | N/A | ✅ | ✅ (Timeout, WebDriver) |
| `clasificacionProgramas.py` | N/A | ✅ (modelos) | N/A | ✅ | ✅ (Pickle, Memory) |

### 🔄 Compatibilidad

Todas las mejoras son **backward-compatible**:
- Las funciones auxiliares son nuevas y no afectan código existente
- Los cambios en módulos existentes solo agregan manejo de errores, no cambian la API pública
- Los mensajes de error mejorados son más informativos pero no rompen flujos existentes

### 📝 Próximos Pasos (Opcional)

Mejoras adicionales que se pueden implementar en el futuro:

1. **Logging estructurado**: Usar formato JSON para logs más fáciles de analizar
2. **Métricas de errores**: Contar y reportar tipos de errores más comunes
3. **Notificaciones**: Alertar al usuario cuando hay errores recuperables (archivo abierto)
4. **Tests de excepciones**: Agregar tests unitarios para validar manejo de errores
