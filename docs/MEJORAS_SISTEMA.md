# Mejoras para hacer el sistema más efectivo y eficiente

Este documento resume un análisis del sistema actual (app, ETL, configuración, logs y pruebas) y propone mejoras concretas priorizadas por impacto y esfuerzo.

---

## 1. Eficiencia del pipeline (rendimiento)

### 1.1 Clasificación ML: evitar embeddings repetidos

**Problema:** En `clasificacionProgramas.py`, `clasificar_programas_nuevos()` llama a `clasificar_programa_nuevo()` por cada programa nuevo. Dentro de cada llamada se hace:

- `modelo_embeddings.encode([nombre_norm])` para el programa nuevo (correcto).
- `modelo_embeddings.encode(nombres_eafit, ...)` para **todos** los programas EAFIT del mismo nivel.

Los programas EAFIT son los mismos para todos los programas nuevos del mismo nivel, pero se vuelven a calcular sus embeddings en cada iteración.

**Mejora:** Precalcular una sola vez los embeddings del catálogo EAFIT (por nivel o global y luego filtrar por nivel) antes del `for idx, row in df_nuevos.iterrows()`. Pasar esos embeddings ya calculados a `clasificar_programa_nuevo()` (o a una versión que los reciba) para reutilizarlos.

**Impacto:** Alto. Reduce drásticamente el tiempo de clasificación cuando hay muchos programas nuevos.

---

### 1.2 Menos lecturas/escrituras de Programas.xlsx

**Problema:** El pipeline hace varias etapas que cada una:

1. Lee `Programas.xlsx`.
2. Transforma en memoria.
3. Escribe de nuevo `Programas.xlsx`.

Flujo actual: **Normalización** (lee + escribe) → **Procesamiento programas nuevos** (lee + escribe) → **Clasificación** (lee + escribe) → **Normalización final** (lee + escribe). Son al menos 4 lecturas y 4 escrituras del mismo archivo.

**Mejora (opción A – rápida):** Mantener el flujo por etapas pero usar un archivo temporal intermedio (por ejemplo `Programas_staging.xlsx`) y solo copiar a `Programas.xlsx` al final del pipeline. Así se evita que un fallo a mitad deje el archivo principal a medias y se reduce riesgo de bloqueos con Excel abierto.

**Mejora (opción B – más trabajo):** Refactorizar para que cada etapa reciba y devuelva un `DataFrame`, y que una sola función orquestadora lea una vez, pase el DF por normalización → procesamiento → clasificación → normalización final, y escriba una sola vez al final. Requiere cambiar firmas de `normalizar_programas()`, `procesar_programas_nuevos()`, `clasificar_programas_nuevos()`, `aplicar_normalizacion_final()` para aceptar/retornar DF opcional.

**Impacto:** Opción A: robustez y menor conflicto con archivos abiertos. Opción B: menos I/O y pipeline más claro.

---

### 1.3 Normalización: operaciones vectorizadas

**Problema:** En `normalizacion.py`, `limpiar_texto` se aplica con `.apply()` columna por columna, lo que es más lento que operaciones vectorizadas de pandas.

**Mejora:** Sustituir donde sea posible por operaciones sobre la serie completa (por ejemplo `df[col].astype(str).str.lower()`, uso de `unidecode` por vectores o con `.map()` sobre una columna ya convertida a string). Mantener `unidecode` para la parte de tildes; si hace falta, aplicar por bloques o con una columna temporal vectorizada.

**Impacto:** Medio. Acelera la etapa de normalización en archivos grandes.

---

## 2. Configuración y mantenibilidad

### 2.1 Caché de configuración

**Problema:** En `config.py`, `_load_config()` se invoca desde varios sitios (`get_base_dir()`, `set_base_dir()`, `_get_path()`, etc.). Cada llamada lee de nuevo el disco. Al inicio también se hace `_refresh_base_path()` y `_CONFIG = _load_config()`; en `update_paths_for_base_dir()` se vuelve a cargar implícitamente.

**Mejora:** Centralizar la lectura en una sola función que devuelve un diccionario en memoria y, si el archivo no ha cambiado (por ejemplo comparando `mtime` del `config.json`), devolver la versión cacheada. Invalidar la caché cuando se llame a `set_base_dir()` o cuando se guarde `config.json`.

**Impacto:** Bajo en tiempo, pero código más claro y menos I/O redundante.

---

### 2.2 Un solo punto de definición de rutas

**Problema:** Algunos módulos siguen usando alias o constantes propias (por ejemplo `ARCHIVO_PROGRAMAS_ACTUAL`, `DIRECTORIO_HISTORICO` en `procesamientoSNIES.py`) que replican valores de `config`. No hay inconsistencia hoy, pero cualquier cambio de nombres en `config` podría olvidarse en un módulo.

**Mejora:** Usar siempre las constantes de `etl.config` (por ejemplo `ARCHIVO_PROGRAMAS`, `HISTORIC_DIR`) en todos los ETL y eliminar alias locales. Si hace falta, exponer en `config` una única función tipo `get_paths()` que devuelva un dataclass o dict con todas las rutas usadas por el pipeline.

**Impacto:** Mantenibilidad y menos riesgo de rutas desincronizadas.

---

## 3. Descarga SNIES (robustez y UX)

### 3.1 Timeouts y reintentos configurables

**Problema:** `MAX_WAIT_DOWNLOAD_SEC` y los timeouts de Selenium están fijos o solo parcialmente configurables. Una red lenta puede hacer que falle sin opción de reintento.

**Mejora:** Añadir en `config.json` (o en variables de entorno) opciones como:

- `max_wait_download_sec`
- `selenium_page_load_timeout_sec`
- `download_retries` (número de reintentos antes de fallar)

En `descargaSNIES.py`, usar estos valores y un bucle de reintentos (por ejemplo 2–3 intentos con backoff breve) antes de devolver error.

**Impacto:** Mayor robustez frente a fallos puntuales de red o de la página.

---

### 3.2 Cierre explícito del driver

**Problema:** Si ocurre una excepción antes de que se llame a `_commit_programas`, el `finally` cierra el driver correctamente, pero conviene asegurar que no queden procesos Chrome huérfanos en cualquier salida.

**Mejora:** Usar un context manager o un `try/except/finally` muy explícito que garantice `driver.quit()` (y si se usa, `driver.close()`) en todos los caminos. Opcionalmente registrar en log cuando se cierra el driver por excepción.

**Impacto:** Evitar procesos Chrome residuales y logs más claros.

---

## 4. Logging y diagnóstico

### 4.1 Niveles de log y rotación

**Problema:** `pipeline_logger` escribe siempre en el mismo archivo (`pipeline.log`) sin niveles (INFO/WARNING/ERROR) ni rotación. Con el tiempo el archivo crece y es difícil filtrar solo errores.

**Mejora:**

- Añadir un nivel mínimo configurable (por ejemplo en `config.json`: `log_level`: `"INFO"` o `"DEBUG"`).
- Incluir el nivel en cada línea, por ejemplo `[INFO]`, `[WARN]`, `[ERROR]`.
- Opcional: rotación por tamaño o por fecha (por ejemplo con `logging.handlers.RotatingFileHandler` o `TimedRotatingFileHandler`) para no borrar logs antiguos de un solo golpe.

**Impacto:** Mejor diagnóstico en producción y menos riesgo de llenar disco.

---

### 4.2 Duración por etapa

**Problema:** Se registra inicio y fin del pipeline y mensajes por etapa, pero no siempre se registra la duración de cada etapa en el log.

**Mejora:** En `run_pipeline()` (y en las funciones de etapa que llamen a `log_etapa_*`), registrar al final de cada etapa algo como: `log_etapa_completada("Normalización", f"duracion=12.3s")`. Opcionalmente guardar en una estructura y al final escribir un resumen (por ejemplo en `log_fin`) con duración total y por etapa.

**Impacto:** Facilita identificar cuellos de botella (descarga, normalización, clasificación, etc.).

---

## 5. Aplicación GUI

### 5.1 Feedback durante clasificación

**Problema:** La clasificación puede tardar mucho; el usuario solo ve “Clasificando programas nuevos...” sin progreso detallado.

**Mejora:** Pasar un callback de progreso desde la GUI a `clasificar_programas_nuevos()` (o a la función que itera sobre programas nuevos) para reportar “Programa X de Y” cada N programas. La GUI puede actualizar una barra de progreso o un label. Requiere añadir un parámetro opcional `progress_callback(current, total, nombre_programa)` en la función de clasificación.

**Impacto:** Mejor experiencia de uso en ejecuciones largas.

---

### 5.2 Validación de schema antes de ejecutar

**Problema:** La validación de columnas mínimas de `Programas.xlsx` ocurre después de la descarga. Si el usuario ya tiene un `Programas.xlsx` previo y ejecuta solo “clasificación” o “normalización” desde el menú, no hay una validación temprana del schema.

**Mejora:** En los puntos de entrada que lean `Programas.xlsx` (pipeline completo, ajuste manual, reentrenamiento, merge), llamar al mismo validador (`validate_programas_schema` o equivalente) al inicio y mostrar un mensaje claro si faltan columnas o el formato no es el esperado, antes de hacer trabajo costoso.

**Impacto:** Fallos más rápidos y mensajes más claros.

---

## 6. Calidad de código y pruebas

### 6.1 Tests de integración del pipeline

**Problema:** Hay tests unitarios y de componentes; no está claro si existe un test de integración que ejecute el flujo completo (o un subflujo sin Selenium) sobre datos fixture y compruebe que el Excel de salida tiene las columnas y tipos esperados.

**Mejora:** Añadir un test (por ejemplo `test_pipeline_integration.py`) que, con un `Programas.xlsx` y referentes de prueba en una carpeta temporal:

- Ejecute normalización → procesamiento → clasificación (y si aplica, normalización final) sin descarga.
- Verifique que el archivo resultante tiene columnas como `PROGRAMA_NUEVO`, `ES_REFERENTE`, `PROGRAMA_EAFIT_CODIGO`, etc., y que no hay excepciones.

Así se detectan roturas cuando se cambien firmas o rutas.

**Impacto:** Mayor confianza al refactorizar (por ejemplo para la opción B de 1.2).

---

### 6.2 Dependencias con versiones fijas

**Problema:** `requirements.txt` mezcla paquetes con versión exacta (por ejemplo `attrs==25.3.0`) y otros sin versión (por ejemplo `pandas`, `numpy`). Esto puede provocar builds no reproducibles o incompatibilidades futuras.

**Mejora:** Fijar versiones en todos los paquetes que use el proyecto (por ejemplo con `pip freeze` o `pip-compile` si usan pip-tools). Mantener un `requirements.txt` (o `requirements.in` + generado) con versiones fijas para desarrollo y despliegue.

**Impacto:** Reproducibilidad y menos sorpresas al desplegar o al instalar en otra máquina.

---

## 7. Resumen de priorización

| Prioridad | Mejora                               | Impacto   | Esfuerzo |
|----------|--------------------------------------|-----------|----------|
| Alta     | 1.1 Precalcular embeddings catálogo  | Alto      | Medio    |
| Alta     | 4.2 Duración por etapa en logs       | Medio     | Bajo     |
| Media    | 1.2 Menos I/O (staging o DF único)   | Medio-Alto| Medio-Alto|
| Media    | 3.1 Reintentos y timeouts configurables | Medio   | Bajo     |
| Media    | 5.1 Progreso durante clasificación  | UX alto   | Bajo     |
| Media    | 4.1 Niveles de log y rotación        | Medio     | Bajo     |
| Baja     | 1.3 Normalización vectorizada        | Medio     | Bajo     |
| Baja     | 2.1 Caché de config                  | Bajo      | Bajo     |
| Baja     | 2.2 Un solo punto de rutas           | Bajo      | Bajo     |
| Baja     | 6.1 Test integración pipeline        | Alto a largo plazo | Medio |
| Baja     | 6.2 Versiones fijas en requirements  | Medio     | Bajo     |

Recomendación práctica: empezar por **1.1** (embeddings del catálogo) y **4.2** (duración por etapa) para ganar rendimiento y visibilidad; luego **5.1** (progreso en clasificación) y **3.1** (reintentos/timeouts) para robustez y UX con poco esfuerzo.
