# Instrucciones para empaquetar y distribuir el proyecto

El script `build_exe.py` genera **`SniesManager.exe`** con PyInstaller (no `PipelineSNIES.exe`). La documentación general del producto está en `README.md`.

## Empaquetar en .exe

### Paso 1: Preparar el entorno

1. Instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```

2. Verificar insumos mínimos en **`ref/`** (nombres típicos; el código acepta `.csv` o `.xlsx` según existan):

   - `referentesUnificados` + `catalogoOfertasEAFIT` — pipeline SNIES / ML.
   - Para **estudio de mercado**: `Referente_Categorias` y la carpeta **`ref/backup/`** (matrículas, inscritos, primer curso, graduados, IES, OLE según uses).

3. Opcional: `models/*.pkl` entrenados; `docs/` si tu build los incluye (p. ej. reglas de normalización).

### Paso 2: Ejecutar el script de empaquetado

```bash
python build_exe.py
```

El script puede dejar el ejecutable en `dist/SniesManager.exe` y, según el modo PyInstaller, la carpeta **`_internal/`** junto al `.exe` **debe** acompañar al binario (modo onedir).

### Paso 3: Distribuir

1. Copiar **`SniesManager.exe`** y, si aplica, **`_internal/`** completos.
2. Copiar **`ref/`** (incluida **`ref/backup/`** si usarán estudio de mercado), **`models/`**, **`docs/`** si los necesitas.
3. Opcional: **`config.json`** junto al `.exe` para `base_dir`, `outputs_dir`, `umbral_referente`, etc. (ver `etl/config.py`).

## Configuración para carpeta compartida

Crear `config.json` en la misma carpeta que el ejecutable (o en la raíz configurada), por ejemplo:

```json
{
  "outputs_dir": "\\\\servidor\\carpeta_compartida\\outputs",
  "ref_dir": "",
  "models_dir": "",
  "docs_dir": "",
  "logs_dir": "",
  "headless": false,
  "max_wait_download_sec": 180
}
```

Rutas vacías se resuelven respecto al directorio base del proyecto (`base_dir` o la carpeta del `.exe` según `config.py`).

### Estructura de ejemplo para distribución

```
CarpetaDistribucion/
├── SniesManager.exe
├── _internal/          (si PyInstaller la generó — no separar del .exe)
├── config.json         (opcional)
├── ref/
│   ├── referentesUnificados.csv
│   ├── catalogoOfertasEAFIT.csv
│   ├── Referente_Categorias.xlsx
│   └── backup/
│       ├── matriculas/
│       ├── inscritos/
│       ├── matriculas primer curso/
│       ├── graduados/
│       └── ies/
├── models/
│   └── *.pkl
└── docs/               (opcional)
```

## Uso del ejecutable

1. Colocar `SniesManager.exe`, `_internal/` (si existe), `ref/`, `models/`, etc., según la convención de tu despliegue.
2. Ajustar `config.json` si las salidas deben ir a otra unidad o red.
3. Ejecutar **`SniesManager.exe`**. Los resultados irán a `outputs/` (y `outputs/estudio_de_mercado/` para el pipeline de mercado) salvo que redes config.

## Solución de problemas

- **Chrome no instalado:** necesario para la descarga SNIES (Selenium).
- **Permisos:** la carpeta de `outputs_dir` debe ser escribible.
- **Archivo Excel en uso:** cerrar el libro antes de reprocesar.
- **Faltan datos de mercado:** revisar `ref/backup/` y `logs/pipeline.log`.
