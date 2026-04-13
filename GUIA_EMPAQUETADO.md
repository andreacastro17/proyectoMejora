# Guía de empaquetado y distribución (PyInstaller)

Resume cómo generar el ejecutable **SniesManager** para usuarios sin Python. Detalle de producto y rutas: **`README.md`**. Instrucciones breves de distribución: **`INSTRUCCIONES_EMPAQUETADO.md`**.

## Objetivo

Obtener **`SniesManager.exe`** que:

- Ejecute la GUI (`app/main.py`) sin instalar Python en el equipo destino.
- Use rutas relativas a la **raíz del proyecto** o a rutas definidas en **`config.json`** (`etl/config.py`).
- Permita tanto el **pipeline SNIES / referentes** como el **estudio de mercado** si se distribuyen los insumos en `ref/` y `ref/backup/`.

## Estructura esperada del repositorio (antes de empaquetar)

```
proyectoMejora/
├── app/
│   └── main.py
├── etl/
├── ref/
│   ├── referentesUnificados.csv   (u .xlsx)
│   ├── catalogoOfertasEAFIT.csv
│   ├── Referente_Categorias.*     (mercado Fase 1)
│   └── backup/                    (insumos SNIES/OLE para mercado)
├── models/
├── docs/                          (opcional, según tu build)
├── build_exe.py
└── requirements.txt
```

## Preparar el entorno (solo máquina de desarrollo)

```bash
python -m venv env
env\Scripts\activate
pip install -r requirements.txt
```

## Empaquetar

```bash
python build_exe.py
```

- Salida típica: **`dist/SniesManager.exe`**.
- En modo **onedir**, PyInstaller crea también **`_internal/`**: debe distribuirse **junto** al `.exe`.
- `build_exe.py` puede copiar el ejecutable a la raíz del proyecto para pruebas locales; revisa la consola al final del script.

## Distribuir a usuarios finales

Copiar como mínimo:

1. `SniesManager.exe` + `_internal/` (si existe).
2. Carpetas **`ref/`** (completa, incluida **`ref/backup/`** si usarán estudio de mercado).
3. **`models/`** con los `.pkl` entrenados.
4. Opcional: **`docs/`**, **`config.json`**.

**Chrome** debe estar instalado en el PC destino para la descarga SNIES.

Los artefactos generados aparecerán en **`outputs/`**, **`outputs/estudio_de_mercado/`** y **`outputs/temp/`** según los flujos ejecutados desde el menú.

## Comportamiento de rutas (`config.py`)

- En desarrollo, la base suele ser la carpeta del repositorio.
- Con **`.exe`** dentro de **`dist/`**, la configuración intenta usar el **padre de `dist/`** como raíz para que `outputs/`, `ref/` y `models/` coincidan con el proyecto y no con una copia aislada dentro de `dist/`.
- Personalización: **`config.json`** (`base_dir`, `outputs_dir`, `ref_dir`, `umbral_referente`, etc.).

## Prueba en equipo limpio

- Windows actualizado, **Chrome** instalado.
- Copiar el paquete completo (`exe`, `_internal`, `ref`, `models`, …).
- Ejecutar `SniesManager.exe` y probar un flujo desde el menú; revisar **`logs/pipeline.log`** ante errores.

## Problemas frecuentes

| Problema | Acción |
|----------|--------|
| `ModuleNotFoundError` al ejecutar el .exe | Añadir el módulo a `hiddenimports` / datos en `build_exe.py` y recompilar |
| Ejecutable muy grande | Esperado (Python embebido, sklearn, sentence-transformers, etc.) |
| Falla la descarga SNIES | Red, portal SNIES, Chrome; revisar `descargaSNIES.py` / logs |
| Mercado sin datos | Completar `ref/backup/` según `README.md` |

## Checklist rápido

- [ ] `pip install -r requirements.txt` sin errores
- [ ] `python build_exe.py` finaliza correctamente
- [ ] Existe `dist/SniesManager.exe` (y `_internal/` si aplica)
- [ ] Paquete de distribución incluye `ref/`, `models/`, y opcionalmente `ref/backup/`
- [ ] Prueba en un PC sin Python
