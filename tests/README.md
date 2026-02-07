# Tests

Esta carpeta contiene pruebas automatizadas del sistema.

## Ejecutar

1. Activa tu entorno virtual.
2. Instala dependencias de desarrollo:

```bash
pip install -r requirements-dev.txt
```

3. Ejecuta todos los tests:

```bash
pytest -q
```

O ejecuta tests específicos:

```bash
# Tests de reentrenamiento automático
pytest tests/test_reentrenamiento_automatico.py -v

# Tests de filtros
pytest tests/test_filtros_ajuste_manual.py -v

# Tests de limpieza de históricos
pytest tests/test_limpieza_historicos.py -v

# Tests de clasificación
pytest tests/test_clasificacion_completa.py -v
```

> Nota: Si corres `pytest` con el Python del sistema (sin el `.venv`), pueden faltar librerías como `unidecode`.
> Asegúrate de activar el entorno virtual antes de ejecutar.

## Marcadores

- `gui`: pruebas que crean ventanas Tkinter (pueden ser frágiles en entornos headless)
- `slow`: pruebas que son lentas de ejecutar (pueden saltarse con --skip-slow)

Para excluirlas:

```bash
# Excluir tests de GUI
pytest -q -m "not gui"

# Excluir tests lentos
pytest -q --skip-slow

# Excluir ambos
pytest -q -m "not gui" --skip-slow
```

## Estructura de Tests

- `test_reentrenamiento_automatico.py`: Tests para el reentrenamiento automático en primera ejecución
- `test_filtros_ajuste_manual.py`: Tests para los filtros en la página de ajuste manual
- `test_limpieza_historicos.py`: Tests para la limpieza automática de archivos históricos
- `test_clasificacion_completa.py`: Tests para la clasificación completa de programas nuevos
- `test_procesamiento_programas_nuevos.py`: Tests para el procesamiento de programas nuevos
- `test_clasificacion_helpers.py`: Tests para funciones auxiliares de clasificación
- `test_lock_and_prechecks.py`: Tests para locks y validaciones previas
- `test_normalizacion_final.py`: Tests para la normalización final
- `test_historico_programas_nuevos.py`: Tests para el histórico de programas nuevos
- `test_descarga_commit.py`: Tests para la descarga transaccional
- `test_app_schema.py`: Tests para validación de schema
- `test_gui_smoke_optional.py`: Tests opcionales de GUI

## Notas

- Las pruebas usan directorios temporales (`tmp_path`) para no modificar `outputs/` real.
- No se ejecuta Selenium en tests unitarios.
- Los tests de GUI pueden requerir un entorno con display (no funcionan en CI headless).

