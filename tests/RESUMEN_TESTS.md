# Resumen de Tests

Este documento describe todos los tests disponibles en el proyecto.

## Tests Creados

### 1. `test_reentrenamiento_automatico.py`
Tests para verificar el reentrenamiento automático del modelo en primera ejecución.

**Tests incluidos:**
- `test_pipeline_entrena_automaticamente_si_no_hay_modelos`: Verifica que el pipeline entrena automáticamente si no existen modelos.
- `test_pipeline_no_entrena_si_ya_existen_modelos`: Verifica que el pipeline NO entrena si ya existen modelos.

**Ejecutar:**
```bash
pytest tests/test_reentrenamiento_automatico.py -v
```

### 2. `test_filtros_ajuste_manual.py` (GUI)
Tests para verificar que los filtros en la página de ajuste manual funcionan correctamente.

**Tests incluidos:**
- `test_filtro_solo_nuevos_muestra_todos_los_nuevos`: Verifica que el filtro SOLO_NUEVOS muestra todos los programas nuevos, incluso con diferentes formatos.
- `test_filtro_solo_referentes_funciona_correctamente`: Verifica que el filtro SOLO_REFERENTES muestra solo los programas referentes.
- `test_filtro_todos_muestra_todos_los_programas`: Verifica que el filtro TODOS muestra todos los programas sin filtrar.
- `test_busqueda_por_texto_funciona`: Verifica que la búsqueda por texto funciona correctamente.

**Ejecutar:**
```bash
pytest tests/test_filtros_ajuste_manual.py -v
```

**Nota:** Estos tests requieren GUI (marcados con `@pytest.mark.gui`).

### 3. `test_limpieza_historicos.py`
Tests para verificar la limpieza automática de archivos históricos.

**Tests incluidos:**
- `test_limpieza_automatica_consolida_cuando_hay_muchos_archivos`: Verifica que la limpieza automática consolida archivos cuando hay más de 20.
- `test_limpieza_automatica_no_hace_nada_con_pocos_archivos`: Verifica que la limpieza automática NO hace nada cuando hay menos de 20 archivos.
- `test_consolidar_historicos_elimina_duplicados`: Verifica que la consolidación elimina duplicados correctamente.

**Ejecutar:**
```bash
pytest tests/test_limpieza_historicos.py -v
```

### 4. `test_clasificacion_completa.py`
Tests para verificar la clasificación completa de programas nuevos.

**Tests incluidos:**
- `test_clasificacion_agrega_columnas_correctas`: Verifica que la clasificación agrega las columnas correctas al archivo.
- `test_clasificacion_solo_procesa_programas_nuevos`: Verifica que la clasificación solo procesa programas donde PROGRAMA_NUEVO == 'Sí'.

**Ejecutar:**
```bash
pytest tests/test_clasificacion_completa.py -v
```

## Tests Existentes

### 5. `test_procesamiento_programas_nuevos.py`
Tests para el procesamiento de programas nuevos.

### 6. `test_clasificacion_helpers.py`
Tests para funciones auxiliares de clasificación (normalización de niveles, etc.).

### 7. `test_lock_and_prechecks.py`
Tests para locks y validaciones previas del pipeline.

### 8. `test_normalizacion_final.py`
Tests para la normalización final de ortografía y formato.

### 9. `test_historico_programas_nuevos.py`
Tests para el histórico de programas nuevos.

### 10. `test_descarga_commit.py`
Tests para la descarga transaccional (staging → commit).

### 11. `test_app_schema.py`
Tests para validación de schema de archivos.

### 12. `test_gui_smoke_optional.py`
Tests opcionales de GUI (smoke tests).

## Ejecutar Todos los Tests

```bash
# Todos los tests (puede ser lento)
pytest -q

# Tests rápidos (excluye GUI y tests lentos)
pytest -q -m "not gui" --skip-slow

# Todos los tests excepto GUI
pytest -q -m "not gui"

# Excluir tests lentos
pytest -q --skip-slow

# Solo tests de GUI
pytest -q -m "gui"

# Modo verbose
pytest -v

# Tests específicos
pytest tests/test_reentrenamiento_automatico.py -v
pytest tests/test_filtros_simple.py -v  # Tests rápidos de filtros
pytest tests/test_filtros_ajuste_manual.py -v  # Tests con GUI (lentos)
pytest tests/test_limpieza_historicos.py -v
pytest tests/test_clasificacion_completa.py -v
```

## Cobertura de Tests

Los tests cubren:

✅ **Reentrenamiento automático**: Verifica que el modelo se entrena automáticamente en primera ejecución
✅ **Filtros**: Verifica que los filtros en ajuste manual funcionan correctamente
✅ **Limpieza de históricos**: Verifica la consolidación y limpieza automática
✅ **Clasificación**: Verifica que la clasificación agrega las columnas correctas
✅ **Procesamiento**: Verifica el procesamiento de programas nuevos
✅ **Validaciones**: Verifica locks y validaciones previas
✅ **Normalización**: Verifica la normalización de datos
✅ **Histórico**: Verifica la actualización del histórico
✅ **Descarga**: Verifica la descarga transaccional
✅ **Schema**: Verifica la validación de schema

## Notas

- Los tests usan directorios temporales (`tmp_path`) para no modificar archivos reales.
- Los tests de GUI pueden requerir un entorno con display.
- Los tests no ejecutan Selenium real (se mockean las funciones de descarga).
- Los tests de modelos ML usan modelos dummy para evitar dependencias pesadas.
