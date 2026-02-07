# Optimización de Tests

## Problema Identificado

Los tests se estaban demorando demasiado debido a:
1. **Imports pesados**: `app.main` importa `tkinter` y otros módulos pesados
2. **Imports de ML**: `etl.clasificacionProgramas` puede cargar `sentence-transformers`
3. **Tests de GUI**: Crear ventanas Tkinter es lento
4. **Falta de mocks**: Algunos tests ejecutaban código real en lugar de mocks

## Optimizaciones Implementadas

### 1. Mocks Antes de Imports
- Los mocks ahora se configuran **ANTES** de importar módulos pesados
- Se usan `types.ModuleType` para crear módulos mock sin importar los reales
- Se evita importar `app.main` completo, solo se importa `run_pipeline`

### 2. Marcadores de Tests
- `@pytest.mark.gui`: Tests que requieren GUI (pueden ser lentos)
- `@pytest.mark.slow`: Tests que son lentos (pueden saltarse)

### 3. Tests Rápidos Alternativos
- `test_filtros_simple.py`: Tests rápidos que verifican solo la lógica sin GUI
- Estos tests son mucho más rápidos y cubren la misma funcionalidad

### 4. Opción --skip-slow
- Permite saltar tests lentos con `pytest --skip-slow`
- Útil para ejecuciones rápidas durante desarrollo

## Cómo Ejecutar Tests Rápidos

```bash
# Solo tests rápidos (sin GUI ni tests lentos)
pytest -q -m "not gui" --skip-slow

# Todos los tests excepto GUI
pytest -q -m "not gui"

# Todos los tests excepto los lentos
pytest -q --skip-slow

# Todos los tests (puede ser lento)
pytest -q
```

## Tests Optimizados

### `test_reentrenamiento_automatico.py`
- ✅ Mockea módulos ETL antes de importar
- ✅ Evita importar `app.main` completo
- ✅ Usa `run_pipeline` directamente

### `test_clasificacion_completa.py`
- ✅ Marcado como `@pytest.mark.slow`
- ✅ Mockea `sentence-transformers` si no está disponible
- ✅ Puede saltarse con `--skip-slow`

### `test_filtros_ajuste_manual.py`
- ✅ Marcado como `@pytest.mark.gui` y `@pytest.mark.slow`
- ✅ Puede saltarse completamente con `-m "not gui"`

### `test_filtros_simple.py` (NUEVO)
- ✅ Tests rápidos que verifican solo la lógica
- ✅ No requiere GUI
- ✅ Ejecuta en milisegundos

## Resultados Esperados

**Antes de optimización:**
- Todos los tests: ~30-60 segundos
- Tests sin GUI: ~20-40 segundos

**Después de optimización:**
- Tests rápidos (`-m "not gui" --skip-slow`): ~2-5 segundos
- Tests sin GUI: ~5-10 segundos
- Todos los tests: ~30-60 segundos (sin cambios, pero ahora se pueden saltar los lentos)

## Recomendaciones

1. **Durante desarrollo**: Usa `pytest -q -m "not gui" --skip-slow` para ejecuciones rápidas
2. **Antes de commit**: Ejecuta todos los tests para verificar que todo funciona
3. **En CI/CD**: Ejecuta todos los tests excepto GUI si no hay display disponible
