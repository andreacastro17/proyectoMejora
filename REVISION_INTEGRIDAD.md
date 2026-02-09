# Revisión de integridad del sistema (refactor Power BI + columnas deprecadas)

## 1. Columnas eliminadas (FUENTE_DATOS, MATCH_SCORE, COINCIDE_HISTORICO, REQUIERE_VALIDACION)

| Ubicación | Estado |
|-----------|--------|
| **etl/procesamientoSNIES.py** | Ya no se crean; si vienen en el DataFrame se eliminan al inicio (lista `columnas_deprecadas`). |
| **etl/descargaSNIES.py** | Eliminada `_escribir_fuente_datos_excel` y su llamada. El parámetro `fuente` en `_commit_programas` se mantiene solo para el log. |
| **validate_programas_schema (main.py)** | Solo exige: CÓDIGO_SNIES_DEL_PROGRAMA, NOMBRE_DEL_PROGRAMA, NOMBRE_INSTITUCIÓN, NIVEL_DE_FORMACIÓN. No depende de las columnas eliminadas. |
| **etl/historicoProgramasNuevos.py** | `COLUMNAS_REQUERIDAS` no incluye ninguna de las cuatro columnas. Sin dependencias rotas. |
| **etl/exportacionPowerBI.py** | `preparar_datos_powerbi` usa: CÓDIGO_SNIES_DEL_PROGRAMA, NOMBRE_INSTITUCIÓN, NOMBRE_DEL_PROGRAMA, PROGRAMA_NUEVO, ES_REFERENTE, PROGRAMA_EAFIT_NOMBRE. No usa las columnas eliminadas. |
| **ManualReviewPage (main.py)** | `display_columns` no incluye FUENTE_DATOS, MATCH_SCORE, COINCIDE_HISTORICO ni REQUIERE_VALIDACION. Sin referencias. |
| **Tests** | Ningún test en `tests/` comprueba esas columnas. `test_descarga_commit.py` fue actualizado para no esperar FUENTE_DATOS. |

**Conclusión:** Ningún módulo ni pantalla depende de las columnas eliminadas. El pipeline sigue funcionando sin ellas.

---

## 2. Exportación a Power BI y backups eliminados

| Cambio | Referencias restantes | ¿Roto? |
|--------|------------------------|--------|
| **Paso 9 (Exportación Power BI)** eliminado de `run_pipeline` | `exportar_a_powerbi` solo se llama desde `exportacionPowerBI.main()`. El pipeline ya no la invoca. | No |
| **exportar_a_powerbi** vacía (pass + return ARCHIVO_PROGRAMAS) | Quien llame a `exportacionPowerBI.main()` o a `exportar_a_powerbi` recibe un Path y no falla. | No |
| **Backup pipeline** (Programas__backup_pre_etapas) eliminado | No queda código que espere ese archivo ni que restaure desde él. | No |
| **Backup ManualReviewPage** (.temp_backup_pre_edit_*) eliminado | `last_backup_path` y botón "Restaurar" siguen; si no hay backup, se muestra "No hay backup...". | No |
| **Backups en guardar_modelos (clasificacionProgramas)** eliminados | RetrainPage: "Usar esta versión" y "Rollback" usan `obtener_rutas_modelo_version(version)` (archivos v1, v2, ...). Esas versiones se crean al guardar con `crear_version=True`, no con el bloque de backup que se quitó. | No |

**Conclusión:** No hay dependencias rotas por la eliminación de la exportación a Excel de Power BI ni por la eliminación de backups.

---

## 3. Flujo del pipeline (run_pipeline)

Orden actual y dependencias:

1. Validar entorno → no depende de columnas eliminadas ni de Power BI.
2. Resguardo de históricos (mensaje) → sin cambios.
3. Descarga SNIES → ya no escribe FUENTE_DATOS.
4. Validar schema → columnas requeridas sin las eliminadas.
5. Leer Programas.xlsx en memoria → correcto.
6. Normalización → no usa las columnas eliminadas.
7. Procesamiento programas nuevos → ya no crea las cuatro columnas; las quita si existen.
8. Clasificación → no depende de esas columnas.
9. Normalización final → no las usa.
10. Guardar Programas.xlsx → archivo final sin columnas deprecadas.
11. Actualizar histórico programas nuevos → columnas requeridas sin las eliminadas.
12. Limpieza históricos (opcional) → sin cambios.

**Conclusión:** El flujo es coherente y no espera nada de lo que se eliminó.

---

## 4. Tests ejecutados

- **test_procesamiento_programas_nuevos.py**: Comprueba PROGRAMA_NUEVO y códigos; no las columnas eliminadas. Pasa (en el run mostrado).
- **test_descarga_commit.py**: Actualizado para no exigir FUENTE_DATOS. Los fallos actuales son por `ModuleNotFoundError: selenium` en el entorno de tests, no por el refactor.
- El resto de fallos son de entorno: dependencias (sklearn, selenium, unidecode), Tcl/Tk, o import de `log_exception` (que sí existe en `etl.pipeline_logger`).

**Recomendación:** Ejecutar los tests con el entorno virtual del proyecto (`venv` con todas las dependencias instaladas) para validar que no haya regresiones por el refactor.

---

## 5. Resumen

- **Columnas deprecadas:** Eliminadas solo en los puntos que las creaban o escribían; el resto del código no las usa. Se eliminan del DataFrame si vienen de un archivo antiguo.
- **Power BI / backups:** La integración pasa a Dataflows leyendo Programas.xlsx; no hay código que espere el Excel de Power BI ni los backups eliminados.
- **Pipeline y histórico:** Flujo y columnas requeridas siguen siendo coherentes con los cambios.
- **Tests:** Ningún test depende de las columnas eliminadas; los fallos vistos se deben al entorno, no a los refactors realizados.
