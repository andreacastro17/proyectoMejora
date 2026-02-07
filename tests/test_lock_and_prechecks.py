from __future__ import annotations

import types
from pathlib import Path

import pandas as pd


def _write_min_programas(path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "NOMBRE_DEL_PROGRAMA": "a",
                "NOMBRE_INSTITUCIÓN": "x",
                "NIVEL_DE_FORMACIÓN": "universitario",
            }
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)


def test_run_pipeline_creates_and_removes_lock(tmp_path: Path, monkeypatch):
    """
    Valida que run_pipeline cree outputs/.pipeline.lock y lo remueva al terminar,
    incluso cuando todo se "mockea" para no depender de Selenium/archivos reales.
    """
    import app.main as m

    out_dir = tmp_path / "outputs"
    docs_dir = tmp_path / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    programas = out_dir / "Programas.xlsx"
    _write_min_programas(programas)
    
    # Crear archivo de normalización final requerido
    normalizacion_final = docs_dir / "normalizacionFinal.xlsx"
    df_norm = pd.DataFrame([{"original": "test", "normalizado": "test"}])
    with pd.ExcelWriter(normalizacion_final, mode="w", engine="openpyxl") as writer:
        df_norm.to_excel(writer, sheet_name="Normalizacion", index=False)

    # Patch rutas en config ANTES de que run_pipeline las importe
    from etl import config
    monkeypatch.setattr(config, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(config, "HISTORIC_DIR", out_dir / "historico")
    monkeypatch.setattr(config, "ARCHIVO_NORMALIZACION", normalizacion_final)
    (out_dir / "historico").mkdir(exist_ok=True)

    # Mockear módulos ETL ANTES de que run_pipeline los importe
    import sys
    mock_normalizar = types.ModuleType("etl.normalizacion")
    mock_normalizar.normalizar_programas = lambda: None
    mock_normalizar.ARCHIVO_PROGRAMAS = programas
    monkeypatch.setitem(sys.modules, "etl.normalizacion", mock_normalizar)
    
    mock_procesamiento = types.ModuleType("etl.procesamientoSNIES")
    mock_procesamiento.procesar_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.procesamientoSNIES", mock_procesamiento)
    
    mock_norm_final = types.ModuleType("etl.normalizacion_final")
    mock_norm_final.aplicar_normalizacion_final = lambda: None
    monkeypatch.setitem(sys.modules, "etl.normalizacion_final", mock_norm_final)
    
    mock_historico = types.ModuleType("etl.historicoProgramasNuevos")
    mock_historico.actualizar_historico_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.historicoProgramasNuevos", mock_historico)
    
    mock_clasificacion = types.ModuleType("etl.clasificacionProgramas")
    mock_clasificacion.clasificar_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.clasificacionProgramas", mock_clasificacion)
    
    mock_logger = types.ModuleType("etl.pipeline_logger")
    mock_logger.log_inicio = lambda: None
    mock_logger.log_fin = lambda *args: None
    mock_logger.log_etapa_iniciada = lambda *args: None
    mock_logger.log_etapa_completada = lambda *args: None
    mock_logger.log_error = lambda *args: None
    mock_logger.log_warning = lambda *args: None
    monkeypatch.setitem(sys.modules, "etl.pipeline_logger", mock_logger)

    # Mockear el módulo etl.descargaSNIES importado en runtime por run_pipeline
    dummy = types.SimpleNamespace(main=lambda log_callback=None: str(programas))
    monkeypatch.setitem(sys.modules, "etl.descargaSNIES", dummy)

    lock = programas.parent / ".pipeline.lock"
    assert not lock.exists()

    rc = m.run_pipeline(tmp_path, log_callback=lambda _s: None)
    assert rc == 0
    assert not lock.exists()


def test_run_pipeline_schema_validation_failure(tmp_path: Path, monkeypatch):
    """
    Si Programas.xlsx no tiene columnas mínimas, run_pipeline debe fallar temprano.
    """
    import app.main as m
    import types
    import sys

    out_dir = tmp_path / "outputs"
    docs_dir = tmp_path / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    programas = out_dir / "Programas.xlsx"

    # Crear Excel inválido (sin columnas mínimas)
    df = pd.DataFrame([{"CÓDIGO_SNIES_DEL_PROGRAMA": "1"}])
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)
    
    # Crear archivo de normalización final requerido
    normalizacion_final = docs_dir / "normalizacionFinal.xlsx"
    df_norm = pd.DataFrame([{"original": "test", "normalizado": "test"}])
    with pd.ExcelWriter(normalizacion_final, mode="w", engine="openpyxl") as writer:
        df_norm.to_excel(writer, sheet_name="Normalizacion", index=False)

    # Patch rutas en config ANTES de que run_pipeline las importe
    from etl import config
    monkeypatch.setattr(config, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(config, "HISTORIC_DIR", out_dir / "historico")
    monkeypatch.setattr(config, "ARCHIVO_NORMALIZACION", normalizacion_final)
    (out_dir / "historico").mkdir(exist_ok=True)

    # Mockear módulos ETL ANTES de que run_pipeline los importe
    mock_normalizar = types.ModuleType("etl.normalizacion")
    called = {"normalizar": False}
    mock_normalizar.normalizar_programas = lambda: called.__setitem__("normalizar", True)
    mock_normalizar.ARCHIVO_PROGRAMAS = programas
    monkeypatch.setitem(sys.modules, "etl.normalizacion", mock_normalizar)
    
    mock_procesamiento = types.ModuleType("etl.procesamientoSNIES")
    mock_procesamiento.procesar_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.procesamientoSNIES", mock_procesamiento)
    
    mock_norm_final = types.ModuleType("etl.normalizacion_final")
    mock_norm_final.aplicar_normalizacion_final = lambda: None
    monkeypatch.setitem(sys.modules, "etl.normalizacion_final", mock_norm_final)
    
    mock_historico = types.ModuleType("etl.historicoProgramasNuevos")
    mock_historico.actualizar_historico_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.historicoProgramasNuevos", mock_historico)
    
    mock_clasificacion = types.ModuleType("etl.clasificacionProgramas")
    mock_clasificacion.clasificar_programas_nuevos = lambda: None
    monkeypatch.setitem(sys.modules, "etl.clasificacionProgramas", mock_clasificacion)
    
    mock_logger = types.ModuleType("etl.pipeline_logger")
    mock_logger.log_inicio = lambda: None
    mock_logger.log_fin = lambda *args: None
    mock_logger.log_etapa_iniciada = lambda *args: None
    mock_logger.log_etapa_completada = lambda *args: None
    mock_logger.log_error = lambda *args: None
    mock_logger.log_warning = lambda *args: None
    monkeypatch.setitem(sys.modules, "etl.pipeline_logger", mock_logger)

    dummy = types.SimpleNamespace(main=lambda log_callback=None: str(programas))
    monkeypatch.setitem(sys.modules, "etl.descargaSNIES", dummy)

    rc = m.run_pipeline(tmp_path, log_callback=lambda _s: None)
    assert rc == 1
    assert called["normalizar"] is False

