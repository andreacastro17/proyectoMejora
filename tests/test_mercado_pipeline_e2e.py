"""
Test de integración end-to-end del pipeline de estudio de mercado (Fases 1–5).

Objetivo:
- Validar que el pipeline corre de punta a punta con datos sintéticos pequeños
  usando un directorio temporal (sin tocar outputs/ref reales).
- Evitar red/SNIES: se precargan CSVs cacheados en outputs/historico/raw para Fase 2.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.slow
def test_pipeline_mercado_end_to_end(tmp_path: Path, monkeypatch):
    pytest.importorskip("pyarrow")
    from etl import config

    # Pre-crear stubs ANTES de update_paths_for_base_dir para evitar prints "[ERROR]" ruidosos
    ref_backup = tmp_path / "ref" / "backup"
    ref_backup.mkdir(parents=True, exist_ok=True)
    for nombre in ("referentesUnificados.xlsx", "catalogoOfertasEAFIT.xlsx"):
        p = ref_backup / nombre
        if not p.exists():
            with pd.ExcelWriter(p, engine="openpyxl") as writer:
                pd.DataFrame({"_stub": [1]}).to_excel(writer, sheet_name="stub", index=False)

    # 1) Redirigir todo a un sandbox temporal
    config.update_paths_for_base_dir(tmp_path)

    # Importar después de redirigir rutas (evita usar rutas globales antiguas)
    from etl.mercado_pipeline import run_fase1, run_fase2, run_fase3, run_fase4, run_fase5
    from etl.config import (
        ARCHIVO_PROGRAMAS,
        ARCHIVO_ESTUDIO_MERCADO,
        CHECKPOINT_BASE_MAESTRA,
        MODELO_CLASIFICADOR_MERCADO,
        RAW_HISTORIC_DIR,
        REF_DIR,
    )

    # 2) Crear insumos mínimos
    # 2.1 Programas.xlsx (hojas: Programas + Cobertura opcional)
    n_programas = 24
    programas = []
    for i in range(1, n_programas + 1):
        programas.append(
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": str(10_000 + i),
                "NOMBRE_DEL_PROGRAMA": f"Programa {i} Ingeniería",
                "TITULO_OTORGADO": "Ingeniero(a)",
                "CINE_F_2013_AC_CAMPO_DETALLADO": "Ingeniería",
                "CINE_F_2013_AC_CAMPO_ESPECÍFIC": "Sistemas",
                "ÁREA_DE_CONOCIMIENTO": "Ingeniería, arquitectura, urbanismo y afines",
                "NOMBRE_INSTITUCIÓN": "IES Test",
                "NIVEL_DE_FORMACIÓN": "Pregrado",
                "ESTADO_PROGRAMA": "Activo" if i % 3 else "Inactivo",
                "FECHA_DE_REGISTRO_EN_SNIES": "2023-01-15" if i % 2 else "2020-06-01",
                # costo en base puede venir vacío; Fase 3 lo completará con Cobertura/mediana
                "COSTO_MATRÍCULA_ESTUD_NUEVOS": pd.NA,
            }
        )
    df_prog = pd.DataFrame(programas)

    # Cobertura (Principal) con algunos costos
    df_cob = pd.DataFrame(
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": str(10_000 + i),
                "TIPO_CUBRIMIENTO": "PRINCIPAL",
                "VALOR_MATRICULA": 8_000_000 + (i * 10_000),
            }
            for i in range(1, 13)
        ]
    )

    ARCHIVO_PROGRAMAS.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(ARCHIVO_PROGRAMAS, engine="openpyxl") as writer:
        df_prog.to_excel(writer, sheet_name="Programas", index=False)
        df_cob.to_excel(writer, sheet_name="Cobertura", index=False)

    # 2.2 Referente_Categorias.xlsx en ref/backup
    # Importante: CalibratedClassifierCV usa cv=5; se requieren >=5 ejemplos por clase en el set de entrenamiento.
    # Con test_size=0.2, asegurar >=7-8 ejemplos por clase en total para que y_train tenga >=5.
    (REF_DIR / "backup").mkdir(parents=True, exist_ok=True)
    cats = ["CAT_A", "CAT_B", "CAT_C"]
    ref_rows = []
    # 24 filas (8 por clase) balanceado
    for ci, cat in enumerate(cats):
        for j in range(1, 9):
            # Usar SNIES únicos para no perder muestras al deduplicar por SNIES en Fase 1
            snies = str(10_000 + (ci * 100 + j))
            ref_rows.append(
                {
                    "SNIES": snies,
                    "CATEGORIA_FINAL": cat,
                    # SALARIO_OLE ya viene en SMLMV (ej. 3.5)
                    "SALARIO_OLE": 3.0 + (ci * 0.5) + (j * 0.1),
                    "TASA_COTIZANTES": 0.65,
                    "INSCRITOS_2023": 50 + j,
                    "INSCRITOS_2024": 60 + j,
                    "NOMBRE_DEL_PROGRAMA": f"Ref {cat} {j}",
                    "TITULO_OTORGADO": "Ingeniero(a)",
                    "ÁREA_DE_CONOCIMIENTO": "Ingeniería",
                    "CINE_F_2013_AC_CAMPO_DETALLADO": "Ingeniería",
                    "CINE_F_2013_AC_CAMPO_ESPECÍFIC": "Sistemas",
                }
            )
    df_ref = pd.DataFrame(ref_rows)
    # Nota: update_paths_for_base_dir hace fallback a REF_DIR/Referente_Categorias.xlsx si no encuentra otros referentes.
    # Para robustez del test, escribimos el referente tanto en ref/backup como en ref/.
    ref_path_backup = REF_DIR / "backup" / "Referente_Categorias.xlsx"
    ref_path_root = REF_DIR / "Referente_Categorias.xlsx"
    for ref_path in (ref_path_backup, ref_path_root):
        with pd.ExcelWriter(ref_path, engine="openpyxl") as writer:
            df_ref.to_excel(writer, sheet_name="1_Consolidado", index=False)

    # Volver a refrescar rutas para que config detecte Referente_Categorias recién creado en ref/backup
    config.update_paths_for_base_dir(tmp_path)
    from etl.config import ARCHIVO_REFERENTE_CATEGORIAS  # re-leer después del refresh
    assert ARCHIVO_REFERENTE_CATEGORIAS.exists()

    # 2.3 Backup OLE (para que Fase 2 lo exporte a raw/ole_indicadores.csv)
    df_ole_backup = pd.DataFrame(
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": [str(10_001), str(10_002), str(10_003)],
            "TASA COTIZANTES\n(0.0-1.0)": [0.70, 0.60, 0.55],
            "SALARIO OLE": [3.2, 3.8, 4.1],
        }
    )
    (REF_DIR / "backup").mkdir(parents=True, exist_ok=True)
    ole_backup_path = REF_DIR / "backup" / "ole_indicadores.csv"
    df_ole_backup.to_csv(ole_backup_path, index=False, encoding="utf-8-sig")

    # 2.4 Cache de históricos (para que Fase 2 NO intente red)
    RAW_HISTORIC_DIR.mkdir(parents=True, exist_ok=True)
    # Crear CSVs mínimos para 2019–2024 (sem 1 y 2) con 3 SNIES
    for year in range(2019, 2025):
        for sem in (1, 2):
            df_mat = pd.DataFrame(
                {
                    "CÓDIGO_SNIES_DEL_PROGRAMA": [str(10_001), str(10_002), str(10_003)],
                    "MATRICULADOS": [10 + sem, 20 + sem, 30 + sem],
                    "SEMESTRE": [sem, sem, sem],
                }
            )
            df_ins = pd.DataFrame(
                {
                    "CÓDIGO_SNIES_DEL_PROGRAMA": [str(10_001), str(10_002), str(10_003)],
                    "INSCRITOS": [40 + sem, 50 + sem, 60 + sem],
                    "SEMESTRE": [sem, sem, sem],
                }
            )
            df_mat.to_csv(RAW_HISTORIC_DIR / f"matriculados_{year}_{sem}.csv", index=False, encoding="utf-8-sig")
            df_ins.to_csv(RAW_HISTORIC_DIR / f"inscritos_{year}_{sem}.csv", index=False, encoding="utf-8-sig")

    # 3) Ejecutar fases 1–5
    df_base = run_fase1()
    assert df_base is not None and len(df_base) == n_programas
    assert CHECKPOINT_BASE_MAESTRA.exists()
    assert MODELO_CLASIFICADOR_MERCADO.exists()

    # Fase 2 debe usar cache y exportar ole_indicadores.csv desde backup
    run_fase2()
    assert (RAW_HISTORIC_DIR / "ole_indicadores.csv").exists()

    run_fase3()
    sabana_path = CHECKPOINT_BASE_MAESTRA.parent / "sabana_consolidada.parquet"
    assert sabana_path.exists()

    ag = run_fase4()
    assert ag is not None and len(ag) >= 1
    assert (CHECKPOINT_BASE_MAESTRA.parent / "agregado_categorias.parquet").exists()
    # Validar columnas salariales (corrección financiera)
    assert "salario_promedio" in ag.columns
    assert "salario_promedio_smlmv" in ag.columns
    assert "salario_proyectado_pesos_hoy" in ag.columns

    run_fase5(ag)
    assert ARCHIVO_ESTUDIO_MERCADO.exists()

    # 4) Validar Excel final: hojas mínimas
    xls = pd.ExcelFile(ARCHIVO_ESTUDIO_MERCADO)
    assert "programas_detalle" in xls.sheet_names
    assert "total" in xls.sheet_names

