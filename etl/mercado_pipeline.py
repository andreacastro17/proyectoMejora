"""
Orquestador del pipeline de mercado (clasificación de programas por categoría).

Sección 1 de 5: Fase 1 — Base maestra con categorías (ML).
Fases 2–5 son stubs y se implementarán en secciones posteriores.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from etl.config import (
    ARCHIVO_PROGRAMAS,
    ARCHIVO_REFERENTE_CATEGORIAS,
    BENCHMARK_COSTO,
    CHECKPOINT_BASE_MAESTRA,
    HOJA_PROGRAMAS,
    HOJA_REFERENTE_CATEGORIAS,
    MODELO_CLASIFICADOR_MERCADO,
    MODELS_DIR,
    RAW_HISTORIC_DIR,
    REF_DIR,
    get_smlmv_sesion,
)
from etl.exceptions_helpers import leer_excel_con_reintentos
from etl.normalizacion import limpiar_texto
from etl.pipeline_logger import (
    log_error,
    log_etapa_completada,
    log_etapa_iniciada,
    log_info,
    log_resultado,
    log_warning,
)
from etl.scraper_matriculas import SNIESMatriculasScraper
from etl.scraper_ole import OLEScraper
from etl.scoring import apply_scoring


def _normalizar_codigo_snies(serie: pd.Series) -> pd.Series:
    """Convierte códigos SNIES a string y elimina sufijo '.0'."""
    return (
        serie.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\.0$", "", regex=True)
    )


def _build_texto_ml(df: pd.DataFrame) -> pd.Series:
    """
    Construye el texto de entrenamiento/predicción combinando campos descriptivos.

    Orden de prioridad: nombre > título > campo detallado > campo específico > área conocimiento.
    NO incluye CINE_F_2013_AC_CAMPO_AMPLIO ni NIVEL_DE_FORMACIÓN para evitar ruido.
    """
    cols = [
        "NOMBRE_DEL_PROGRAMA",
        "TITULO_OTORGADO",
        "CINE_F_2013_AC_CAMPO_DETALLADO",
        "CINE_F_2013_AC_CAMPO_ESPECÍFIC",
        "ÁREA_DE_CONOCIMIENTO",
    ]
    texto = pd.Series("", index=df.index, dtype="string")
    for col in cols:
        if col in df.columns:
            texto = texto + " " + df[col].fillna("").astype(str)
    return texto.astype(str).str.strip()


def run_fase1() -> pd.DataFrame:
    """
    Fase 1: Base maestra con categorías (ML).

    - Lee Programas.xlsx y Referente_Categorias.xlsx.
    - Cruza por SNIES (left join).
    - Entrena TF-IDF + Logistic Regression sobre el referente.
    - Predice CATEGORIA_FINAL para programas sin cruce.
    - Guarda checkpoint en outputs/temp/base_maestra.parquet.

    Returns:
        DataFrame con todas las columnas de Programas + CATEGORIA_FINAL +
        FUENTE_CATEGORIA + TASA_COTIZANTES + SALARIO_OLE + INSCRITOS_2023 + INSCRITOS_2024.
    """
    log_etapa_iniciada("Fase 1: Base maestra con categorías (ML)")

    # 1.1 Leer Programas.xlsx
    if not ARCHIVO_PROGRAMAS.exists():
        msg = f"No se encontró {ARCHIVO_PROGRAMAS}. Ejecute primero el pipeline principal."
        log_error(msg)
        raise FileNotFoundError(msg)
    df_programas = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
    log_info(f"Programas.xlsx cargado: {len(df_programas)} filas")

    # 1.2 Leer Referente_Categorias.xlsx y deduplicar por SNIES (primera ocurrencia)
    if not ARCHIVO_REFERENTE_CATEGORIAS.exists():
        msg = f"No se encontró {ARCHIVO_REFERENTE_CATEGORIAS}."
        log_error(msg)
        raise FileNotFoundError(msg)
    df_referente = leer_excel_con_reintentos(
        ARCHIVO_REFERENTE_CATEGORIAS,
        sheet_name=HOJA_REFERENTE_CATEGORIAS,
    )
    if "SNIES" not in df_referente.columns:
        msg = "Referente_Categorias debe tener columna SNIES."
        log_error(msg)
        raise ValueError(msg)
    df_referente = df_referente.drop_duplicates(subset=["SNIES"], keep="first").reset_index(drop=True)
    log_info(f"Referente_Categorias cargado y deduplicado: {len(df_referente)} filas")

    # 1.2b Human-in-the-Loop: aplicar correcciones manuales desde ref/feedback_manual.csv (no se modifica el Excel del referente)
    feedback_path = REF_DIR / "feedback_manual.csv"
    if feedback_path.exists():
        try:
            df_feedback = pd.read_csv(feedback_path, dtype={"SNIES": str}, encoding="utf-8-sig")
            if "SNIES" in df_feedback.columns and "CATEGORIA_FINAL" in df_feedback.columns:
                df_fb = (
                    df_feedback[["SNIES", "CATEGORIA_FINAL"]]
                    .dropna(subset=["SNIES"])
                    .drop_duplicates(subset=["SNIES"], keep="last")
                )
                df_fb["SNIES"] = df_fb["SNIES"].astype(str).str.strip()
                if len(df_fb) > 0:
                    ref_idx = df_referente.set_index("SNIES")
                    fb_idx = df_fb.set_index("SNIES")
                    ref_idx.update(fb_idx)
                    df_referente = ref_idx.reset_index()
                    log_info(f"Referente actualizado con {len(df_fb)} correcciones desde feedback_manual.csv")
        except Exception as e:
            log_warning(f"No se pudo aplicar feedback_manual.csv: {e}. Se continúa con el referente original.")

    # 1.3 Normalizar llaves de cruce y texto para ML
    clave_prog = "CÓDIGO_SNIES_DEL_PROGRAMA"
    if clave_prog not in df_programas.columns:
        msg = f"Programas.xlsx debe tener columna {clave_prog}."
        log_error(msg)
        raise ValueError(msg)
    df_programas["_snies_norm"] = _normalizar_codigo_snies(df_programas[clave_prog])
    df_referente["_snies_norm"] = _normalizar_codigo_snies(df_referente["SNIES"])

    df_programas["_texto_ml_norm"] = _build_texto_ml(df_programas).apply(limpiar_texto)
    df_referente["_texto_ml_norm"] = _build_texto_ml(df_referente).apply(limpiar_texto)

    if "CATEGORIA_FINAL" not in df_referente.columns:
        msg = "Referente_Categorias debe tener columna CATEGORIA_FINAL."
        log_error(msg)
        raise ValueError(msg)

    # 1.4 Left join Programas sobre Referente por SNIES
    columnas_extra_ref = [
        "CATEGORIA_FINAL",
        "TASA_COTIZANTES",
        "SALARIO_OLE",
        "INSCRITOS_2023",
        "INSCRITOS_2024",
    ]
    columnas_traer = [c for c in columnas_extra_ref if c in df_referente.columns]
    ref_merge = df_referente[["_snies_norm"] + columnas_traer].copy()

    df_base = df_programas.merge(
        ref_merge,
        on="_snies_norm",
        how="left",
        suffixes=("", "_ref"),
    )

    # Asegurar columnas de referente (nulas donde no hay cruce)
    for c in columnas_extra_ref:
        if c not in df_base.columns:
            df_base[c] = pd.NA

    # FUENTE_CATEGORIA: CRUCE_SNIES donde sí hubo cruce, resto se llenará con PREDICCION_ML
    df_base["FUENTE_CATEGORIA"] = pd.NA
    mask_cruce = df_base["CATEGORIA_FINAL"].notna()
    df_base.loc[mask_cruce, "FUENTE_CATEGORIA"] = "CRUCE_SNIES"
    log_info(f"Registros con categoría por cruce SNIES: {mask_cruce.sum()}")
    log_info(f"Registros sin categoría (a predecir): {(~mask_cruce).sum()}")

    # 1.5 Entrenar clasificador TF-IDF + Logistic Regression (calibrado)
    df_entrenar = df_referente.dropna(subset=["CATEGORIA_FINAL", "_texto_ml_norm"])
    df_entrenar = df_entrenar[df_entrenar["_texto_ml_norm"].astype(str).str.len() > 0]
    if len(df_entrenar) < 10:
        msg = "Insuficientes registros en referente para entrenar (mínimo 10)."
        log_error(msg)
        raise ValueError(msg)

    X_text = df_entrenar["_texto_ml_norm"].astype(str)
    y = df_entrenar["CATEGORIA_FINAL"].astype(str)

    # Stratified split solo es posible si todas las clases tienen al menos 2 ejemplos.
    y_counts = y.value_counts()
    rare_classes = y_counts[y_counts < 2]
    if not rare_classes.empty:
        log_warning(
            "Se desactiva stratify en train_test_split porque hay categorías con menos de 2 ejemplos: "
            + ", ".join(f"{c}({n})" for c, n in rare_classes.items())
        )
        stratify = None
    else:
        stratify = y

    X_train_t, X_test_t, y_train, y_test = train_test_split(
        X_text,
        y,
        test_size=0.2,
        random_state=42,
        stratify=stratify,
    )

    # Determinar cv efectivo para CalibratedClassifierCV según tamaño mínimo de clase en y_train
    y_train_counts = y_train.value_counts()
    min_count = int(y_train_counts.min()) if not y_train_counts.empty else 0
    use_calibration = True
    calib_cv = 5
    if min_count < 2:
        log_warning(
            "Se omite CalibratedClassifierCV porque hay categorías con menos de 2 ejemplos en entrenamiento: "
            + ", ".join(f"{c}({n})" for c, n in y_train_counts.items())
        )
        use_calibration = False
    elif min_count < 5:
        calib_cv = min_count
        log_warning(
            f"Se ajusta CalibratedClassifierCV a cv={calib_cv} porque hay categorías escasas "
            f"(mínimo por clase en y_train: {min_count})."
        )

    base_clf = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        C=1.5,
        solver="lbfgs",
        n_jobs=-1,
        random_state=42,
    )
    if use_calibration:
        final_clf = CalibratedClassifierCV(base_clf, cv=calib_cv, method="sigmoid")
    else:
        final_clf = base_clf

    pipeline_ml = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    max_features=30_000,
                    ngram_range=(1, 3),
                    sublinear_tf=True,
                    min_df=2,
                    analyzer="word",
                    strip_accents="unicode",
                    token_pattern=r"(?u)\b[a-záéíóúüñA-ZÁÉÍÓÚÜÑ]{2,}\b",
                ),
            ),
            ("clf", final_clf),
        ]
    )
    pipeline_ml.fit(X_train_t, y_train)

    y_pred = pipeline_ml.predict(X_test_t)
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average="macro", zero_division=0)
    report = classification_report(y_test, y_pred, zero_division=0)

    log_info(f"Modelo ML — Accuracy (test 20%): {accuracy:.3f}")
    log_info(f"Modelo ML — F1 macro (test 20%): {f1:.3f}")
    log_info(f"Classification report (test):\n{report}")

    # Cinco categorías con peor F1
    report_dict = classification_report(
        y_test, y_pred, output_dict=True, zero_division=0
    )
    f1_by_class = [
        (k, report_dict[k]["f1-score"])
        for k in report_dict
        if k not in ("accuracy", "macro avg", "weighted avg") and isinstance(k, str)
    ]
    f1_by_class.sort(key=lambda x: x[1])
    peores_5 = f1_by_class[:5]
    log_resultado(
        "Cinco categorías con peor F1: " + ", ".join(f"{c}({f:.3f})" for c, f in peores_5)
    )

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline_ml, MODELO_CLASIFICADOR_MERCADO)
    log_info(f"Modelo guardado en {MODELO_CLASIFICADOR_MERCADO}")

    # 1.6 Predecir CATEGORIA_FINAL para los sin categoría
    mask_sin_cat = df_base["FUENTE_CATEGORIA"].isna()
    if mask_sin_cat.any():
        X_pred_text = df_programas.loc[mask_sin_cat, "_texto_ml_norm"].astype(str)
        preds = pipeline_ml.predict(X_pred_text)
        df_base.loc[mask_sin_cat, "CATEGORIA_FINAL"] = preds
        df_base.loc[mask_sin_cat, "FUENTE_CATEGORIA"] = "PREDICCION_ML"

    # 1.6b Probabilidades calibradas y bandera de revisión
    if "PROBABILIDAD" not in df_base.columns:
        df_base["PROBABILIDAD"] = pd.NA
    df_base["REQUIERE_REVISION"] = False

    mask_prediccion = df_base["FUENTE_CATEGORIA"] == "PREDICCION_ML"
    if mask_prediccion.any():
        X_pred_text = df_programas.loc[mask_prediccion, "_texto_ml_norm"].astype(str)
        proba_matrix = pipeline_ml.predict_proba(X_pred_text)
        max_proba = proba_matrix.max(axis=1)
        df_base.loc[mask_prediccion, "PROBABILIDAD"] = (
            pd.Series(max_proba, index=df_base.index[mask_prediccion]).round(4)
        )

        UMBRAL_REVISION = 0.70
        prob_num = pd.to_numeric(df_base["PROBABILIDAD"], errors="coerce")
        df_base.loc[
            mask_prediccion & (prob_num < UMBRAL_REVISION),
            "REQUIERE_REVISION",
        ] = True

        n_total_pred = int(mask_prediccion.sum())
        n_revision = int(df_base["REQUIERE_REVISION"].sum())
        n_confiables = n_total_pred - n_revision
        pct_revision = (n_revision / n_total_pred * 100) if n_total_pred > 0 else 0

        log_info(f"Predicciones ML: {n_total_pred:,} programas")
        log_info(f"  Confianza >= 0.70 (OK):               {n_confiables:,} ({100 - pct_revision:.1f}%)")
        log_info(f"  Confianza < 0.70 (REQUIERE REVISIÓN): {n_revision:,} ({pct_revision:.1f}%)")
        if pct_revision > 20:
            log_warning(
                f"Más del 20% de las predicciones tienen baja confianza ({pct_revision:.1f}%). "
                f"Considerar revisar manualmente los {n_revision} programas marcados."
            )

        bins = [0, 0.50, 0.60, 0.70, 0.80, 0.90, 1.01]
        labels = ["<0.50", "0.50-0.60", "0.60-0.70", "0.70-0.80", "0.80-0.90", ">=0.90"]
        proba_series = pd.Series(max_proba)
        counts = (
            pd.cut(proba_series, bins=bins, labels=labels, right=False)
            .value_counts()
            .sort_index()
        )
        log_info("Distribución de confianza en predicciones ML:")
        for rango, count in counts.items():
            pct = (count / len(proba_series) * 100) if len(proba_series) else 0
            marker = " ← REVISIÓN REQUERIDA" if str(rango) in ["<0.50", "0.50-0.60", "0.60-0.70"] else ""
            log_info(f"  {rango}: {int(count):,} programas ({pct:.1f}%){marker}")

    # Para programas que cruzaron por SNIES: probabilidad = 1.0, no requieren revisión
    mask_cruce_snies = df_base["FUENTE_CATEGORIA"] == "CRUCE_SNIES"
    if mask_cruce_snies.any():
        df_base.loc[mask_cruce_snies, "PROBABILIDAD"] = 1.0
        df_base.loc[mask_cruce_snies, "REQUIERE_REVISION"] = False

    # Reservar 'MANUAL' para correcciones futuras (no se asigna aquí)
    df_base = df_base.drop(columns=["_snies_norm", "_nombre_norm", "_texto_ml_norm"], errors="ignore")

    # 1.7 Guardar checkpoint parquet
    assert "PROBABILIDAD" in df_base.columns, "Falta columna PROBABILIDAD"
    assert "REQUIERE_REVISION" in df_base.columns, "Falta columna REQUIERE_REVISION"
    CHECKPOINT_BASE_MAESTRA.parent.mkdir(parents=True, exist_ok=True)
    df_base.to_parquet(CHECKPOINT_BASE_MAESTRA, index=False)
    log_info(f"Checkpoint guardado: {CHECKPOINT_BASE_MAESTRA} ({len(df_base)} filas)")
    log_etapa_completada("Fase 1: Base maestra con categorías (ML)", f"{len(df_base)} filas")

    return df_base


def run_fase2() -> None:
    """
    Fase 2: Descarga de datos faltantes (matrículas históricas SNIES e indicadores OLE).
    Si los scrapers no están implementados o fallan, se registra warning y se continúa
    para que Fase 3 pueda trabajar con imputación.
    """
    log_etapa_iniciada("Fase 2: Descarga de datos faltantes (Selenium)")
    scraper_mat = SNIESMatriculasScraper()
    scraper_ole = OLEScraper()

    any_matriculas = False
    any_ole = False

    # Scraper A: matrículas e inscritos por año y semestre (2019-2024 × 1, 2)
    for year in range(2019, 2025):
        for semestre in (1, 2):
            try:
                df_mat = scraper_mat.download_matriculados(year, semestre)
                if df_mat is not None and len(df_mat) > 0:
                    any_matriculas = True
            except Exception as e:
                log_warning(f"Matriculados {year}-{semestre}: {e}. Continuando.")
            try:
                df_ins = scraper_mat.download_inscritos(year, semestre)
                if df_ins is not None and len(df_ins) > 0:
                    any_matriculas = True
            except Exception as e:
                log_warning(f"Inscritos {year}-{semestre}: {e}. Continuando.")

    # Scraper B: indicadores OLE (lista SNIES desde checkpoint Fase 1)
    snies_list = []
    if CHECKPOINT_BASE_MAESTRA.exists():
        try:
            df_base = pd.read_parquet(CHECKPOINT_BASE_MAESTRA)
            col = "CÓDIGO_SNIES_DEL_PROGRAMA"
            if col in df_base.columns:
                snies_list = df_base[col].dropna().astype(str).str.strip().unique().tolist()
        except Exception as e:
            log_warning(f"No se pudo leer base maestra para lista SNIES: {e}")
    try:
        df_ole = scraper_ole.download_indicadores(snies_list)
        if df_ole is not None and len(df_ole) > 0:
            any_ole = True
    except Exception as e:
        log_warning(f"Descarga OLE: {e}. Continuando.")

    if not any_matriculas and not any_ole:
        log_error(
            "Fase 2: ambos scrapers fallaron o no devolvieron datos. "
            "Fase 3 continuará con imputación."
        )
    else:
        log_info("Fase 2: al menos un scraper aportó datos.")
    log_etapa_completada("Fase 2: Descarga de datos faltantes", "")


def _cargar_csv_raw(raw_dir: Path, nombre: str) -> pd.DataFrame:
    """Carga un CSV de raw si existe; si no, retorna DataFrame vacío con columnas esperadas."""
    path = raw_dir / nombre
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(
            path,
            dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str, "CODIGO_SNIES_DEL_PROGRAMA": str},
            encoding="utf-8-sig",
        )
        return df if df is not None and len(df) > 0 else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _cargar_ole_indicadores(raw_dir: Path, ref_dir: Path) -> tuple[pd.DataFrame, str]:
    """
    Carga ole_indicadores con cascada de fuentes:
      1) raw_dir/ole_indicadores.csv           → scraper reciente (prioridad máxima)
      2) ref_dir/backup/ole_indicadores.csv    → datos estáticos (backup)
      3) vacío                                 → Fase 3 imputará por mediana

    Retorna: (df_ole, fuente_label) donde fuente_label ∈ {"SCRAPER", "BACKUP", "NONE"}.
    El df retornado queda estandarizado con columnas:
      - CÓDIGO_SNIES_DEL_PROGRAMA
      - TASA_COTIZANTES
      - SALARIO_OLE
    """
    COLS_OUT = ["CÓDIGO_SNIES_DEL_PROGRAMA", "TASA_COTIZANTES", "SALARIO_OLE"]

    def _normalizar_cols(df: pd.DataFrame) -> pd.DataFrame:
        # Unificar variantes de nombre de columna SNIES
        rename_map = {}
        for c in df.columns:
            c_str = str(c).strip()
            if c_str.upper() in {"CODIGO_SNIES_DEL_PROGRAMA", "CÓDIGO_SNIES_DEL_PROGRAMA"}:
                rename_map[c] = "CÓDIGO_SNIES_DEL_PROGRAMA"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _intentar_cargar(ruta: Path, label: str) -> pd.DataFrame | None:
        if not ruta.exists():
            return None
        try:
            df = pd.read_csv(ruta, dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str, "CODIGO_SNIES_DEL_PROGRAMA": str}, encoding="utf-8-sig")
            df = _normalizar_cols(df)
            if "CÓDIGO_SNIES_DEL_PROGRAMA" not in df.columns:
                log_warning(f"[Fase 3] OLE desde {label}: falta columna CÓDIGO_SNIES_DEL_PROGRAMA/CODIGO_SNIES_DEL_PROGRAMA.")
                return None
            if "TASA_COTIZANTES" not in df.columns or "SALARIO_OLE" not in df.columns:
                log_warning(
                    f"[Fase 3] OLE desde {label}: columnas insuficientes. "
                    f"Encontradas: {set(df.columns)}"
                )
                return None
            df["CÓDIGO_SNIES_DEL_PROGRAMA"] = df["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.strip()
            df["TASA_COTIZANTES"] = pd.to_numeric(df["TASA_COTIZANTES"], errors="coerce")
            df["SALARIO_OLE"] = pd.to_numeric(df["SALARIO_OLE"], errors="coerce")
            tasa_max = df["TASA_COTIZANTES"].max(skipna=True)
            if pd.notna(tasa_max) and float(tasa_max) > 1.5:
                log_warning(
                    f"[Fase 3] OLE desde {label}: TASA_COTIZANTES parece venir en porcentaje (max={tasa_max:.1f}). "
                    "Convirtiendo a decimal (/100)."
                )
                df["TASA_COTIZANTES"] = df["TASA_COTIZANTES"] / 100.0
            log_info(
                f"[Fase 3] OLE cargado desde {label}: {len(df):,} programas | "
                f"con tasa: {int(df['TASA_COTIZANTES'].notna().sum()):,} | "
                f"con salario: {int(df['SALARIO_OLE'].notna().sum()):,}"
            )
            return df[COLS_OUT].copy()
        except Exception as e:
            log_warning(f"[Fase 3] Error leyendo OLE desde {label}: {e}")
            return None

    fuentes = [
        (raw_dir / "ole_indicadores.csv", "RAW/scraper", "SCRAPER"),
        (ref_dir / "backup" / "ole_indicadores.csv", "ref/backup", "BACKUP"),
    ]
    for ruta, label, out_label in fuentes:
        df = _intentar_cargar(ruta, label)
        if df is not None and len(df) > 0:
            return df, out_label

    log_warning(
        "[Fase 3] No se encontró ole_indicadores.csv en ninguna fuente. "
        "TASA_COTIZANTES y SALARIO_OLE serán imputados por mediana de categoría."
    )
    return pd.DataFrame(columns=COLS_OUT), "NONE"


def run_fase3() -> None:
    """
    Fase 3: Consolidación en sábana única.
    Incorpora matrículas históricas, OLE en cascada, costo de matrícula y columnas derivadas.
    """
    log_etapa_iniciada("Fase 3: Consolidación en sábana única")
    raw_dir = RAW_HISTORIC_DIR
    base = pd.read_parquet(CHECKPOINT_BASE_MAESTRA)
    codigo_col = "CÓDIGO_SNIES_DEL_PROGRAMA"

    # Guard idempotencia: eliminar columnas previas que serán recalculadas (evita mezclar ejecuciones)
    cols_to_refresh = (
        [f"matricula_{y}" for y in range(2019, 2025)]
        + [f"matricula_{y}_1" for y in range(2019, 2025)]
        + [f"matricula_{y}_2" for y in range(2019, 2025)]
        + [f"inscritos_{y}" for y in range(2019, 2025)]
    )
    cols_existentes = [c for c in cols_to_refresh if c in base.columns]
    if cols_existentes:
        base = base.drop(columns=cols_existentes)
        log_info(
            f"[Fase 3] Idempotencia: eliminadas {len(cols_existentes)} columnas previas de matrículas/inscritos para recalcular."
        )

    # Normalizar código para joins
    base["_codigo_norm"] = _normalizar_codigo_snies(base[codigo_col])

    # 3.1 Matrículas e inscritos históricos (2019-2024, semestre 1+2)
    matricula_cols = [f"matricula_{y}" for y in range(2019, 2025)]
    sem_cols = [f"matricula_{y}_{s}" for y in range(2019, 2025) for s in (1, 2)]
    inscritos_cols = [f"inscritos_{y}" for y in range(2019, 2025)]

    for year in range(2019, 2025):
        m1 = _cargar_csv_raw(raw_dir, f"matriculados_{year}_1.csv")
        m2 = _cargar_csv_raw(raw_dir, f"matriculados_{year}_2.csv")
        if codigo_col in m1.columns:
            m1[codigo_col] = _normalizar_codigo_snies(m1[codigo_col])
        if codigo_col in m2.columns:
            m2[codigo_col] = _normalizar_codigo_snies(m2[codigo_col])

        # Columnas semestrales individuales (matricula_{year}_1, matricula_{year}_2)
        col_sem1 = f"matricula_{year}_1"
        col_sem2 = f"matricula_{year}_2"
        if len(m1) > 0 and "MATRICULADOS" in m1.columns:
            val_m1_sem = m1.groupby(codigo_col, as_index=False)["MATRICULADOS"].sum()
            val_m1_sem = val_m1_sem.rename(columns={"MATRICULADOS": col_sem1})
            val_m1_sem["_codigo_norm"] = _normalizar_codigo_snies(val_m1_sem[codigo_col])
            if col_sem1 in base.columns:
                base = base.drop(columns=[col_sem1])
            base = base.merge(
                val_m1_sem[["_codigo_norm", col_sem1]],
                on="_codigo_norm",
                how="left",
            )
        else:
            base[col_sem1] = 0
        if len(m2) > 0 and "MATRICULADOS" in m2.columns:
            val_m2_sem = m2.groupby(codigo_col, as_index=False)["MATRICULADOS"].sum()
            val_m2_sem = val_m2_sem.rename(columns={"MATRICULADOS": col_sem2})
            val_m2_sem["_codigo_norm"] = _normalizar_codigo_snies(val_m2_sem[codigo_col])
            if col_sem2 in base.columns:
                base = base.drop(columns=[col_sem2])
            base = base.merge(
                val_m2_sem[["_codigo_norm", col_sem2]],
                on="_codigo_norm",
                how="left",
            )
        else:
            base[col_sem2] = 0

        # Suma anual (matricula_{year} = sem1 + sem2)
        val_m1 = m1.groupby(codigo_col, as_index=False)["MATRICULADOS"].sum() if len(m1) > 0 and "MATRICULADOS" in m1.columns else pd.DataFrame(columns=[codigo_col, "MATRICULADOS"])
        val_m2 = m2.groupby(codigo_col, as_index=False)["MATRICULADOS"].sum() if len(m2) > 0 and "MATRICULADOS" in m2.columns else pd.DataFrame(columns=[codigo_col, "MATRICULADOS"])
        merge_m = val_m1.merge(val_m2, on=codigo_col, how="outer", suffixes=("", "_2"))
        merge_m["matricula"] = merge_m["MATRICULADOS"].fillna(0) + (merge_m["MATRICULADOS_2"].fillna(0) if "MATRICULADOS_2" in merge_m.columns else 0)
        merge_m["_codigo_norm"] = _normalizar_codigo_snies(merge_m[codigo_col])
        col_name_mat = f"matricula_{year}"
        if col_name_mat in base.columns:
            base = base.drop(columns=[col_name_mat])
        base = base.merge(merge_m[["_codigo_norm", "matricula"]].rename(columns={"matricula": col_name_mat}), on="_codigo_norm", how="left")

        # Inscritos: sem1 + sem2
        i1 = _cargar_csv_raw(raw_dir, f"inscritos_{year}_1.csv")
        i2 = _cargar_csv_raw(raw_dir, f"inscritos_{year}_2.csv")
        if codigo_col in i1.columns:
            i1[codigo_col] = _normalizar_codigo_snies(i1[codigo_col])
        if codigo_col in i2.columns:
            i2[codigo_col] = _normalizar_codigo_snies(i2[codigo_col])
        val_i1 = i1.groupby(codigo_col, as_index=False)["INSCRITOS"].sum() if len(i1) > 0 and "INSCRITOS" in i1.columns else pd.DataFrame(columns=[codigo_col, "INSCRITOS"])
        val_i2 = i2.groupby(codigo_col, as_index=False)["INSCRITOS"].sum() if len(i2) > 0 and "INSCRITOS" in i2.columns else pd.DataFrame(columns=[codigo_col, "INSCRITOS"])
        merge_i = val_i1.merge(val_i2, on=codigo_col, how="outer", suffixes=("", "_2"))
        merge_i["inscritos"] = merge_i["INSCRITOS"].fillna(0) + (merge_i["INSCRITOS_2"].fillna(0) if "INSCRITOS_2" in merge_i.columns else 0)
        merge_i["_codigo_norm"] = _normalizar_codigo_snies(merge_i[codigo_col])
        col_name_ins = f"inscritos_{year}"
        if col_name_ins in base.columns:
            base = base.drop(columns=[col_name_ins])
        base = base.merge(merge_i[["_codigo_norm", "inscritos"]].rename(columns={"inscritos": f"inscritos_{year}"}), on="_codigo_norm", how="left")

    # Rellenar nulos de matrícula (anual + semestral) e inscritos con 0
    for col in matricula_cols + sem_cols + inscritos_cols:
        if col in base.columns:
            base[col] = base[col].fillna(0)

    # Fallback: si inscritos del scraper están en 0 pero el referente trae INSCRITOS_2023/2024, usar esos valores
    for year in (2023, 2024):
        col_ins = f"inscritos_{year}"
        col_ref = f"INSCRITOS_{year}"
        if col_ins not in base.columns:
            continue
        if base[col_ins].sum() == 0 and col_ref in base.columns:
            base[col_ins] = base[col_ref].fillna(0)
            log_info(f"[Fase 3] Fallback: {col_ins} rellenado desde referente ({col_ref})")

    # 3.2 OLE en cascada: REFERENTE → SCRAPER → IMPUTADO (mediana por CATEGORIA_FINAL)
    if "FUENTE_OLE" not in base.columns:
        base["FUENTE_OLE"] = pd.NA
    tiene_ref = base["TASA_COTIZANTES"].notna() | base["SALARIO_OLE"].notna()
    base.loc[tiene_ref, "FUENTE_OLE"] = "REFERENTE"

    df_ole, fuente_ole = _cargar_ole_indicadores(raw_dir=raw_dir, ref_dir=REF_DIR)
    if len(df_ole) > 0 and "CÓDIGO_SNIES_DEL_PROGRAMA" in df_ole.columns and "TASA_COTIZANTES" in df_ole.columns:
        df_ole["_codigo_norm"] = _normalizar_codigo_snies(df_ole["CÓDIGO_SNIES_DEL_PROGRAMA"])
        ole_agg = df_ole.drop_duplicates(subset=["_codigo_norm"], keep="first")[["_codigo_norm", "TASA_COTIZANTES", "SALARIO_OLE"]]
        ole_agg = ole_agg.rename(columns={"TASA_COTIZANTES": "_ole_tasa", "SALARIO_OLE": "_ole_salario"})
        base = base.merge(ole_agg, on="_codigo_norm", how="left")
        mask_sin_ole = base["FUENTE_OLE"].isna()
        base.loc[mask_sin_ole, "TASA_COTIZANTES"] = base.loc[mask_sin_ole, "TASA_COTIZANTES"].fillna(base.loc[mask_sin_ole, "_ole_tasa"])
        base.loc[mask_sin_ole, "SALARIO_OLE"] = base.loc[mask_sin_ole, "SALARIO_OLE"].fillna(base.loc[mask_sin_ole, "_ole_salario"])
        base.loc[
            mask_sin_ole & (base["TASA_COTIZANTES"].notna() | base["SALARIO_OLE"].notna()),
            "FUENTE_OLE",
        ] = fuente_ole if fuente_ole in ("SCRAPER", "BACKUP") else "SCRAPER"
        base = base.drop(columns=["_ole_tasa", "_ole_salario"], errors="ignore")

    mask_imputar_ole = base["FUENTE_OLE"].isna()
    if mask_imputar_ole.any() and "CATEGORIA_FINAL" in base.columns:
        for col_ole in ["TASA_COTIZANTES", "SALARIO_OLE"]:
            medianas = base.groupby("CATEGORIA_FINAL")[col_ole].transform("median")
            base.loc[mask_imputar_ole, col_ole] = base.loc[mask_imputar_ole, col_ole].fillna(medianas[mask_imputar_ole])
        base.loc[mask_imputar_ole, "FUENTE_OLE"] = "IMPUTADO"
    base["FUENTE_OLE"] = base["FUENTE_OLE"].fillna("IMPUTADO")

    # Logs detallados de OLE
    total_prog = len(base)
    if total_prog:
        fuente_counts = base["FUENTE_OLE"].value_counts(dropna=False)
        n_ref = int(fuente_counts.get("REFERENTE", 0))
        n_scr = int(fuente_counts.get("SCRAPER", 0))
        n_bak = int(fuente_counts.get("BACKUP", 0))
        n_imp = int(fuente_counts.get("IMPUTADO", 0))
        log_info(
            f"OLE — Referente: {n_ref} | Scraper: {n_scr} | Backup: {n_bak} | Imputado: {n_imp} "
            f"(total: {total_prog} programas)"
        )
        if "CATEGORIA_FINAL" in base.columns and n_imp:
            df_imp_ole = base[base["FUENTE_OLE"] == "IMPUTADO"]
            if not df_imp_ole.empty:
                imp_por_cat = df_imp_ole.groupby("CATEGORIA_FINAL")["FUENTE_OLE"].count()
                total_por_cat = base.groupby("CATEGORIA_FINAL")["FUENTE_OLE"].count()
                top_imp = imp_por_cat.sort_values(ascending=False).head(5)
                log_info("Top 5 categorías con más imputación OLE:")
                for cat, cnt in top_imp.items():
                    total_cat = int(total_por_cat.get(cat, 0))
                    pct = (cnt / total_cat * 100) if total_cat else 0
                    log_info(f"  · {cat}: {cnt} imputados de {total_cat} ({pct:.1f}%)")
                    if total_cat and (cnt / total_cat) > 0.80:
                        log_warning(
                            f"⚠️ Categoría '{cat}' tiene {pct:.1f}% de datos OLE imputados — baja confiabilidad."
                        )

    # 3.3 Costo de matrícula: base → Cobertura (Principal) → mediana por categoría
    costo_col = "COSTO_MATRÍCULA_ESTUD_NUEVOS"
    if costo_col not in base.columns:
        base[costo_col] = pd.NA
    # Bandera para identificar costos imputados por mediana de categoría
    if "COSTO_IMPUTADO_MEDIANA" not in base.columns:
        base["COSTO_IMPUTADO_MEDIANA"] = False
    try:
        df_cob = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name="Cobertura")
        if df_cob is not None and len(df_cob) > 0 and "TIPO_CUBRIMIENTO" in df_cob.columns:
            cob_principal = df_cob[df_cob["TIPO_CUBRIMIENTO"].astype(str).str.strip().str.upper() == "PRINCIPAL"].copy()
            if "VALOR_MATRICULA" in cob_principal.columns and codigo_col in cob_principal.columns:
                cob_principal["_codigo_norm"] = _normalizar_codigo_snies(cob_principal[codigo_col])
                cob_principal = cob_principal.groupby("_codigo_norm", as_index=False)["VALOR_MATRICULA"].first()
                base = base.merge(cob_principal.rename(columns={"VALOR_MATRICULA": "_costo_cob"}), on="_codigo_norm", how="left")
                base[costo_col] = base[costo_col].fillna(base["_costo_cob"])
                base = base.drop(columns=["_costo_cob"], errors="ignore")
    except Exception as e:
        log_warning(f"No se pudo cargar hoja Cobertura: {e}")
    mask_costo_nulo = base[costo_col].isna()
    if mask_costo_nulo.any() and "CATEGORIA_FINAL" in base.columns:
        medianas_costo = base.groupby("CATEGORIA_FINAL")[costo_col].transform("median")
        mask_imputar_costo = mask_costo_nulo & medianas_costo.notna()
        base.loc[mask_imputar_costo, costo_col] = base.loc[mask_imputar_costo, costo_col].fillna(
            medianas_costo[mask_imputar_costo]
        )
        base.loc[mask_imputar_costo, "COSTO_IMPUTADO_MEDIANA"] = True

    # Logs detallados de costos
    n_total_costos = len(base)
    if n_total_costos:
        n_imputados_costo = int(base["COSTO_IMPUTADO_MEDIANA"].sum())
        n_con_dato = int(base[costo_col].notna().sum())
        log_info(
            f"Costos — Con dato real: {n_con_dato} | Imputados por mediana: {n_imputados_costo}"
        )
        if "CATEGORIA_FINAL" in base.columns and n_imputados_costo:
            df_imp_costo = base[base["COSTO_IMPUTADO_MEDIANA"]]
            if not df_imp_costo.empty:
                imp_costo_cat = df_imp_costo.groupby("CATEGORIA_FINAL")["COSTO_IMPUTADO_MEDIANA"].count()
                total_costo_cat = base.groupby("CATEGORIA_FINAL")[costo_col].count()
                top_costo = imp_costo_cat.sort_values(ascending=False).head(5)
                log_info("Top 5 categorías con más costos imputados:")
                for cat, cnt in top_costo.items():
                    total_cat = int(total_costo_cat.get(cat, 0))
                    pct = (cnt / total_cat * 100) if total_cat else 0
                    log_info(f"  · {cat}: {cnt} imputados de {total_cat} ({pct:.1f}%)")
                    if total_cat and (cnt / total_cat) > 0.80:
                        log_warning(
                            f"⚠️ Categoría '{cat}' tiene {pct:.1f}% de costos imputados — baja confiabilidad."
                        )

    # 3.4 Columnas derivadas
    # Regla de negocio: ACTIVO = (matricula_último_año > 0) OR (ESTADO_PROGRAMA == 'activo')
    # Corrige programas marcados "inactivo" pero con matrícula real.
    ultimo_y = next((y for y in range(2025, 2018, -1) if f"matricula_{y}" in base.columns), None)
    mat_ultimo = base.get(f"matricula_{ultimo_y}", pd.Series(0, index=base.index)).fillna(0) if ultimo_y else pd.Series(0, index=base.index)
    estado_activo = (
        base["ESTADO_PROGRAMA"].astype(str).str.strip().str.lower() == "activo"
        if "ESTADO_PROGRAMA" in base.columns
        else pd.Series(False, index=base.index)
    )
    base["es_activo"] = ((mat_ultimo > 0) | estado_activo).astype(bool)
    try:
        log_info(
            f"[Fase 3] es_activo: {int(base['es_activo'].sum()):,} activos | "
            f"{int((mat_ultimo > 0).sum()):,} por matrícula ({ultimo_y}) | "
            f"{int(estado_activo.sum()):,} por ESTADO_PROGRAMA"
        )
    except Exception:
        pass
    try:
        fechas = pd.to_datetime(base.get("FECHA_DE_REGISTRO_EN_SNIES", pd.Series()), errors="coerce")
        base["es_programa_nuevo"] = fechas >= pd.Timestamp("2022-01-01")
    except Exception:
        base["es_programa_nuevo"] = False
    base["es_programa_nuevo"] = base["es_programa_nuevo"].fillna(False)
    mat_2024 = base.get("matricula_2024", pd.Series(0, index=base.index)).fillna(0)
    base["tiene_matricula_2024"] = (mat_2024 > 0).astype(bool)

    base = base.drop(columns=["_codigo_norm"], errors="ignore")

    # Validación post-merge: matrículas anuales y semestrales
    n_total = len(base)
    log_info("[Fase 3] === VALIDACIÓN DE MATRÍCULAS ===")
    for year in range(2019, 2025):
        col = f"matricula_{year}"
        if col in base.columns:
            nonzero = int((base[col].fillna(0) > 0).sum())
            pct = (nonzero / n_total * 100) if n_total else 0
            flag = "✓" if pct > 5 else "⚠ ALERTA"
            log_info(
                f"[Fase 3] {flag} {col}: {nonzero:,} programas con matrícula > 0 ({pct:.1f}%)"
            )
        col_s1 = f"matricula_{year}_1"
        col_s2 = f"matricula_{year}_2"
        if col_s1 in base.columns and col_s2 in base.columns:
            nz1 = int((base[col_s1].fillna(0) > 0).sum())
            nz2 = int((base[col_s2].fillna(0) > 0).sum())
            log_info(f"[Fase 3]   sem1={nz1:,}  sem2={nz2:,}")

    # Alerta: si la mayoría de programas tiene matrícula en 0 (falla masiva)
    for col in ["matricula_2022", "matricula_2023", "matricula_2024"]:
        if col not in base.columns or n_total == 0:
            continue
        con_dato = (base[col].fillna(0) > 0).sum()
        pct = con_dato / n_total
        if pct < 0.05:
            log_warning(
                f"[Fase 3] ALERTA: Falla masiva de datos en {col} "
                f"({pct * 100:.1f}% programas con valor > 0). Coloca los archivos en ref/backup/matriculas/."
            )

    # 3.5 Guardar sábana y log
    sabana_path = CHECKPOINT_BASE_MAESTRA.parent / "sabana_consolidada.parquet"
    SCHEMA_VERSION = "v3"
    base["schema_version"] = SCHEMA_VERSION
    base.to_parquet(sabana_path, index=False)
    n = len(base)
    pct_mat24 = (base["tiene_matricula_2024"].sum() / n * 100) if n else 0
    ole_reales = (base["FUENTE_OLE"].isin(["REFERENTE", "SCRAPER", "BACKUP"])).sum()
    pct_ole = (ole_reales / n * 100) if n else 0
    tiene_costo = base[costo_col].notna().sum()
    pct_costo = (tiene_costo / n * 100) if n else 0
    log_info(f"Sábana consolidada: {sabana_path} ({n} filas)")
    log_resultado(f"Total filas: {n}")
    log_resultado(f"% programas con matricula_2024 > 0: {pct_mat24:.1f}%")
    log_resultado(f"% programas con datos OLE reales (no imputados): {pct_ole:.1f}%")
    log_resultado(f"% programas con costo de matrícula disponible: {pct_costo:.1f}%")
    log_etapa_completada("Fase 3: Consolidación en sábana única", f"{n} filas")


def run_fase4_desde_sabana(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta la lógica de agregación y scoring de la Fase 4 a partir de un DataFrame de sábana ya cargado.

    No lee ni escribe archivos; retorna únicamente el DataFrame agregado por CATEGORIA_FINAL.
    """
    if "CATEGORIA_FINAL" not in df.columns:
        raise ValueError("La sábana no tiene columna CATEGORIA_FINAL.")

    dup_cols = [c for c in df.columns if str(c).endswith("_x") or str(c).endswith("_y")]
    if dup_cols:
        raise ValueError(
            f"La sábana tiene columnas duplicadas (_x/_y) como {dup_cols[:5]}. "
            "Elimine el archivo 'outputs/temp/sabana_consolidada.parquet' y vuelva a ejecutar la Fase 3 para limpiar los datos."
        )

    years = list(range(2019, 2025))
    grouped = df.groupby("CATEGORIA_FINAL", as_index=True)

    def _count_false(s: pd.Series) -> int:
        return (s == False).sum()

    simple_agg = {}
    for y in years:
        c = f"matricula_{y}"
        if c in df.columns:
            simple_agg[f"suma_matricula_{y}"] = pd.NamedAgg(column=c, aggfunc="sum")
            simple_agg[f"prom_matricula_{y}"] = pd.NamedAgg(column=c, aggfunc="mean")
    for y in years:
        for s in (1, 2):
            c_sem = f"matricula_{y}_{s}"
            if c_sem in df.columns:
                simple_agg[f"suma_matricula_{y}_{s}"] = pd.NamedAgg(column=c_sem, aggfunc="sum")
    for y in [2023, 2024]:
        c = f"inscritos_{y}"
        if c in df.columns:
            simple_agg[f"inscritos_{y}_suma"] = pd.NamedAgg(column=c, aggfunc="sum")
            simple_agg[f"inscritos_{y}_prom"] = pd.NamedAgg(column=c, aggfunc="mean")
    if "SALARIO_OLE" in df.columns:
        simple_agg["salario_promedio"] = pd.NamedAgg(column="SALARIO_OLE", aggfunc="mean")
    if "COSTO_MATRÍCULA_ESTUD_NUEVOS" in df.columns:
        simple_agg["costo_promedio"] = pd.NamedAgg(column="COSTO_MATRÍCULA_ESTUD_NUEVOS", aggfunc="mean")
    if "es_activo" in df.columns:
        simple_agg["programas_activos"] = pd.NamedAgg(column="es_activo", aggfunc="sum")
        simple_agg["programas_inactivos"] = pd.NamedAgg(column="es_activo", aggfunc=_count_false)
    if "es_programa_nuevo" in df.columns:
        simple_agg["programas_nuevos_3a"] = pd.NamedAgg(column="es_programa_nuevo", aggfunc="sum")

    ag = grouped.agg(**simple_agg)

    for y in years:
        mat_col = f"matricula_{y}"
        if mat_col in df.columns:
            ag[f"num_programas_{y}"] = grouped[mat_col].apply(lambda s: (s > 0).sum())
    if "num_programas_2019" not in ag.columns:
        ag["num_programas_2019"] = 0
    if "num_programas_2024" not in ag.columns:
        ag["num_programas_2024"] = 0

    ag = ag.reset_index()

    # var_suma y var_prom (2020-2024)
    for y in range(2020, 2025):
        s_curr = ag.get(f"suma_matricula_{y}", pd.Series(dtype=float))
        s_prev = ag.get(f"suma_matricula_{y-1}", pd.Series(dtype=float))
        if s_curr is not None and s_prev is not None and len(s_prev) == len(ag):
            den = s_prev.replace(0, np.nan)
            ag[f"var_suma_{y}"] = (s_curr - s_prev) / den
        p_curr = ag.get(f"prom_matricula_{y}", pd.Series(dtype=float))
        p_prev = ag.get(f"prom_matricula_{y-1}", pd.Series(dtype=float))
        if p_curr is not None and p_prev is not None and len(p_prev) == len(ag):
            den_p = p_prev.replace(0, np.nan)
            ag[f"var_prom_{y}"] = (p_curr - p_prev) / den_p

    total_2019 = ag["suma_matricula_2019"].sum() if "suma_matricula_2019" in ag.columns else 0
    total_2024 = ag["suma_matricula_2024"].sum() if "suma_matricula_2024" in ag.columns else 0
    if total_2019 and total_2019 != 0:
        ag["participacion_2019"] = ag["suma_matricula_2019"] / total_2019
    else:
        ag["participacion_2019"] = np.nan
    if total_2024 and total_2024 != 0:
        ag["participacion_2024"] = ag["suma_matricula_2024"] / total_2024
    else:
        ag["participacion_2024"] = np.nan

    var_suma_cols = [f"var_suma_{y}" for y in range(2020, 2025) if f"var_suma_{y}" in ag.columns]
    var_prom_cols = [f"var_prom_{y}" for y in range(2020, 2025) if f"var_prom_{y}" in ag.columns]
    ag["AAGR_suma"] = ag[var_suma_cols].mean(axis=1) if var_suma_cols else np.nan
    ag["AAGR_prom"] = ag[var_prom_cols].mean(axis=1) if var_prom_cols else np.nan

    # CAGR_suma requiere suma_matricula_2019 y suma_matricula_2024
    ag["CAGR_suma"] = np.nan
    if "suma_matricula_2019" in ag.columns and "suma_matricula_2024" in ag.columns:
        suma_2019 = ag["suma_matricula_2019"]
        suma_2024 = ag["suma_matricula_2024"]
        mask_cagr = (suma_2019 > 0) & (suma_2024 > 0)
        ag.loc[mask_cagr, "CAGR_suma"] = (suma_2024[mask_cagr] / suma_2019[mask_cagr]) ** (1 / 5) - 1

    # Bloque B: pct_no_matriculados y var_inscritos
    if "inscritos_2023_suma" in ag.columns and "suma_matricula_2023" in ag.columns:
        den = ag["inscritos_2023_suma"].replace(0, np.nan)
        pct_raw = (ag["inscritos_2023_suma"] - ag["suma_matricula_2023"]) / den
        # Clip a [0, 1]: cuando inscritos < matrícula (datos de distinta escala),
        # el valor negativo es un artefacto — se trata como "todos matriculados" (pct=0).
        # Cuando inscritos = 0 (sin datos), se deja NaN para que scoring use fill neutral.
        ag["pct_no_matriculados_2023"] = pct_raw.clip(lower=0, upper=1)
    else:
        ag["pct_no_matriculados_2023"] = np.nan
    if "inscritos_2024_suma" in ag.columns and "suma_matricula_2024" in ag.columns:
        den4 = ag["inscritos_2024_suma"].replace(0, np.nan)
        pct_raw = (ag["inscritos_2024_suma"] - ag["suma_matricula_2024"]) / den4
        ag["pct_no_matriculados_2024"] = pct_raw.clip(lower=0, upper=1)
    else:
        ag["pct_no_matriculados_2024"] = np.nan
    if "inscritos_2023_suma" in ag.columns and "inscritos_2024_suma" in ag.columns:
        den_i = ag["inscritos_2023_suma"].replace(0, np.nan)
        ag["var_inscritos"] = (ag["inscritos_2024_suma"] - ag["inscritos_2023_suma"]) / den_i
    else:
        ag["var_inscritos"] = np.nan

    # Bloque C: var_programas, pct_con_matricula, prom_matricula_por_programa_2024
    if "num_programas_2019" in ag.columns and "num_programas_2024" in ag.columns:
        den_p = ag["num_programas_2019"].replace(0, np.nan)
        ag["var_programas"] = (ag["num_programas_2024"] - ag["num_programas_2019"]) / den_p
    else:
        ag["var_programas"] = np.nan
    if "programas_activos" in ag.columns:
        ag["pct_con_matricula"] = ag["num_programas_2024"] / ag["programas_activos"].replace(0, np.nan)
    else:
        ag["pct_con_matricula"] = np.nan
    if "num_programas_2024" in ag.columns and "suma_matricula_2024" in ag.columns:
        ag["prom_matricula_por_programa_2024"] = ag["suma_matricula_2024"] / ag["num_programas_2024"].replace(0, np.nan)
    else:
        ag["prom_matricula_por_programa_2024"] = np.nan

    # Bloque D: distancia_costo_pct
    if "costo_promedio" in ag.columns:
        ag["distancia_costo_pct"] = (ag["costo_promedio"] - BENCHMARK_COSTO) / BENCHMARK_COSTO * 100
    else:
        ag["distancia_costo_pct"] = np.nan

    # salario_promedio ya viene expresado en SMLMV (ej. 3.5). No dividir por SMLMV en pesos.
    if "salario_promedio" in ag.columns:
        ag["salario_promedio_smlmv"] = ag["salario_promedio"]
        smlmv = float(get_smlmv_sesion())
        ag["salario_proyectado_pesos_hoy"] = ag["salario_promedio"] * smlmv if smlmv else np.nan
    else:
        ag["salario_promedio_smlmv"] = np.nan
        ag["salario_proyectado_pesos_hoy"] = np.nan

    # Reemplazar inf por nan
    ag = ag.replace([np.inf, -np.inf], np.nan)

    # Bloque E: scoring
    ag = apply_scoring(ag)

    return ag


def run_fase4() -> pd.DataFrame | None:
    """
    Fase 4: Agregación por CATEGORIA_FINAL y scoring ponderado.
    Agrupa por categoría, genera bloques A–D y aplica apply_scoring (bloque E).
    Retorna el DataFrame agregado para uso en Fase 5.
    """
    log_etapa_iniciada("Fase 4: Agregación por categoría")
    out_path = CHECKPOINT_BASE_MAESTRA.parent / "agregado_categorias.parquet"
    sabana_path = CHECKPOINT_BASE_MAESTRA.parent / "sabana_consolidada.parquet"

    if not sabana_path.exists():
        log_error("No existe sábana consolidada. Ejecutar Fase 3 antes.")
        return None
    df = pd.read_parquet(sabana_path)
    SCHEMA_VERSION = "v3"
    if "schema_version" in df.columns:
        sv = str(df["schema_version"].iloc[0]) if len(df) else ""
        if sv and sv != SCHEMA_VERSION:
            log_warning(
                f"[Fase 4] ALERTA: sabana_consolidada.parquet tiene schema_version='{sv}' "
                f"pero se esperaba '{SCHEMA_VERSION}'. Elimine 'sabana_consolidada.parquet' y re-ejecute Fase 3."
            )
    else:
        log_warning(
            f"[Fase 4] ALERTA: sabana_consolidada.parquet no tiene schema_version. "
            "Si ve columnas mezcladas, elimine el parquet y re-ejecute Fase 3."
        )
    if "CATEGORIA_FINAL" not in df.columns:
        log_error("Sábana sin columna CATEGORIA_FINAL.")
        return None

    smlmv_actual = get_smlmv_sesion()
    log_info(f"SMLMV usado en scoring: ${smlmv_actual:,.0f}")

    ag = run_fase4_desde_sabana(df)
    if ag is None:
        log_error("Fase 4: la agregación no produjo datos.")
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    ag.to_parquet(out_path, index=False)
    log_info(f"Agregado por categoría guardado: {out_path} ({len(ag)} filas)")
    log_etapa_completada("Fase 4: Agregación por categoría", f"{len(ag)} categorías")
    return ag


# Bloques para hoja "total" (encabezado fila 1)
_BLOQUES_TOTAL = [
    ("MATRÍCULAS SEMESTRAL", [
        "suma_matricula_2019_1", "suma_matricula_2019_2",
        "suma_matricula_2020_1", "suma_matricula_2020_2",
        "suma_matricula_2021_1", "suma_matricula_2021_2",
        "suma_matricula_2022_1", "suma_matricula_2022_2",
        "suma_matricula_2023_1", "suma_matricula_2023_2",
        "suma_matricula_2024_1", "suma_matricula_2024_2",
    ]),
    ("MATRÍCULAS", [
        "suma_matricula_2019", "suma_matricula_2020", "suma_matricula_2021", "suma_matricula_2022", "suma_matricula_2023", "suma_matricula_2024",
        "prom_matricula_2019", "prom_matricula_2020", "prom_matricula_2021", "prom_matricula_2022", "prom_matricula_2023", "prom_matricula_2024",
        "var_suma_2020", "var_suma_2021", "var_suma_2022", "var_suma_2023", "var_suma_2024",
        "var_prom_2020", "var_prom_2021", "var_prom_2022", "var_prom_2023", "var_prom_2024",
        "participacion_2019", "participacion_2024", "AAGR_suma", "CAGR_suma", "AAGR_prom",
    ]),
    ("OLE", [
        "salario_promedio", "salario_proyectado_pesos_hoy", "inscritos_2023_suma", "inscritos_2024_suma", "inscritos_2023_prom", "inscritos_2024_prom",
        "pct_no_matriculados_2023", "pct_no_matriculados_2024", "var_inscritos",
    ]),
    ("OFERTA", [
        "num_programas_2019", "num_programas_2024", "programas_activos", "programas_inactivos", "programas_nuevos_3a",
        "var_programas", "pct_con_matricula", "prom_matricula_por_programa_2024",
    ]),
    ("COSTOS", ["costo_promedio", "distancia_costo_pct"]),
    ("SCORING", [
        "score_matricula", "score_participacion", "score_AAGR", "score_salario", "score_pct_no_matriculados",
        "score_num_programas", "score_costo", "calificacion_final",
    ]),
]

COL_ANCHOS_PROGRAMAS = {
    "CÓDIGO_SNIES_DEL_PROGRAMA": 18,
    "NOMBRE_DEL_PROGRAMA": 50,
    "NOMBRE_INSTITUCIÓN": 40,
}

VERDE = "C6EFCE"
AMARILLO = "FFEB9C"
ROJO = "FFC7CE"


def run_fase5(agregado_df: pd.DataFrame | None) -> None:
    """
    Fase 5: Exportación formateada a Estudio_Mercado_Colombia.xlsx.
    Hoja programas_detalle: sábana Fase 3 (freeze, filtros, anchos).
    Hoja total: agregado Fase 4 con dos filas de encabezado y formato por bloque.
    """
    from etl.config import ARCHIVO_ESTUDIO_MERCADO

    log_etapa_iniciada("Fase 5: Exportación formateada")
    sabana_path = CHECKPOINT_BASE_MAESTRA.parent / "sabana_consolidada.parquet"
    if not sabana_path.exists():
        log_error("No existe sábana consolidada. Ejecutar Fase 3 antes.")
        return
    sabana = pd.read_parquet(sabana_path)
    if agregado_df is None or len(agregado_df) == 0:
        log_error("No hay DataFrame agregado. Ejecutar Fase 4 antes.")
        return

    out_path = ARCHIVO_ESTUDIO_MERCADO
    out_path.parent.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            # Merge incremental: mantiene histórico, respeta manuales y guarda snapshot antes de modificar
            try:
                from etl.merge_incremental import ESTUDIO_PATH, merge_incremental

                merged = merge_incremental(nuevo=sabana, nuevo_total=agregado_df)
                sabana_final = merged.get("programas_detalle") if isinstance(merged, dict) else sabana
                total_final = merged.get("total") if isinstance(merged, dict) else agregado_df
                if sabana_final is None or len(sabana_final) == 0:
                    sabana_final = sabana
                if total_final is None or len(total_final) == 0:
                    total_final = agregado_df
                out_path = ESTUDIO_PATH
            except Exception as e:
                log_warning(f"[Fase 5] Merge incremental falló ({e}). Exportando modo clásico (sobrescritura).")
                sabana_final = sabana
                total_final = agregado_df

            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                sabana_final.to_excel(writer, sheet_name="programas_detalle", index=False)
                col_order = _escribir_hoja_total(writer, total_final)
                wb = writer.book
                ws_detalle = wb["programas_detalle"]
                ws_detalle.freeze_panes = "A2"
                ws_detalle.auto_filter.ref = ws_detalle.dimensions
                from openpyxl.utils import get_column_letter
                for col_name, width in COL_ANCHOS_PROGRAMAS.items():
                    if col_name in sabana_final.columns:
                        idx = list(sabana_final.columns).index(col_name) + 1
                        ws_detalle.column_dimensions[get_column_letter(idx)].width = width
                _aplicar_formato_total(wb["total"], col_order)

                # Hoja informativa: programas con baja confianza del ML (REQUIERE_REVISION == True)
                if "REQUIERE_REVISION" in sabana_final.columns:
                    df_revision = sabana_final[sabana_final["REQUIERE_REVISION"] == True].copy()  # noqa: E712
                else:
                    df_revision = pd.DataFrame()

                if df_revision is not None and len(df_revision) > 0:
                    cols_revision = [
                        "CÓDIGO_SNIES_DEL_PROGRAMA",
                        "NOMBRE_DEL_PROGRAMA",
                        "NIVEL_DE_FORMACIÓN",
                        "ÁREA_DE_CONOCIMIENTO",
                        "CINE_F_2013_AC_CAMPO_DETALLADO",
                        "CATEGORIA_FINAL",
                        "PROBABILIDAD",
                        "NOMBRE_INSTITUCIÓN",
                        "DEPARTAMENTO_OFERTA_PROGRAMA",
                        "ESTADO_PROGRAMA",
                    ]
                    cols_existentes = [c for c in cols_revision if c in df_revision.columns]
                    df_revision_export = df_revision[cols_existentes].copy()
                    if "PROBABILIDAD" in df_revision_export.columns:
                        df_revision_export = df_revision_export.sort_values("PROBABILIDAD")

                    df_revision_export.to_excel(writer, sheet_name="revision_requerida", index=False)
                    ws_rev = wb["revision_requerida"]
                    ws_rev.freeze_panes = "A2"
                    ws_rev.auto_filter.ref = ws_rev.dimensions

                    # Resaltar filas con probabilidad < 0.50 en amarillo
                    try:
                        from openpyxl.styles import PatternFill

                        yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
                        if "PROBABILIDAD" in df_revision_export.columns:
                            prob_col_idx = list(df_revision_export.columns).index("PROBABILIDAD") + 1
                            for r in range(2, ws_rev.max_row + 1):
                                v = ws_rev.cell(row=r, column=prob_col_idx).value
                                try:
                                    pv = float(v) if v is not None else 1.0
                                except (TypeError, ValueError):
                                    pv = 1.0
                                if pv < 0.50:
                                    for c in range(1, ws_rev.max_column + 1):
                                        ws_rev.cell(row=r, column=c).fill = yellow
                    except Exception:
                        pass

                    log_info(
                        "Hoja 'revision_requerida' exportada: "
                        f"{len(df_revision_export):,} programas con confianza ML < 0.70."
                    )
                else:
                    log_info("No hay programas que requieran revisión (todos >= 0.70 de confianza).")
            break
        except PermissionError:
            try:
                from tkinter import messagebox
                reintentar = messagebox.askretrycancel(
                    "Archivo en uso",
                    "El archivo Estudio_Mercado_Colombia.xlsx está abierto en otro programa (por ejemplo Excel).\n\n"
                    "Cierre el archivo y presione 'Reintentar' para guardar, o 'Cancelar' para abortar la exportación.",
                    parent=None,
                )
            except Exception:
                reintentar = False
            if not reintentar:
                log_error("Exportación cancelada: el usuario eligió no reintentar (archivo Excel en uso).")
                raise RuntimeError(
                    "Exportación cancelada por el usuario: cierre Estudio_Mercado_Colombia.xlsx e intente de nuevo."
                ) from None

    log_info(f"Exportado: {out_path}")
    log_etapa_completada("Fase 5: Exportación formateada", str(out_path))


def _escribir_hoja_total(writer: pd.ExcelWriter, ag: pd.DataFrame) -> list[str]:
    """Escribe la hoja 'total' con fila 1 = bloques, fila 2 = nombres de columnas. Retorna orden de columnas."""
    wb = writer.book
    ws = wb.create_sheet("total", 1)
    col_order = ["CATEGORIA_FINAL"]
    col_idx = 1
    ws.cell(row=1, column=1, value="CATEGORIA_FINAL")
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=1)
    col_idx = 2
    for block_name, cols in _BLOQUES_TOTAL:
        present = [c for c in cols if c in ag.columns]
        if not present:
            continue
        start = col_idx
        for c in present:
            ws.cell(row=2, column=col_idx, value=c)
            col_order.append(c)
            col_idx += 1
        ws.cell(row=1, column=start, value=block_name)
        ws.merge_cells(start_row=1, start_column=start, end_row=1, end_column=col_idx - 1)
    for r_idx, row in enumerate(ag.itertuples(index=False), start=3):
        ws.cell(row=r_idx, column=1, value=getattr(row, "CATEGORIA_FINAL", ""))
        c_idx = 2
        for _, cols in _BLOQUES_TOTAL:
            for c in cols:
                if c in ag.columns:
                    val = getattr(row, c, None)
                    ws.cell(row=r_idx, column=c_idx, value=val)
                    c_idx += 1
    return col_order


def _aplicar_formato_total(ws, col_order: list[str]) -> None:
    """Aplica formatos de número y color de fila según calificacion_final. col_order es el orden de columnas en la hoja."""
    from openpyxl.styles import PatternFill
    pct_fmt = "0.0%"
    moneda_fmt = "#,##0"
    score_fmt = "0"
    calif_fmt = "0.00"
    col_calif = None
    for j, col in enumerate(col_order):
        if col == "calificacion_final":
            col_calif = j
            break
    for r in range(3, ws.max_row + 1):
        calif = ws.cell(row=r, column=col_calif + 1).value if col_calif is not None else None
        try:
            calif_f = float(calif) if calif is not None else 0.0
        except (TypeError, ValueError):
            calif_f = 0.0
        if calif_f >= 4.0:
            fill = PatternFill(start_color=VERDE, end_color=VERDE, fill_type="solid")
        elif calif_f >= 3.0:
            fill = PatternFill(start_color=AMARILLO, end_color=AMARILLO, fill_type="solid")
        else:
            fill = PatternFill(start_color=ROJO, end_color=ROJO, fill_type="solid")
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(row=r, column=c)
            cell.fill = fill
            if c == 1:
                continue
            col_name = col_order[c - 1] if c - 1 < len(col_order) else None
            if col_name and ("participacion" in col_name or "pct_" in col_name or "var_" in col_name or "AAGR" in col_name or "CAGR" in col_name):
                cell.number_format = pct_fmt
            elif col_name and ("costo" in col_name or "salario" in col_name):
                cell.number_format = moneda_fmt
            elif col_name and col_name.startswith("score_"):
                cell.number_format = score_fmt
            elif col_name == "calificacion_final":
                cell.number_format = calif_fmt


def run_pipeline(
    ask_reuse_checkpoint: Callable[[str], bool] | None = None,
) -> None:
    """
    Orquesta el pipeline completo de estudio de mercado.
    Opcionalmente pregunta si reusar base_maestra y/o sabana_consolidada.
    """
    import time
    from etl.config import ARCHIVO_ESTUDIO_MERCADO

    t0 = time.perf_counter()
    log_etapa_iniciada("Pipeline estudio de mercado Colombia")

    def _ask(name: str) -> bool:
        if ask_reuse_checkpoint is not None:
            return ask_reuse_checkpoint(name)
        try:
            return input(f"¿Reusar checkpoint '{name}'? (s/n): ").strip().lower() == "s"
        except (EOFError, OSError):
            return False

    if not CHECKPOINT_BASE_MAESTRA.exists():
        run_fase1()
    else:
        if _ask("base_maestra.parquet"):
            log_info("Reusando base_maestra.parquet")
        else:
            run_fase1()

    run_fase2()

    sabana_path = CHECKPOINT_BASE_MAESTRA.parent / "sabana_consolidada.parquet"
    if not sabana_path.exists():
        run_fase3()
    else:
        if _ask("sabana_consolidada.parquet"):
            log_info("Reusando sabana_consolidada.parquet")
        else:
            run_fase3()

    ag = run_fase4()
    run_fase5(ag)

    elapsed = time.perf_counter() - t0
    log_resultado(f"Tiempo total: {elapsed:.1f}s")
    log_info(f"Salida: {ARCHIVO_ESTUDIO_MERCADO}")

    try:
        n_prog = len(pd.read_parquet(sabana_path)) if sabana_path.exists() else 0
    except Exception:
        n_prog = 0
    n_cat = len(ag) if ag is not None else 0
    verdes = (ag["calificacion_final"] >= 4.0).sum() if ag is not None and "calificacion_final" in ag.columns else 0
    amarillos = ((ag["calificacion_final"] >= 3.0) & (ag["calificacion_final"] < 4.0)).sum() if ag is not None and "calificacion_final" in ag.columns else 0
    rojos = (ag["calificacion_final"] < 3.0).sum() if ag is not None and "calificacion_final" in ag.columns else 0
    log_resultado(f"Programas: {n_prog}, Categorías: {n_cat}, Verde(>=4): {verdes}, Amarillo(>=3): {amarillos}, Rojo(<3): {rojos}")
    log_etapa_completada("Pipeline estudio de mercado Colombia", f"{elapsed:.1f}s")


def run_pipeline_mercado() -> None:
    """
    Ejecuta todas las fases del pipeline de mercado (sin preguntar checkpoints).
    """
    run_fase1()
    run_fase2()
    run_fase3()
    ag = run_fase4()
    run_fase5(ag)


if __name__ == "__main__":
    run_fase1()
