"""
Scraper de matrículas SNIES (Fase 2 pipeline mercado).

Lectura local desde ref/backup/matriculas/. El usuario descarga manualmente
los Excels de matriculados y los coloca en esa carpeta. Inscritos no se procesan
desde Excel; la Fase 3 imputa desde el referente (INSCRITOS_2023, INSCRITOS_2024).
"""

from __future__ import annotations

from pathlib import Path
import unicodedata

import pandas as pd

from etl.config import RAW_HISTORIC_DIR, REF_DIR
from etl.pipeline_logger import log_info, log_warning


def _empty_matriculados() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS", "SEMESTRE"])


def _empty_inscritos() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "INSCRITOS", "SEMESTRE"])


class SNIESMatriculasScraper:
    """
    Lee Excels de matrículas desde ref/backup/matriculas/ (descarga manual del usuario).
    Detecta dinámicamente la fila de encabezados (palabra 'snies' en las primeras 20 filas),
    divide por semestre y guarda CSVs en raw (outputs/historico/raw/).
    Inscritos no se leen desde Excel; download_inscritos retorna DataFrame vacío.
    """

    def __init__(self, raw_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or RAW_HISTORIC_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.manual_dir = REF_DIR / "backup" / "matriculas"

    @staticmethod
    def _norm_col_name(name: str) -> str:
        s = str(name).replace("\n", " ").replace("\r", " ").strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = " ".join(s.split())
        return s.upper()

    @classmethod
    def _normalize_cached_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or len(df.columns) == 0:
            return df
        rename_map = {}
        for c in df.columns:
            cn = cls._norm_col_name(c)
            if "SNIES" in cn and "PROGRAMA" in cn:
                rename_map[c] = "CÓDIGO_SNIES_DEL_PROGRAMA"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _load_cached(self, path: Path, required_cols: set[str]) -> pd.DataFrame | None:
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path, dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str}, encoding="utf-8-sig")
            df = self._normalize_cached_columns(df)
            if required_cols.issubset(set(df.columns)):
                return df
        except Exception as e:
            log_warning(f"[Fase 2] Error leyendo {path.name}: {e}.")
        return None

    def _find_manual_excel_for_year(self, year: int) -> Path | None:
        """Busca en self.manual_dir un archivo cuyo nombre contenga el año (ej. 2019)."""
        if not self.manual_dir.exists():
            return None
        year_str = str(year)
        for ext in ("*.xlsx", "*.xls"):
            for f in self.manual_dir.glob(ext):
                if year_str in f.name:
                    return f
        return None

    def _read_excel_local_dynamic_header(self, filepath: Path) -> pd.DataFrame:
        """
        Lee Excel SNIES con detección robusta de hoja y fila de encabezado.

        Estrategia hoja:
          - Intenta primero la hoja '1.' (presente en 2023 y 2024)
          - Si no existe, usa la última hoja (robustez para formatos futuros)
          - Si falla todo, usa sheet[0]

        Estrategia header:
          - Busca una CELDA INDIVIDUAL cuyo valor normalizado contenga
            'snies' Y 'programa' y sea corto (< 80 chars), para evitar
            falsos positivos en filas de notas/texto libre.
        """
        xl = pd.ExcelFile(filepath)
        sheet_names = xl.sheet_names

        candidates = []
        if "1." in sheet_names:
            candidates.append("1.")
        for s in reversed(sheet_names):
            if s not in candidates:
                candidates.append(s)

        last_error = None
        for sheet in candidates:
            try:
                df = pd.read_excel(filepath, sheet_name=sheet, header=None)
            except Exception as e:
                last_error = e
                continue

            if df is None or len(df) < 5:
                continue

            header_idx = None
            for idx in range(min(25, len(df))):
                row = df.iloc[idx]
                for cell_val in row:
                    if pd.isna(cell_val):
                        continue
                    cell_str = (
                        str(cell_val)
                        .replace("\n", " ")
                        .strip()
                        .lower()
                    )
                    if "snies" in cell_str and "program" in cell_str and len(cell_str) < 80:
                        header_idx = idx
                        break
                if header_idx is not None:
                    break

            if header_idx is None:
                continue

            df.columns = df.iloc[header_idx]
            df = df.iloc[header_idx + 1 :].reset_index(drop=True)
            log_info(
                f"[Fase 2] {filepath.name}: hoja='{sheet}', "
                f"header en fila {header_idx}, {len(df):,} filas de datos"
            )
            return df

        raise ValueError(
            f"No se encontró hoja con datos SNIES en {filepath.name}. "
            f"Hojas disponibles: {sheet_names}. Último error: {last_error}"
        )

    @staticmethod
    def _normalize_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
        cols = [str(c) for c in df.columns]
        cols_norm = {c: str(c).strip().replace("\n", " ").replace("\r", " ").lower() for c in cols}

        col_snies = None
        for c, cn in cols_norm.items():
            if "snies" in cn and "program" in cn:
                col_snies = c
                break
        if col_snies is None:
            for c, cn in cols_norm.items():
                if "snies" in cn:
                    col_snies = c
                    break
        col_sem = None
        for c, cn in cols_norm.items():
            if "semestre" in cn:
                col_sem = c
                break

        if not col_snies:
            raise ValueError(f"No se pudo detectar columna SNIES. Columnas: {list(df.columns)[:15]}")

        if not col_sem:
            df["SEMESTRE"] = 1
            col_sem = "SEMESTRE"

        df = df.rename(columns={col_snies: "CÓDIGO_SNIES_DEL_PROGRAMA", col_sem: "SEMESTRE"})
        df["CÓDIGO_SNIES_DEL_PROGRAMA"] = (
            df["CÓDIGO_SNIES_DEL_PROGRAMA"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        df["SEMESTRE"] = pd.to_numeric(df["SEMESTRE"], errors="coerce").astype("Int64")

        for c in list(df.columns):
            cn = str(c).strip().replace("\n", " ").replace("\r", " ").lower()
            if "matriculad" in cn:
                df = df.rename(columns={c: "MATRICULADOS"})
                break
        for c in list(df.columns):
            cn = str(c).strip().replace("\n", " ").replace("\r", " ").lower()
            if "inscrit" in cn:
                df = df.rename(columns={c: "INSCRITOS"})
                break

        return df, "CÓDIGO_SNIES_DEL_PROGRAMA", "SEMESTRE"

    def _split_y_cache_por_semestre(
        self,
        df: pd.DataFrame,
        year: int,
        prefix: str,
        value_col_candidates: list[str],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        df, _, _ = self._normalize_cols(df)

        value_col = None
        cols_norm = {c: str(c).strip().replace("\n", " ").replace("\r", " ").lower() for c in df.columns}
        for cand in value_col_candidates:
            cand_l = cand.lower()
            for c, cn in cols_norm.items():
                if cand_l in cn:
                    value_col = c
                    break
            if value_col:
                break
        if not value_col:
            numeric_cols = [c for c in df.columns if c not in ("CÓDIGO_SNIES_DEL_PROGRAMA", "SEMESTRE")]
            for c in numeric_cols:
                try:
                    s = pd.to_numeric(df[c], errors="coerce")
                    if s.notna().sum() > 0:
                        value_col = c
                        break
                except Exception:
                    continue
        if not value_col:
            raise ValueError("No se pudo detectar columna de valor (matriculados/inscritos).")

        out_value_col = "MATRICULADOS" if prefix == "matriculados" else "INSCRITOS"
        df[out_value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0).astype(int)
        df_out = df[["CÓDIGO_SNIES_DEL_PROGRAMA", out_value_col, "SEMESTRE"]].copy()

        df_1 = df_out[df_out["SEMESTRE"] == 1].copy()
        df_2 = df_out[df_out["SEMESTRE"] == 2].copy()

        p1 = self.raw_dir / f"{prefix}_{year}_1.csv"
        p2 = self.raw_dir / f"{prefix}_{year}_2.csv"
        df_1.to_csv(p1, index=False, encoding="utf-8-sig")
        df_2.to_csv(p2, index=False, encoding="utf-8-sig")
        log_info(f"[Fase 2] {prefix} {year}: guardados {p1.name} ({len(df_1):,}) y {p2.name} ({len(df_2):,})")

        return df_1, df_2

    def download_matriculados(self, year: int, semestre: int) -> pd.DataFrame:
        """
        Lee matrículas desde ref/backup/matriculas/ (archivo cuyo nombre contenga el año).
        Si ya existen los CSVs en raw, los carga desde disco.
        Lectura dinámica: header=None y búsqueda de fila con 'snies' en las primeras 20 filas.
        """
        archivo = self.raw_dir / f"matriculados_{year}_{semestre}.csv"
        cached = self._load_cached(archivo, {"CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS"})
        if cached is not None:
            # Si el Excel fuente es más nuevo, invalidar caché y reconstruir ambos semestres
            try:
                filepath = self._find_manual_excel_for_year(year)
                if filepath is not None:
                    csv_mtime = archivo.stat().st_mtime
                    xlsx_mtime = filepath.stat().st_mtime
                    if xlsx_mtime > csv_mtime:
                        log_info(
                            f"[Fase 2] Excel {filepath.name} fue modificado después del CSV en caché. "
                            f"Reconstruyendo CSVs para {year}..."
                        )
                        for sem in (1, 2):
                            old = self.raw_dir / f"matriculados_{year}_{sem}.csv"
                            if old.exists():
                                old.unlink()
                        cached = None
            except Exception:
                pass
            if cached is not None:
                log_info(f"[Fase 2] Matriculados {year}-{semestre}: cargado desde disco ({len(cached):,} filas)")
                return cached

        filepath = self._find_manual_excel_for_year(year)
        if filepath is None:
            log_warning(
                f"[Fase 2] Matriculados {year}: no hay archivo en {self.manual_dir} con el año en el nombre. "
                "Coloque el Excel de matriculados (ej. matriculados_2019.xlsx) en ref/backup/matriculas/."
            )
            return _empty_matriculados()

        try:
            df_raw = self._read_excel_local_dynamic_header(filepath)
            if df_raw is None or len(df_raw) == 0:
                return _empty_matriculados()
            df_1, df_2 = self._split_y_cache_por_semestre(
                df_raw,
                year=year,
                prefix="matriculados",
                value_col_candidates=["MATRICULADOS", "MATRICULA"],
            )
            return df_1 if int(semestre) == 1 else df_2
        except Exception as e:
            log_warning(f"[Fase 2] Matriculados {year}-{semestre}: {e}. Continuando con vacío.")
            return _empty_matriculados()

    def download_inscritos(self, year: int, semestre: int) -> pd.DataFrame:
        """
        No se procesan inscritos desde Excels. Retorna DataFrame vacío con columnas esperadas.
        La Fase 3 imputa inscritos_2023 e inscritos_2024 desde el referente (INSCRITOS_2023, INSCRITOS_2024).
        """
        return _empty_inscritos()
