"""
Scraper de indicadores OLE (Fase 2 pipeline mercado).

Refactor: reemplaza descargas web por lectura de archivo local estandarizado en ref/backup.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.config import RAW_HISTORIC_DIR, REF_DIR
from etl.pipeline_logger import log_info, log_warning

OLE_FILENAME = "ole_indicadores.csv"


def _empty_ole() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "TASA_COTIZANTES", "SALARIO_OLE"]
    )


class OLEScraper:
    """
    Adaptador local para indicadores OLE (tasa cotizantes, salario).

    Lee desde ref/backup/ole_indicadores.(csv|xlsx), normaliza cabeceras y guarda una copia limpia en
    outputs/historico/raw/ole_indicadores.csv para consumo por Fase 3.
    """

    def __init__(self, raw_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or RAW_HISTORIC_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.raw_dir / OLE_FILENAME

    @staticmethod
    def _clean_header(name: str) -> str:
        return (
            str(name)
            .replace("\n", " ")
            .replace("\r", " ")
            .strip()
        )

    def _load_backup(self) -> pd.DataFrame | None:
        candidates = [
            REF_DIR / "backup" / "ole_indicadores.csv",
            REF_DIR / "backup" / "ole_indicadores.xlsx",
            REF_DIR / "ole_indicadores.csv",
            REF_DIR / "ole_indicadores.xlsx",
        ]
        for p in candidates:
            if not p.exists():
                continue
            try:
                if p.suffix.lower() == ".csv":
                    return pd.read_csv(p, encoding="utf-8-sig")
                if p.suffix.lower() in (".xlsx", ".xls"):
                    return pd.read_excel(p)
            except Exception as e:
                log_warning(f"OLE: no se pudo leer {p.name}: {e}")
        return None

    @staticmethod
    def _normalize_ole_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza cabeceras sucias con saltos de línea y mapea forzosamente a:
          - CÓDIGO_SNIES_DEL_PROGRAMA
          - TASA_COTIZANTES
          - SALARIO_OLE
        """
        if df is None or len(df) == 0:
            return _empty_ole()

        df = df.copy()
        df.columns = [OLEScraper._clean_header(c) for c in df.columns]

        # Mapeo por subcadenas (tolerante a variaciones como "TASA COTIZANTES\n(0.0-1.0)")
        cols_norm = {c: OLEScraper._clean_header(c).lower() for c in df.columns}

        col_snies = None
        col_tasa = None
        col_sal = None

        for c, cn in cols_norm.items():
            if "snies" in cn and "program" in cn:
                col_snies = c
                break
        if col_snies is None:
            for c, cn in cols_norm.items():
                if "snies" in cn:
                    col_snies = c
                    break

        for c, cn in cols_norm.items():
            if "tasa" in cn and "cotiz" in cn:
                col_tasa = c
                break

        for c, cn in cols_norm.items():
            if "salario" in cn:
                col_sal = c
                break

        if not col_snies or not col_tasa or not col_sal:
            log_warning(
                "OLE: columnas no detectadas de forma confiable. "
                f"Encontradas: {list(df.columns)}"
            )
            return _empty_ole()

        out = df[[col_snies, col_tasa, col_sal]].rename(
            columns={
                col_snies: "CÓDIGO_SNIES_DEL_PROGRAMA",
                col_tasa: "TASA_COTIZANTES",
                col_sal: "SALARIO_OLE",
            }
        )
        out["CÓDIGO_SNIES_DEL_PROGRAMA"] = out["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.strip()
        out["TASA_COTIZANTES"] = pd.to_numeric(out["TASA_COTIZANTES"], errors="coerce")
        out["SALARIO_OLE"] = pd.to_numeric(out["SALARIO_OLE"], errors="coerce")

        # Si tasa parece venir en porcentaje (ej. 65.2), convertir a decimal
        tasa_max = out["TASA_COTIZANTES"].max(skipna=True)
        if pd.notna(tasa_max) and float(tasa_max) > 1.5:
            out["TASA_COTIZANTES"] = out["TASA_COTIZANTES"] / 100.0

        return out

    def download_indicadores(self, snies_list: list) -> pd.DataFrame:
        """
        Lee el archivo estático desde ref/backup, limpia cabeceras/columnas,
        guarda el CSV limpio en outputs/historico/raw/ole_indicadores.csv y retorna el DataFrame.

        Si no existe archivo de backup, retorna _empty_ole() y registra warning.
        """
        df_raw = self._load_backup()
        if df_raw is None or len(df_raw) == 0:
            log_warning("OLE: no existe archivo en ref/backup (ole_indicadores.csv/.xlsx). Retornando vacío.")
            return _empty_ole()

        try:
            df = self._normalize_ole_columns(df_raw)
            if df is None or len(df) == 0:
                log_warning("OLE: archivo leído pero no se pudo normalizar. Retornando vacío.")
                return _empty_ole()

            self.raw_dir.mkdir(parents=True, exist_ok=True)
            df.to_csv(self.path, index=False, encoding="utf-8-sig")
            log_info(f"OLE: exportado CSV limpio a {self.path.name} ({len(df):,} filas)")
            return df
        except Exception as e:
            log_warning(f"OLE: fallo normalizando/exportando: {e}. Retornando vacío.")
            return _empty_ole()
