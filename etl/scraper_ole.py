"""
Scraper de indicadores OLE (Fase 2 pipeline mercado).

Reutiliza el patrón de Selenium de etl/descargaSNIES.py.
URLs y selectores se leen desde etl/config.OLE_URLS.
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from etl.config import OLE_URLS, RAW_HISTORIC_DIR
from etl.pipeline_logger import log_info, log_warning

OLE_CACHE_DAYS = 7
OLE_FILENAME = "ole_indicadores.csv"


def _empty_ole() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "TASA_COTIZANTES", "SALARIO_OLE"]
    )


class OLEScraper:
    """
    Descarga indicadores OLE (tasa cotizantes, salario) para una lista de códigos SNIES.
    Usa cache por fecha de modificación: si el archivo existe y tiene menos de 7 días, se carga desde disco.
    """

    def __init__(self, raw_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or RAW_HISTORIC_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.raw_dir / OLE_FILENAME

    def download_indicadores(self, snies_list: list) -> pd.DataFrame:
        """
        Obtiene indicadores OLE para la lista de códigos SNIES.
        Si el archivo existe y tiene menos de 7 días, lo carga sin descargar.
        Si la descarga falla, registra warning y retorna DataFrame vacío con columnas esperadas.
        """
        if self.path.exists():
            try:
                mtime = self.path.stat().st_mtime
                age_days = (time.time() - mtime) / 86400
                if age_days < OLE_CACHE_DAYS:
                    df = pd.read_csv(self.path, encoding="utf-8-sig")
                    if "CÓDIGO_SNIES_DEL_PROGRAMA" in df.columns:
                        log_info(f"OLE: cargado desde cache ({self.path.name}, < {OLE_CACHE_DAYS} días)")
                        return df
            except Exception as e:
                log_warning(f"Error al leer {self.path.name}: {e}. Se reintentará descarga.")
        try:
            # TODO: confirmar URL y selectores con cliente (etl/config.OLE_URLS).
            # Al implementar: usar Selenium, guardar df en self.path y return df.
            raise NotImplementedError("# TODO: confirmar URL y selectores con cliente")
        except NotImplementedError:
            log_warning("Descarga OLE no implementada (URL/selectores pendientes). Retornando DataFrame vacío.")
            return _empty_ole()
        except Exception as e:
            log_warning(f"Descarga OLE falló: {e}. Retornando DataFrame vacío.")
            return _empty_ole()
