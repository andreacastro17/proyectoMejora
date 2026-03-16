"""
Scraper de matrículas e inscritos históricos SNIES (Fase 2 pipeline mercado).

Reutiliza el patrón de Selenium de etl/descargaSNIES.py.
URLs y selectores se leen desde etl/config.SNIES_URLS.
"""

from __future__ import annotations

from pathlib import Path
import time

import pandas as pd

from etl.config import RAW_HISTORIC_DIR, SNIES_URLS
from etl.pipeline_logger import log_info, log_warning


def _empty_matriculados() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS"])


def _empty_inscritos() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "INSCRITOS"])


class SNIESMatriculasScraper:
    """
    Scraper para descargar matrículas e inscritos del portal SNIES del MEN.

    TODO: Reemplazar SNIES_MATRICULAS_BASE_URL con la URL real del portal cuando esté disponible.
    La URL debe permitir filtrar por año y semestre y descargar en formato CSV o Excel.

    Mientras la URL esté pendiente, el scraper NO rompe el pipeline:
    retorna DataFrames vacíos con las columnas correctas y deja un log accionable.
    """

    # TODO: reemplazar con la URL real del portal SNIES de matrículas
    SNIES_MATRICULAS_BASE_URL = "PENDIENTE_URL_SNIES_MATRICULAS"

    # Tiempo máximo de espera por descarga (segundos)
    TIMEOUT_DESCARGA = 120

    # Pausa entre descargas para no saturar el servidor
    PAUSA_ENTRE_DESCARGAS = 2.0

    def __init__(self, raw_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or RAW_HISTORIC_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def download_matriculados(self, year: int, semestre: int) -> pd.DataFrame:
        """
        Obtiene matrículas para el año y semestre dados.
        Si el archivo ya existe en disco, lo carga sin descargar.
        Si la descarga falla, registra warning y retorna DataFrame vacío con columnas esperadas.
        """
        archivo = self.raw_dir / f"matriculados_{year}_{semestre}.csv"

        # Si el archivo ya existe y tiene datos, cargar desde disco
        if archivo.exists():
            try:
                df = pd.read_csv(archivo, dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str}, encoding="utf-8-sig")
                if {"CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS"}.issubset(df.columns):
                    log_info(f"[Fase 2] Matriculados {year}-{semestre}: cargado desde disco ({len(df):,} filas)")
                    return df
            except Exception as e:
                log_warning(f"[Fase 2] Error leyendo {archivo.name}: {e}. Re-descargando.")

        # Verificar que la URL esté configurada
        if self.SNIES_MATRICULAS_BASE_URL == "PENDIENTE_URL_SNIES_MATRICULAS":
            log_warning(
                f"[Fase 2] URL del SNIES no configurada. "
                f"Matrículas {year}-{semestre} no disponibles. "
                f"Actualizar SNIES_MATRICULAS_BASE_URL en SNIESMatriculasScraper."
            )
            return _empty_matriculados()

        # TODO: implementar la descarga real cuando la URL esté disponible.
        # La lógica dependerá del formato del portal (API REST, formulario web, descarga directa).
        log_warning(f"[Fase 2] Descarga de matriculados {year}-{semestre}: pendiente de implementación.")
        time.sleep(self.PAUSA_ENTRE_DESCARGAS)
        return _empty_matriculados()

    def download_inscritos(self, year: int, semestre: int) -> pd.DataFrame:
        """
        Obtiene inscritos para el año y semestre dados.
        Si el archivo ya existe en disco, lo carga sin descargar.
        Si la descarga falla, registra warning y retorna DataFrame vacío con columnas esperadas.
        """
        archivo = self.raw_dir / f"inscritos_{year}_{semestre}.csv"

        # Si el archivo ya existe y tiene datos, cargar desde disco
        if archivo.exists():
            try:
                df = pd.read_csv(archivo, dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str}, encoding="utf-8-sig")
                if {"CÓDIGO_SNIES_DEL_PROGRAMA", "INSCRITOS"}.issubset(df.columns):
                    log_info(f"[Fase 2] Inscritos {year}-{semestre}: cargado desde disco ({len(df):,} filas)")
                    return df
            except Exception as e:
                log_warning(f"[Fase 2] Error leyendo {archivo.name}: {e}. Re-descargando.")

        # Verificar que la URL esté configurada
        if self.SNIES_MATRICULAS_BASE_URL == "PENDIENTE_URL_SNIES_MATRICULAS":
            log_warning(
                f"[Fase 2] URL del SNIES no configurada. "
                f"Inscritos {year}-{semestre} no disponibles. "
                f"Actualizar SNIES_MATRICULAS_BASE_URL en SNIESMatriculasScraper."
            )
            return _empty_inscritos()

        # TODO: implementar la descarga real cuando la URL esté disponible.
        log_warning(f"[Fase 2] Descarga de inscritos {year}-{semestre}: pendiente de implementación.")
        time.sleep(self.PAUSA_ENTRE_DESCARGAS)
        return _empty_inscritos()
