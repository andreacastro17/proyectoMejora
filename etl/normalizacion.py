"""
Herramientas para normalizar columnas de texto en la hoja `Programas`
del archivo Programas.xlsx.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from unidecode import unidecode

from etl.pipeline_logger import log_error, log_info
from etl.config import ARCHIVO_PROGRAMAS, HOJA_PROGRAMAS
COLUMNAS_A_NORMALIZAR = [
    "NOMBRE_DEL_PROGRAMA",
    "NOMBRE_INSTITUCIÓN",
    "ESTADO_PROGRAMA",
    "CINE_F_2013_AC_CAMPO_AMPLIO",
    "CINE_F_2013_AC_CAMPO_ESPECÍFIC",
    "CINE_F_2013_AC_CAMPO_DETALLADO",
    "ÁREA_DE_CONOCIMIENTO",
    "NÚCLEO_BÁSICO_DEL_CONOCIMIENTO",
    "NIVEL_ACADÉMICO",
    "NIVEL_DE_FORMACIÓN",
    "DEPARTAMENTO_OFERTA_PROGRAMA",
    "MUNICIPIO_OFERTA_PROGRAMA",
]


def limpiar_texto(valor: object) -> object:
    """Limpia texto eliminando tildes, signos y espacios extra."""
    if pd.isna(valor):
        return valor

    texto = str(valor)
    texto = unidecode(texto)
    texto = texto.lower()
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto


def normalizar_programas() -> None:
    """Normaliza las columnas configuradas en la hoja Programas."""
    if not ARCHIVO_PROGRAMAS.exists():
        error_msg = f"No se encontró el archivo: {ARCHIVO_PROGRAMAS}"
        log_error(error_msg)
        raise FileNotFoundError(error_msg)

    df = pd.read_excel(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
    log_info(f"Archivo cargado: {ARCHIVO_PROGRAMAS.name} ({len(df)} filas)")

    columnas_faltantes = [col for col in COLUMNAS_A_NORMALIZAR if col not in df.columns]
    if columnas_faltantes:
        warning_msg = (
            f"No se encontraron las siguientes columnas en la hoja "
            f"`{HOJA_PROGRAMAS}`: {', '.join(columnas_faltantes)}"
        )
        print(f"Advertencia: {warning_msg}")
        log_info(f"Advertencia: {warning_msg}")

    columnas_normalizadas = 0
    for columna in COLUMNAS_A_NORMALIZAR:
        if columna in df.columns:
            df[columna] = df[columna].apply(limpiar_texto)
            columnas_normalizadas += 1

    log_info(f"Columnas normalizadas: {columnas_normalizadas}")

    with pd.ExcelWriter(
        ARCHIVO_PROGRAMAS,
        mode="a",
        if_sheet_exists="replace",
        engine="openpyxl",
    ) as writer:
        df.to_excel(writer, sheet_name=HOJA_PROGRAMAS, index=False)
    
    log_info(f"Archivo normalizado guardado: {ARCHIVO_PROGRAMAS.name}")


if __name__ == "__main__":
    normalizar_programas()
