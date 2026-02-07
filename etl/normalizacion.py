"""
Herramientas para normalizar columnas de texto en la hoja `Programas`
del archivo Programas.xlsx.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from unidecode import unidecode

from etl.pipeline_logger import log_error, log_info, log_warning
from etl.config import ARCHIVO_PROGRAMAS, HOJA_PROGRAMAS
from etl.exceptions_helpers import (
    leer_excel_con_reintentos,
    escribir_excel_con_reintentos,
    validar_excel_basico,
    explicar_error_archivo_abierto,
)
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


def normalizar_programas(df: pd.DataFrame | None = None, archivo: Path | None = None) -> pd.DataFrame:
    """
    Normaliza las columnas configuradas en la hoja Programas.
    
    Args:
        df: DataFrame opcional. Si se proporciona, se normaliza en memoria sin leer/escribir archivo.
        archivo: Archivo opcional. Si df es None, se lee desde este archivo (o ARCHIVO_PROGRAMAS por defecto).
        
    Returns:
        DataFrame normalizado
        
    Si df es None, lee desde archivo y escribe de vuelta.
    Si df se proporciona, solo normaliza y retorna (sin I/O).
    """
    # Si se proporciona DataFrame, trabajar en memoria
    if df is not None:
        df = df.copy()
        log_info(f"Normalizando DataFrame en memoria ({len(df)} filas)")
    else:
        # Modo tradicional: leer desde archivo
        archivo = archivo or ARCHIVO_PROGRAMAS
        if not archivo.exists():
            error_msg = f"No se encontró el archivo: {archivo}"
            log_error(error_msg)
            raise FileNotFoundError(error_msg)

        # Validar que el archivo sea un Excel válido
        es_valido, msg_error = validar_excel_basico(archivo)
        if not es_valido:
            log_error(f"Validación fallida: {msg_error}")
            raise ValueError(msg_error)

        # Leer con manejo robusto de errores
        try:
            df = leer_excel_con_reintentos(archivo, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo cargado: {archivo.name} ({len(df)} filas)")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(archivo, "leer")
            log_error(error_msg)
            raise PermissionError(error_msg) from e

    columnas_faltantes = [col for col in COLUMNAS_A_NORMALIZAR if col not in df.columns]
    if columnas_faltantes:
        warning_msg = (
            f"No se encontraron las siguientes columnas en la hoja "
            f"`{HOJA_PROGRAMAS}`: {', '.join(columnas_faltantes)}"
        )
        print(f"Advertencia: {warning_msg}")
        log_info(f"Advertencia: {warning_msg}")

    columnas_normalizadas = 0
    # OPTIMIZACIÓN: Usar operaciones vectorizadas en lugar de .apply() donde sea posible
    for columna in COLUMNAS_A_NORMALIZAR:
        if columna in df.columns:
            try:
                # Optimización: operaciones vectorizadas directas
                s = df[columna].fillna("").astype(str)
                # Aplicar unidecode de forma más eficiente (batch processing)
                # Para datasets grandes, procesar en chunks es más eficiente que .apply()
                if len(s) > 100:  # Solo para datasets grandes
                    # Procesar en chunks para mejor rendimiento
                    chunks = [s.iloc[i:i+100] for i in range(0, len(s), 100)]
                    s_normalized = pd.concat([
                        pd.Series([unidecode(str(x)) if x else "" for x in chunk], index=chunk.index)
                        for chunk in chunks
                    ])
                    s = s_normalized
                else:
                    # Para datasets pequeños, usar map es aceptable
                    s = s.map(lambda x: unidecode(x) if x else "")
                # Aplicar normalizaciones vectorizadas después de unidecode
                s = s.str.lower().str.replace(r"[^a-z0-9\s]", " ", regex=True)
                s = s.str.replace(r"\s+", " ", regex=True).str.strip()
                df[columna] = s
                columnas_normalizadas += 1
            except Exception as e:
                log_warning(f"Error al normalizar columna '{columna}': {e}. Continuando con las demás.")
                continue

    log_info(f"Columnas normalizadas: {columnas_normalizadas}")

    # Si se proporcionó df, solo retornar (sin escribir)
    if df is not None and archivo is None:
        return df

    # Modo tradicional: escribir de vuelta al archivo
    archivo = archivo or ARCHIVO_PROGRAMAS
    try:
        escribir_excel_con_reintentos(archivo, df, sheet_name=HOJA_PROGRAMAS)
        log_info(f"Archivo normalizado guardado: {archivo.name}")
    except PermissionError as e:
        error_msg = explicar_error_archivo_abierto(archivo, "escribir")
        log_error(error_msg)
        raise PermissionError(error_msg) from e
    
    return df


if __name__ == "__main__":
    normalizar_programas()
