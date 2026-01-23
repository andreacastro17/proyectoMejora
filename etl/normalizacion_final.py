"""
Script para aplicar normalización final de ortografía y formato al archivo Programas.xlsx
usando los mapeos definidos en normalizacionFinal.xlsx.

Cada hoja del archivo de normalización corresponde a una columna del archivo Programas,
donde la primera columna tiene el valor actual y la segunda columna tiene el valor normalizado.
La hoja "NOMBRE_INSTITUCIÓN" es especial: la primera columna es CÓDIGO_INSTITUCIÓN_PADRE.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Configurar sys.path para permitir ejecución directa del script
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.pipeline_logger import log_error, log_info
from etl.config import (
    ARCHIVO_NORMALIZACION,
    ARCHIVO_PROGRAMAS,
    HOJA_PROGRAMAS,
)
HOJA_ESPECIAL_INSTITUCION = "NOMBRE_INSTITUCIÓN"
COLUMNA_ID_INSTITUCION = "CÓDIGO_INSTITUCIÓN_PADRE"


def cargar_mapeos_normalizacion() -> dict[str, dict[str, str]]:
    """
    Carga los mapeos de normalización desde el archivo Excel.
    
    Returns:
        Diccionario donde las claves son nombres de columnas y los valores son
        diccionarios de mapeo {valor_actual: valor_normalizado}
    """
    if not ARCHIVO_NORMALIZACION.exists():
        error_msg = f"No se encontró el archivo de normalización: {ARCHIVO_NORMALIZACION}"
        log_error(error_msg)
        raise FileNotFoundError(error_msg)
    
    log_info(f"Cargando mapeos de normalización desde: {ARCHIVO_NORMALIZACION.name}")
    
    # Leer todas las hojas del archivo
    xls = pd.ExcelFile(ARCHIVO_NORMALIZACION)
    mapeos = {}
    
    for hoja in xls.sheet_names:
        df_hoja = pd.read_excel(ARCHIVO_NORMALIZACION, sheet_name=hoja)
        
        if df_hoja.empty:
            log_info(f"Hoja '{hoja}' está vacía, omitiendo")
            continue
        
        # Obtener nombres de columnas (primera fila es encabezado)
        if len(df_hoja.columns) < 2:
            log_error(f"Hoja '{hoja}' debe tener al menos 2 columnas")
            continue
        
        col_actual = df_hoja.columns[0]
        col_normalizada = df_hoja.columns[1]
        
        # Crear diccionario de mapeo usando operaciones vectorizadas (optimizado)
        # Filtrar filas con valores nulos en col_actual
        mask_validos = df_hoja[col_actual].notna()
        df_validos = df_hoja.loc[mask_validos]
        
        if df_validos.empty:
            continue
        
        # Convertir a string y limpiar espacios usando operaciones vectorizadas
        claves = df_validos[col_actual].astype(str).str.strip()
        valores = df_validos[col_normalizada]
        
        # Filtrar valores normalizados no nulos y crear diccionario
        mask_valores_validos = valores.notna()
        claves_validas = claves[mask_valores_validos]
        valores_validos = valores[mask_valores_validos].astype(str).str.strip()
        
        # Crear diccionario directamente desde Series (más eficiente)
        mapeo = dict(zip(claves_validas, valores_validos))
        
        if mapeo:
            mapeos[hoja] = mapeo
            log_info(f"Cargados {len(mapeo)} mapeos para la columna '{hoja}'")
        else:
            log_info(f"No se encontraron mapeos válidos para la columna '{hoja}'")
    
    log_info(f"Total de columnas con mapeos: {len(mapeos)}")
    return mapeos


def aplicar_normalizacion_final() -> None:
    """
    Aplica la normalización final de ortografía y formato al archivo Programas.xlsx.
    """
    if not ARCHIVO_PROGRAMAS.exists():
        error_msg = f"No se encontró el archivo: {ARCHIVO_PROGRAMAS}"
        log_error(error_msg)
        raise FileNotFoundError(error_msg)
    
    log_info(f"Cargando archivo: {ARCHIVO_PROGRAMAS.name}")
    df = pd.read_excel(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
    log_info(f"Archivo cargado: {len(df)} filas, {len(df.columns)} columnas")
    
    # Cargar mapeos
    mapeos = cargar_mapeos_normalizacion()
    
    columnas_procesadas = 0
    total_reemplazos = 0
    
    # Aplicar mapeos a cada columna (optimizado para memoria)
    for nombre_columna, mapeo in mapeos.items():
        if nombre_columna not in df.columns:
            log_info(f"Columna '{nombre_columna}' no encontrada en el archivo, omitiendo")
            continue
        
        log_info(f"Procesando columna: {nombre_columna}")
        
        # Caso especial: NOMBRE_INSTITUCIÓN usa CÓDIGO_INSTITUCIÓN_PADRE
        if nombre_columna == HOJA_ESPECIAL_INSTITUCION:
            if COLUMNA_ID_INSTITUCION not in df.columns:
                error_msg = (
                    f"Columna '{COLUMNA_ID_INSTITUCION}' no encontrada. "
                    f"Necesaria para mapear '{nombre_columna}'"
                )
                log_error(error_msg)
                raise KeyError(error_msg)
            
            # Mapear usando CÓDIGO_INSTITUCIÓN_PADRE (operación vectorizada)
            # Guardar referencia a columna original antes de modificar (para conteo)
            columna_original = df[nombre_columna]
            # Aplicar mapeo usando el código de institución padre
            valores_mapeados = (
                df[COLUMNA_ID_INSTITUCION].astype(str).str.strip().map(mapeo)
            )
            # Mantener valores originales donde no hay mapeo (eficiente con fillna)
            df[nombre_columna] = valores_mapeados.fillna(columna_original)
            log_info(f"  -> Usando columna '{COLUMNA_ID_INSTITUCION}' para mapeo por ID")
            
        else:
            # Para las demás columnas, mapear el texto directamente
            # Guardar referencia a columna original (para conteo y restauración)
            columna_original = df[nombre_columna]
            # Crear máscara de NaN antes de conversión para preservarlos
            mask_nan_original = columna_original.isna()
            
            # Convertir a string, limpiar espacios y aplicar mapeo (vectorizado)
            valores_mapeados = columna_original.astype(str).str.strip().map(mapeo)
            
            # Restaurar valores originales donde no hay mapeo (eficiente)
            df[nombre_columna] = valores_mapeados.fillna(columna_original)
            
            # Restaurar NaN originales solo donde era necesario (optimizado)
            if mask_nan_original.any():
                df.loc[mask_nan_original, nombre_columna] = pd.NA
        
        # Contar reemplazos de forma optimizada (común para ambos casos)
        # Comparar solo donde ambos tienen valores (evita comparaciones innecesarias)
        mask_ambos_valores = columna_original.notna() & df[nombre_columna].notna()
        if mask_ambos_valores.any():
            cambios_valores = (
                columna_original[mask_ambos_valores] != df[nombre_columna][mask_ambos_valores]
            ).sum()
        else:
            cambios_valores = 0
        
        # Contar cambios de NaN a valor o viceversa
        cambios_na = (columna_original.isna() != df[nombre_columna].isna()).sum()
        reemplazos = cambios_valores + cambios_na
        
        total_reemplazos += reemplazos
        columnas_procesadas += 1
        
        if reemplazos > 0:
            log_info(f"  -> {reemplazos} valores normalizados en '{nombre_columna}'")
    
    log_info(f"Total de columnas procesadas: {columnas_procesadas}")
    log_info(f"Total de valores reemplazados: {total_reemplazos}")
    
    # Guardar archivo actualizado
    log_info(f"Guardando archivo actualizado: {ARCHIVO_PROGRAMAS.name}")
    with pd.ExcelWriter(
        ARCHIVO_PROGRAMAS,
        mode="a",
        if_sheet_exists="replace",
        engine="openpyxl",
    ) as writer:
        df.to_excel(writer, sheet_name=HOJA_PROGRAMAS, index=False)
    
    log_info(f"Normalización final completada: {ARCHIVO_PROGRAMAS.name}")


if __name__ == "__main__":
    aplicar_normalizacion_final()

