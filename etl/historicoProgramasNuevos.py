"""
Módulo para actualizar el archivo histórico de programas nuevos.

Cada vez que se ejecuta el pipeline, se agregan los programas nuevos detectados
al archivo HistoricoProgramasNuevos.xlsx con la fecha de ejecución.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

import pandas as pd

# Agregar el directorio raíz al path para importar módulos
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.pipeline_logger import log_error, log_info, log_resultado
from etl.config import (
    ARCHIVO_PROGRAMAS,
    ARCHIVO_HISTORICO,
    HOJA_PROGRAMAS,
    HOJA_HISTORICO,
)

# Columnas a extraer del archivo Programas.xlsx
COLUMNAS_REQUERIDAS = [
    "CÓDIGO_SNIES_DEL_PROGRAMA",
    "NOMBRE_INSTITUCIÓN",
    "NOMBRE_DEL_PROGRAMA",
    "PROGRAMA_NUEVO",
    "ES_REFERENTE",
    "PROGRAMA_EAFIT_CODIGO",
    "PROGRAMA_EAFIT_NOMBRE",
]

# Nombre de la columna de fecha (primera columna)
COLUMNA_FECHA = "FECHA"


def actualizar_historico_programas_nuevos() -> None:
    """
    Actualiza el archivo histórico de programas nuevos.
    
    Lee los programas nuevos de Programas.xlsx y los agrega al archivo
    HistoricoProgramasNuevos.xlsx con la fecha de ejecución.
    """
    # Verificar que existe el archivo de programas
    if not ARCHIVO_PROGRAMAS.exists():
        error_msg = f"No se encontró el archivo: {ARCHIVO_PROGRAMAS}"
        log_error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Leer el archivo de programas
    print(f"Leyendo programas desde: {ARCHIVO_PROGRAMAS}")
    try:
        df_programas = pd.read_excel(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
        log_info(f"Archivo de programas cargado: {ARCHIVO_PROGRAMAS.name}")
    except PermissionError as e:
        error_msg = (
            f"No se puede leer el archivo {ARCHIVO_PROGRAMAS.name}. "
            f"El archivo está abierto en otro programa (Excel, Power BI, etc.). "
            f"Por favor, ciérralo e intenta de nuevo."
        )
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        raise PermissionError(error_msg) from e
    
    # Verificar que existe la columna PROGRAMA_NUEVO
    if "PROGRAMA_NUEVO" not in df_programas.columns:
        error_msg = (
            "No se encontró la columna 'PROGRAMA_NUEVO'. "
            "Ejecute primero procesamientoSNIES.py"
        )
        log_error(error_msg)
        raise ValueError(error_msg)
    
    # Filtrar solo programas nuevos
    df_nuevos = df_programas[df_programas["PROGRAMA_NUEVO"] == "Sí"].copy()
    
    if len(df_nuevos) == 0:
        info_msg = "No hay programas nuevos para agregar al histórico."
        print(info_msg)
        log_info(info_msg)
        return
    
    print(f"Programas nuevos detectados: {len(df_nuevos)}")
    
    # Verificar que todas las columnas requeridas existen
    columnas_faltantes = [
        col for col in COLUMNAS_REQUERIDAS if col not in df_nuevos.columns
    ]
    if columnas_faltantes:
        error_msg = (
            f"No se encontraron las siguientes columnas en el archivo: "
            f"{', '.join(columnas_faltantes)}"
        )
        log_error(error_msg)
        raise ValueError(error_msg)
    
    # Seleccionar solo las columnas requeridas
    df_para_historico = df_nuevos[COLUMNAS_REQUERIDAS].copy()
    
    # Agregar la fecha de ejecución como primera columna
    fecha_ejecucion = datetime.datetime.now().strftime("%Y-%m-%d")
    df_para_historico.insert(0, COLUMNA_FECHA, fecha_ejecucion)
    
    # Leer el archivo histórico existente (si existe)
    if ARCHIVO_HISTORICO.exists():
        print(f"Leyendo archivo histórico existente: {ARCHIVO_HISTORICO}")
        try:
            df_historico_existente = pd.read_excel(
                ARCHIVO_HISTORICO, sheet_name=HOJA_HISTORICO
            )
            log_info(f"Archivo histórico existente cargado: {len(df_historico_existente)} registros")
            
            # Verificar que las columnas coinciden
            columnas_esperadas = [COLUMNA_FECHA] + COLUMNAS_REQUERIDAS
            if list(df_historico_existente.columns) != columnas_esperadas:
                error_msg = (
                    f"Las columnas del archivo histórico no coinciden. "
                    f"Esperadas: {columnas_esperadas}, "
                    f"Encontradas: {list(df_historico_existente.columns)}"
                )
                log_error(error_msg)
                raise ValueError(error_msg)
            
            # Filtrar filas con más de 3 meses de antigüedad
            fecha_limite = datetime.datetime.now() - datetime.timedelta(days=90)
            fecha_limite_str = fecha_limite.strftime("%Y-%m-%d")
            
            # Convertir la columna FECHA_EJECUCION a datetime para comparación
            df_historico_existente[COLUMNA_FECHA] = pd.to_datetime(
                df_historico_existente[COLUMNA_FECHA], errors='coerce'
            )
            
            # Filtrar solo las filas con menos de 3 meses
            df_historico_filtrado = df_historico_existente[
                df_historico_existente[COLUMNA_FECHA] >= fecha_limite
            ].copy()
            
            # Convertir de vuelta a string para mantener consistencia
            df_historico_filtrado[COLUMNA_FECHA] = df_historico_filtrado[COLUMNA_FECHA].dt.strftime("%Y-%m-%d")
            
            filas_eliminadas = len(df_historico_existente) - len(df_historico_filtrado)
            if filas_eliminadas > 0:
                print(f"Filas eliminadas (más de 3 meses): {filas_eliminadas}")
                log_info(f"Filas eliminadas por antigüedad (>3 meses): {filas_eliminadas}")
            
            # Concatenar los nuevos registros con los existentes filtrados
            df_historico_final = pd.concat(
                [df_historico_filtrado, df_para_historico], ignore_index=True
            )
            print(f"Total de registros en histórico (después de limpieza): {len(df_historico_final)}")
        except Exception as e:
            error_msg = f"Error al leer el archivo histórico existente: {e}"
            log_error(error_msg)
            print(f"[WARN] {error_msg}. Creando nuevo archivo histórico.")
            df_historico_final = df_para_historico
    else:
        print("No existe archivo histórico. Creando nuevo archivo.")
        df_historico_final = df_para_historico
    
    # Guardar el archivo histórico
    print(f"Guardando archivo histórico: {ARCHIVO_HISTORICO}")
    with pd.ExcelWriter(
        ARCHIVO_HISTORICO,
        mode="w",
        engine="openpyxl",
    ) as writer:
        df_historico_final.to_excel(
            writer, sheet_name=HOJA_HISTORICO, index=False
        )
    
    total_registros = len(df_historico_final)
    nuevos_registros = len(df_para_historico)
    log_info(f"Archivo histórico actualizado: {ARCHIVO_HISTORICO.name}")
    log_resultado(f"Registros agregados al histórico: {nuevos_registros}")
    log_resultado(f"Total de registros en histórico: {total_registros}")
    print(f"Archivo histórico actualizado exitosamente.")
    print(f"  - Registros agregados en esta ejecución: {nuevos_registros}")
    print(f"  - Total de registros en histórico: {total_registros}")


if __name__ == "__main__":
    actualizar_historico_programas_nuevos()

