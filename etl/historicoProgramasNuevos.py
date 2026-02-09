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
    "CÓDIGO_INSTITUCIÓN_PADRE",
    "CÓDIGO_INSTITUCIÓN",
    "NOMBRE_INSTITUCIÓN",
    "CÓDIGO_SNIES_DEL_PROGRAMA",
    "NOMBRE_DEL_PROGRAMA",
    "PROGRAMA_NUEVO",
    "ES_REFERENTE",
    "PROGRAMA_EAFIT_CODIGO",
    "PROGRAMA_EAFIT_NOMBRE",
]

# Nombre de la columna de fecha (primera columna)
COLUMNA_FECHA = "FECHA"

# Orden completo de columnas en el archivo histórico (incluyendo las que no se extraen)
# Las columnas que no se extraen se llenarán con None/NaN
COLUMNAS_ORDEN_HISTORICO = [
    COLUMNA_FECHA,
    "CÓDIGO_INSTITUCIÓN_PADRE",
    "CÓDIGO_INSTITUCIÓN",
    "NOMBRE_INSTITUCIÓN",
    "CÓDIGO_SNIES_DEL_PROGRAMA",
    "NOMBRE_DEL_PROGRAMA",
    "Cod PROGRAMA + Nombre PROGRAMA+ IES",  # No se extrae de Programas.xlsx
    "Cod PROGRAMA + Nombre PROGRAMA",  # No se extrae de Programas.xlsx
    "Cod PROGRAMA + Nombre PROGRAMA EAFIT",  # No se extrae de Programas.xlsx
    "PROGRAMA_NUEVO",
    "ES_REFERENTE",
    "PROGRAMA_EAFIT_CODIGO",
    "PROGRAMA_EAFIT_NOMBRE",
    "Afinidad",  # No se extrae de Programas.xlsx
    "Nivel",  # No se extrae de Programas.xlsx
    "ESTADO_PROGRAMA",  # No se extrae de Programas.xlsx
]


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
    
    # Leer el archivo de programas usando función con reintentos
    print(f"Leyendo programas desde: {ARCHIVO_PROGRAMAS}")
    try:
        from etl.exceptions_helpers import leer_excel_con_reintentos
        df_programas = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
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
    
    # Seleccionar solo las columnas requeridas que se extraen de Programas.xlsx
    df_extraido = df_nuevos[COLUMNAS_REQUERIDAS].copy()
    
    # Agregar la fecha de ejecución
    fecha_ejecucion = datetime.datetime.now().strftime("%Y-%m-%d")
    df_extraido.insert(0, COLUMNA_FECHA, fecha_ejecucion)
    
    # Inicializar variable para el orden de columnas
    columnas_orden_historico = None
    
    # Leer el archivo histórico existente (si existe) para obtener el orden de columnas
    if ARCHIVO_HISTORICO.exists():
        print(f"Leyendo archivo histórico existente: {ARCHIVO_HISTORICO}")
        try:
            from etl.exceptions_helpers import leer_excel_con_reintentos
            df_historico_existente = leer_excel_con_reintentos(
                ARCHIVO_HISTORICO, sheet_name=HOJA_HISTORICO
            )
            log_info(f"Archivo histórico existente cargado: {len(df_historico_existente)} registros")
            
            # Obtener el orden de columnas del archivo histórico existente
            columnas_orden_historico = list(df_historico_existente.columns)
            
            # Construir DataFrame con todas las columnas en el orden correcto
            df_para_historico = pd.DataFrame(index=df_extraido.index)
            
            # Agregar cada columna en el orden del histórico
            for col in columnas_orden_historico:
                if col in df_extraido.columns:
                    # Si la columna se extrae de Programas.xlsx, usar su valor
                    df_para_historico[col] = df_extraido[col]
                else:
                    # Si la columna no se extrae, rellenar con None/NaN
                    df_para_historico[col] = None
            
            # Verificar que todas las columnas requeridas están presentes
            columnas_faltantes_en_historico = [
                col for col in [COLUMNA_FECHA] + COLUMNAS_REQUERIDAS 
                if col not in columnas_orden_historico
            ]
            if columnas_faltantes_en_historico:
                error_msg = (
                    f"Faltan columnas requeridas en el archivo histórico: "
                    f"{', '.join(columnas_faltantes_en_historico)}"
                )
                log_error(error_msg)
                raise ValueError(error_msg)
            
            # Concatenar los nuevos registros con los existentes (sin eliminar ningún registro)
            df_historico_final = pd.concat(
                [df_historico_existente, df_para_historico], ignore_index=True
            )
            print(f"Total de registros en histórico: {len(df_historico_final)}")
        except Exception as e:
            error_msg = f"Error al leer el archivo histórico existente: {e}"
            log_error(error_msg)
            print(f"[WARN] {error_msg}. Creando nuevo archivo histórico.")
            # Si hay error, crear DataFrame con el orden completo definido
            df_para_historico = pd.DataFrame(index=df_extraido.index)
            for col in COLUMNAS_ORDEN_HISTORICO:
                if col in df_extraido.columns:
                    df_para_historico[col] = df_extraido[col]
                else:
                    df_para_historico[col] = None
            df_historico_final = df_para_historico
    else:
        print("No existe archivo histórico. Creando nuevo archivo.")
        # Crear DataFrame con todas las columnas en el orden definido
        df_para_historico = pd.DataFrame(index=df_extraido.index)
        for col in COLUMNAS_ORDEN_HISTORICO:
            if col in df_extraido.columns:
                df_para_historico[col] = df_extraido[col]
            else:
                df_para_historico[col] = None
        df_historico_final = df_para_historico
    
    # Asegurar que las columnas estén en el orden correcto
    # Si existe histórico, usar su orden; si no, usar el orden definido
    if columnas_orden_historico is not None:
        orden_columnas = columnas_orden_historico
    else:
        orden_columnas = COLUMNAS_ORDEN_HISTORICO
    
    # Reordenar columnas según el orden esperado (solo las que existen)
    columnas_existentes = [col for col in orden_columnas if col in df_historico_final.columns]
    columnas_adicionales = [col for col in df_historico_final.columns if col not in orden_columnas]
    df_historico_final = df_historico_final[columnas_existentes + columnas_adicionales]
    
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

