"""
Módulo para actualizar el archivo histórico de programas nuevos.

Cada vez que se ejecuta el pipeline, se agregan los programas nuevos detectados
al archivo HistoricoProgramasNuevos .xlsx (con espacio) con la fecha de ejecución.
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

from etl.pipeline_logger import log_error, log_info, log_resultado, log_warning
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
    "ÁREA_DE_CONOCIMIENTO",  # Agregada para guardar área imputada
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
    "ÁREA_DE_CONOCIMIENTO",  # Agregada para guardar área imputada
    "Afinidad",  # No se extrae de Programas.xlsx
    "Nivel",  # No se extrae de Programas.xlsx
    "ESTADO_PROGRAMA",  # No se extrae de Programas.xlsx
]


def _limpiar_archivos_temporales_excel(directorio: Path) -> None:
    """
    Elimina archivos temporales de Excel (que empiezan con ~$) en el directorio especificado.
    
    Estos archivos se crean cuando Excel tiene un archivo abierto y pueden causar problemas.
    """
    import glob
    import os
    
    patron = str(directorio / "~$*.xlsx")
    archivos_temp = glob.glob(patron)
    
    for archivo_temp in archivos_temp:
        try:
            os.remove(archivo_temp)
            log_info(f"Archivo temporal eliminado: {os.path.basename(archivo_temp)}")
        except Exception as e:
            log_warning(f"No se pudo eliminar archivo temporal {os.path.basename(archivo_temp)}: {e}")


def _consolidar_archivos_historicos_duplicados() -> pd.DataFrame | None:
    """
    Consolida archivos históricos duplicados si existen múltiples variaciones del nombre.
    
    Busca variaciones del nombre (con/sin espacio) y consolida todos los registros.
    Mantiene el archivo con más registros (ARCHIVO_HISTORICO) y elimina los que tienen menos.
    
    Returns:
        DataFrame consolidado con todos los registros, o None si no hay archivos históricos.
    """
    from etl.exceptions_helpers import leer_excel_con_reintentos
    
    # Limpiar archivos temporales de Excel antes de procesar
    _limpiar_archivos_temporales_excel(ARCHIVO_HISTORICO.parent)
    
    archivos_encontrados = []
    # Buscar variaciones: el archivo principal ahora es el que tiene espacio
    variaciones = [
        ARCHIVO_HISTORICO,  # HistoricoProgramasNuevos .xlsx (con espacio - archivo principal)
        ARCHIVO_HISTORICO.parent / "HistoricoProgramasNuevos.xlsx",  # Sin espacio (variación antigua)
        ARCHIVO_HISTORICO.parent / "HistoricoProgramasNuevos  .xlsx",  # Con dos espacios (variación)
    ]
    
    # Buscar todos los archivos históricos posibles
    for archivo in variaciones:
        if archivo.exists():
            # Ignorar archivos temporales de Excel
            if archivo.name.startswith("~$"):
                continue
            try:
                df = leer_excel_con_reintentos(archivo, sheet_name=HOJA_HISTORICO)
                archivos_encontrados.append((archivo, df, len(df)))
                print(f"Encontrado archivo histórico: {archivo.name} ({len(df)} registros)")
                log_info(f"Archivo histórico encontrado: {archivo.name} ({len(df)} registros)")
            except Exception as e:
                print(f"[WARN] No se pudo leer {archivo.name}: {e}")
                log_error(f"Error al leer {archivo.name}: {e}")
    
    if not archivos_encontrados:
        return None
    
    # Si solo hay un archivo, retornarlo directamente
    if len(archivos_encontrados) == 1:
        archivo, df, _ = archivos_encontrados[0]
        # Si no es el archivo principal, renombrarlo al nombre correcto
        if archivo != ARCHIVO_HISTORICO:
            try:
                archivo.rename(ARCHIVO_HISTORICO)
                print(f"✓ Archivo renombrado: {archivo.name} → {ARCHIVO_HISTORICO.name}")
                log_info(f"Archivo histórico renombrado: {archivo.name} → {ARCHIVO_HISTORICO.name}")
            except Exception as e:
                print(f"[WARN] No se pudo renombrar {archivo.name}: {e}")
                log_error(f"Error al renombrar archivo histórico {archivo.name}: {e}")
        return df
    
    # Hay múltiples archivos, consolidarlos
    print(f"⚠️ Se encontraron {len(archivos_encontrados)} archivos históricos duplicados. Consolidando...")
    log_warning(f"Se encontraron {len(archivos_encontrados)} archivos históricos duplicados. Consolidando.")
    
    # Ordenar por número de registros (descendente) para identificar el archivo con más datos
    archivos_encontrados.sort(key=lambda x: x[2], reverse=True)
    archivo_principal, df_principal, registros_principal = archivos_encontrados[0]
    print(f"Archivo con más registros: {archivo_principal.name} ({registros_principal} registros)")
    
    # Consolidar todos los DataFrames
    dfs_consolidar = [df for _, df, _ in archivos_encontrados]
    df_consolidado = pd.concat(dfs_consolidar, ignore_index=True)
    
    # Eliminar duplicados basados en código SNIES y fecha (mantener el más reciente)
    if "CÓDIGO_SNIES_DEL_PROGRAMA" in df_consolidado.columns:
        # Ordenar por fecha descendente si existe (mantener el más reciente)
        if COLUMNA_FECHA in df_consolidado.columns:
            df_consolidado = df_consolidado.sort_values(
                by=COLUMNA_FECHA, ascending=False, na_position="last"
            )
        
        antes = len(df_consolidado)
        # Eliminar duplicados: mismo código SNIES y misma fecha (o solo código si no hay fecha)
        subset_dup = ["CÓDIGO_SNIES_DEL_PROGRAMA"]
        if COLUMNA_FECHA in df_consolidado.columns:
            subset_dup.append(COLUMNA_FECHA)
        
        df_consolidado = df_consolidado.drop_duplicates(
            subset=subset_dup,
            keep="first"  # Mantener el primero (más reciente después del sort)
        )
        despues = len(df_consolidado)
        duplicados_eliminados = antes - despues
        if duplicados_eliminados > 0:
            print(f"Eliminados {duplicados_eliminados} registros duplicados en la consolidación.")
            log_info(f"Eliminados {duplicados_eliminados} registros duplicados durante consolidación.")
    
    print(f"✓ Consolidación completada: {len(df_consolidado)} registros únicos")
    log_info(f"Consolidación completada: {len(df_consolidado)} registros únicos de {len(archivos_encontrados)} archivos")
    
    # Eliminar los archivos con menos registros (mantener solo el principal)
    # El archivo principal se escribirá después con todos los datos consolidados
    for archivo, _, registros in archivos_encontrados:
        # Eliminar todos los archivos duplicados (el principal se reescribirá después)
        try:
            archivo.unlink()
            print(f"  → Eliminado archivo duplicado: {archivo.name} ({registros} registros)")
            log_info(f"Archivo duplicado eliminado: {archivo.name} ({registros} registros)")
        except Exception as e:
            print(f"[WARN] No se pudo eliminar {archivo.name}: {e}")
            log_error(f"Error al eliminar archivo duplicado {archivo.name}: {e}")
    
    return df_consolidado


def actualizar_historico_programas_nuevos() -> None:
    """
    Actualiza el archivo histórico de programas nuevos.
    
    Lee los programas nuevos de Programas.xlsx y los agrega al archivo
    HistoricoProgramasNuevos .xlsx (con espacio) con la fecha de ejecución.
    
    Maneja automáticamente archivos históricos duplicados:
    - Consolida todos los archivos encontrados
    - Mantiene el archivo con más registros (ARCHIVO_HISTORICO)
    - Elimina los archivos con menos registros
    """
    # Limpiar archivos temporales de Excel antes de procesar
    _limpiar_archivos_temporales_excel(ARCHIVO_HISTORICO.parent)
    
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
        # Si falta ÁREA_DE_CONOCIMIENTO, crear la columna vacía (puede no existir si aún no se ha imputado)
        if "ÁREA_DE_CONOCIMIENTO" in columnas_faltantes:
            df_nuevos["ÁREA_DE_CONOCIMIENTO"] = None
            columnas_faltantes.remove("ÁREA_DE_CONOCIMIENTO")
            log_info("Columna ÁREA_DE_CONOCIMIENTO no encontrada, se creará vacía en el histórico")
        
        # Si aún faltan otras columnas críticas, lanzar error
        if columnas_faltantes:
            error_msg = (
                f"No se encontraron las siguientes columnas en el archivo: "
                f"{', '.join(columnas_faltantes)}"
            )
            log_error(error_msg)
            raise ValueError(error_msg)
    
    # Seleccionar solo las columnas requeridas que se extraen de Programas.xlsx
    df_extraido = df_nuevos[COLUMNAS_REQUERIDAS].copy()
    
    # Log para verificar que se están guardando las áreas
    if "ÁREA_DE_CONOCIMIENTO" in df_extraido.columns:
        areas_asignadas = df_extraido["ÁREA_DE_CONOCIMIENTO"].notna().sum()
        areas_totales = len(df_extraido)
        log_info(f"Programas nuevos con área asignada: {areas_asignadas}/{areas_totales}")
        if areas_asignadas > 0:
            print(f"✓ {areas_asignadas} programas nuevos tienen área de conocimiento asignada")
    
    # Agregar la fecha de ejecución
    fecha_ejecucion = datetime.datetime.now().strftime("%Y-%m-%d")
    df_extraido.insert(0, COLUMNA_FECHA, fecha_ejecucion)
    
    # Inicializar variable para el orden de columnas
    columnas_orden_historico = None
    
    # Buscar y consolidar archivos históricos existentes (maneja duplicados con/sin espacio)
    df_historico_existente = _consolidar_archivos_historicos_duplicados()
    
    if df_historico_existente is not None:
        print(f"Archivo histórico existente cargado: {len(df_historico_existente)} registros")
        log_info(f"Archivo histórico existente cargado: {len(df_historico_existente)} registros")
        
        # Obtener el orden de columnas del archivo histórico existente
        columnas_orden_historico = list(df_historico_existente.columns)
        
        # Verificar y agregar columnas faltantes al histórico existente (migración de esquema)
        # Esto permite que históricos antiguos se actualicen con nuevas columnas sin perder datos
        columnas_faltantes_en_historico = [
            col for col in [COLUMNA_FECHA] + COLUMNAS_REQUERIDAS 
            if col not in columnas_orden_historico
        ]
        if columnas_faltantes_en_historico:
            # Agregar columnas faltantes al histórico existente con valores None/NaN para registros antiguos
            log_info(f"Agregando columnas faltantes al histórico existente: {', '.join(columnas_faltantes_en_historico)}")
            print(f"⚠️ El histórico no tiene las siguientes columnas. Se agregarán automáticamente: {', '.join(columnas_faltantes_en_historico)}")
            for col in columnas_faltantes_en_historico:
                df_historico_existente[col] = None
            # Actualizar el orden de columnas para incluir las nuevas
            columnas_orden_historico = list(df_historico_existente.columns)
            log_info(f"Histórico actualizado con nuevas columnas. Total de columnas: {len(columnas_orden_historico)}")
        
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
        
        # Concatenar los nuevos registros con los existentes (sin eliminar ningún registro)
        df_historico_final = pd.concat(
            [df_historico_existente, df_para_historico], ignore_index=True
        )
        print(f"Total de registros en histórico: {len(df_historico_final)}")
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
        df_historico_final.to_excel(writer, sheet_name=HOJA_HISTORICO, index=False)
    
    print(f"✓ Archivo histórico guardado: {len(df_historico_final)} registros totales")
    log_info(f"Archivo histórico guardado: {ARCHIVO_HISTORICO.name} ({len(df_historico_final)} registros totales)")


def actualizar_registros_historicos_ajustes_manuales(
    cambios: dict[str, dict[str, any]],
    df_programas: pd.DataFrame | None = None,
) -> None:
    """
    Actualiza registros existentes en el histórico cuando se hacen ajustes manuales.
    
    Esta función se llama desde la página de ajustes manuales para sincronizar
    los cambios de referentes (ES_REFERENTE, PROGRAMA_EAFIT_CODIGO, PROGRAMA_EAFIT_NOMBRE)
    con el archivo histórico.
    
    Args:
        cambios: Diccionario con código SNIES normalizado como clave y diccionario de cambios como valor.
                 Ejemplo: {"12345": {"ES_REFERENTE": "Sí", "PROGRAMA_EAFIT_CODIGO": 123}}
        df_programas: DataFrame opcional con los datos de Programas.xlsx. Si es None, se lee del archivo.
    """
    # Limpiar archivos temporales de Excel antes de procesar
    _limpiar_archivos_temporales_excel(ARCHIVO_HISTORICO.parent)
    
    # Si no hay cambios, no hacer nada
    if not cambios:
        log_info("No hay cambios para actualizar en el histórico")
        return
    
    # Leer Programas.xlsx si no se proporciona
    if df_programas is None:
        from etl.exceptions_helpers import leer_excel_con_reintentos
        try:
            df_programas = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name=HOJA_PROGRAMAS)
        except Exception as e:
            error_msg = f"No se pudo leer Programas.xlsx para actualizar histórico: {e}"
            log_error(error_msg)
            raise FileNotFoundError(error_msg) from e
    
    # Normalizar códigos SNIES en Programas.xlsx para comparación
    def _normalizar_codigo(valor: object) -> str:
        if pd.isna(valor):
            return ""
        codigo_str = str(valor).strip().upper()
        codigo_str = codigo_str.replace(".0", "")
        return codigo_str
    
    df_programas["_CODIGO_NORM"] = df_programas["CÓDIGO_SNIES_DEL_PROGRAMA"].apply(_normalizar_codigo)
    
    # Cargar histórico existente
    df_historico = _consolidar_archivos_historicos_duplicados()
    
    if df_historico is None or len(df_historico) == 0:
        log_info("No existe archivo histórico para actualizar. Los cambios se guardarán cuando se ejecute el pipeline.")
        return
    
    # Normalizar códigos SNIES en histórico para comparación
    if "CÓDIGO_SNIES_DEL_PROGRAMA" not in df_historico.columns:
        log_warning("El histórico no tiene columna CÓDIGO_SNIES_DEL_PROGRAMA. No se pueden actualizar registros.")
        return
    
    df_historico["_CODIGO_NORM"] = df_historico["CÓDIGO_SNIES_DEL_PROGRAMA"].apply(_normalizar_codigo)
    
    # Columnas que se pueden actualizar desde ajustes manuales
    columnas_actualizables = ["ES_REFERENTE", "PROGRAMA_EAFIT_CODIGO", "PROGRAMA_EAFIT_NOMBRE"]
    
    # Actualizar registros en el histórico
    registros_actualizados = 0
    for codigo_norm, cambios_dict in cambios.items():
        if not codigo_norm:
            continue
        
        # Buscar el registro en el histórico
        mask_historico = df_historico["_CODIGO_NORM"] == codigo_norm
        
        if not mask_historico.any():
            # El programa no está en el histórico (probablemente no es nuevo)
            continue
        
        # Obtener los valores actualizados de Programas.xlsx
        mask_programas = df_programas["_CODIGO_NORM"] == codigo_norm
        if not mask_programas.any():
            continue
        
        # Actualizar solo las columnas que están en cambios_dict y son actualizables
        for col in columnas_actualizables:
            if col in cambios_dict:
                nuevo_valor = cambios_dict[col]
                # Obtener el valor de Programas.xlsx para asegurar consistencia
                valor_programas = df_programas.loc[mask_programas, col].iloc[0] if mask_programas.any() else nuevo_valor
                
                # Actualizar en histórico
                if col in df_historico.columns:
                    df_historico.loc[mask_historico, col] = valor_programas
                    registros_actualizados += 1
    
    # Limpiar columna temporal
    df_historico = df_historico.drop(columns=["_CODIGO_NORM"])
    
    if registros_actualizados > 0:
        # Guardar histórico actualizado
        print(f"Actualizando {registros_actualizados} registros en el histórico...")
        log_info(f"Actualizando {registros_actualizados} registros en el histórico con ajustes manuales")
        
        try:
            with pd.ExcelWriter(
                ARCHIVO_HISTORICO,
                mode="w",
                engine="openpyxl",
            ) as writer:
                df_historico.to_excel(writer, sheet_name=HOJA_HISTORICO, index=False)
            
            print(f"✓ Histórico actualizado: {registros_actualizados} registros modificados")
            log_info(f"Histórico actualizado exitosamente: {registros_actualizados} registros modificados")
        except Exception as e:
            error_msg = f"Error al guardar histórico actualizado: {e}"
            log_error(error_msg)
            raise RuntimeError(error_msg) from e
    else:
        log_info("No se encontraron registros en el histórico para actualizar con los cambios realizados")


if __name__ == "__main__":
    actualizar_historico_programas_nuevos()
