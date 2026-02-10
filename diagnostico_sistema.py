"""
Script de diagnóstico del sistema SNIES Manager.
Verifica que todos los componentes estén funcionando correctamente.
"""

import sys
from pathlib import Path

print("=" * 70)
print("DIAGNÓSTICO DEL SISTEMA SNIES MANAGER")
print("=" * 70)
print()

# 1. Verificar estructura de directorios
print("1. VERIFICANDO ESTRUCTURA DE DIRECTORIOS...")
directorios_requeridos = [
    "app",
    "etl",
    "outputs",
    "ref",
    "models",
    "docs",
    "logs"
]

for dir_name in directorios_requeridos:
    dir_path = Path(dir_name)
    if dir_path.exists():
        print(f"  [OK] {dir_name}/ existe")
    else:
        print(f"  [ERROR] {dir_name}/ NO existe")

print()

# 2. Verificar archivos Python principales
print("2. VERIFICANDO ARCHIVOS PRINCIPALES...")
archivos_requeridos = [
    "app/main.py",
    "app/__init__.py",
    "etl/config.py",
    "etl/historicoProgramasNuevos.py",
    "etl/pipeline_logger.py",
    "etl/procesamientoSNIES.py",
    "etl/clasificacionProgramas.py",
    "etl/descargaSNIES.py",
    "build_exe.py",
    "requirements.txt"
]

for archivo in archivos_requeridos:
    archivo_path = Path(archivo)
    if archivo_path.exists():
        print(f"  [OK] {archivo} existe")
    else:
        print(f"  [ERROR] {archivo} NO existe")

print()

# 3. Verificar imports críticos
print("3. VERIFICANDO IMPORTS CRÍTICOS...")
try:
    from etl.config import (
        ARCHIVO_HISTORICO,
        ARCHIVO_PROGRAMAS,
        OUTPUTS_DIR,
        REF_DIR,
        MODELS_DIR,
        DOCS_DIR,
        LOGS_DIR
    )
    print("  [OK] etl.config importado correctamente")
    print(f"    - ARCHIVO_HISTORICO: {ARCHIVO_HISTORICO}")
    print(f"    - ARCHIVO_PROGRAMAS: {ARCHIVO_PROGRAMAS}")
    print(f"    - OUTPUTS_DIR: {OUTPUTS_DIR}")
    print(f"    - REF_DIR: {REF_DIR}")
except Exception as e:
    print(f"  [ERROR] Error importando etl.config: {e}")

try:
    from etl.historicoProgramasNuevos import actualizar_historico_programas_nuevos
    print("  [OK] etl.historicoProgramasNuevos importado correctamente")
except Exception as e:
    print(f"  [ERROR] Error importando etl.historicoProgramasNuevos: {e}")

try:
    from etl.pipeline_logger import log_info, log_error, log_warning
    print("  [OK] etl.pipeline_logger importado correctamente")
except Exception as e:
    print(f"  [ERROR] Error importando etl.pipeline_logger: {e}")

try:
    from etl.exceptions_helpers import leer_excel_con_reintentos
    print("  [OK] etl.exceptions_helpers importado correctamente")
except Exception as e:
    print(f"  [ERROR] Error importando etl.exceptions_helpers: {e}")

print()

# 4. Verificar configuración del archivo histórico
print("4. VERIFICANDO CONFIGURACIÓN DEL ARCHIVO HISTÓRICO...")
try:
    from etl.config import ARCHIVO_HISTORICO
    nombre_esperado = "HistoricoProgramasNuevos .xlsx"
    if ARCHIVO_HISTORICO.name == nombre_esperado:
        print(f"  [OK] ARCHIVO_HISTORICO configurado correctamente: {ARCHIVO_HISTORICO.name}")
    else:
        print(f"  [ADVERTENCIA] ARCHIVO_HISTORICO tiene nombre diferente: {ARCHIVO_HISTORICO.name}")
        print(f"    Esperado: {nombre_esperado}")
    
    # Verificar si existe el archivo histórico
    if ARCHIVO_HISTORICO.exists():
        print(f"  [OK] Archivo histórico existe: {ARCHIVO_HISTORICO}")
        try:
            from etl.config import HOJA_HISTORICO
            import pandas as pd
            df = pd.read_excel(ARCHIVO_HISTORICO, sheet_name=HOJA_HISTORICO)
            print(f"  [OK] Archivo histórico es válido: {len(df)} registros")
        except Exception as e:
            print(f"  [ERROR] Error leyendo archivo histórico: {e}")
    else:
        print(f"  [ADVERTENCIA] Archivo histórico no existe aún (se creará en primera ejecución)")
    
    # Verificar si hay archivos duplicados
    outputs_dir = ARCHIVO_HISTORICO.parent
    archivos_historicos = list(outputs_dir.glob("HistoricoProgramasNuevos*.xlsx"))
    if len(archivos_historicos) > 1:
        print(f"  [ADVERTENCIA] Se encontraron {len(archivos_historicos)} archivos históricos:")
        for archivo in archivos_historicos:
            print(f"    - {archivo.name}")
    elif len(archivos_historicos) == 1:
        print(f"  [OK] Solo existe un archivo histórico: {archivos_historicos[0].name}")
    else:
        print(f"  [ADVERTENCIA] No se encontraron archivos históricos")
        
except Exception as e:
    print(f"  [ERROR] Error verificando configuración: {e}")

print()

# 5. Verificar archivos de referencia
print("5. VERIFICANDO ARCHIVOS DE REFERENCIA...")
try:
    from etl.config import REF_DIR, ARCHIVO_REFERENTES, ARCHIVO_CATALOGO_EAFIT
    
    if REF_DIR.exists():
        print(f"  [OK] REF_DIR existe: {REF_DIR}")
    else:
        print(f"  [ERROR] REF_DIR NO existe: {REF_DIR}")
    
    # Buscar archivos de referencia
    ref_backup = REF_DIR / "backup"
    archivos_ref = []
    
    for carpeta in [REF_DIR, ref_backup]:
        if carpeta.exists():
            for nombre in ["referentesUnificados", "catalogoOfertasEAFIT"]:
                for ext in [".xlsx", ".csv", ".XLSX", ".CSV"]:
                    archivo = carpeta / f"{nombre}{ext}"
                    if archivo.exists():
                        archivos_ref.append(archivo)
                        break
    
    if archivos_ref:
        print(f"  [OK] Se encontraron {len(archivos_ref)} archivos de referencia:")
        for archivo in archivos_ref:
            print(f"    - {archivo}")
    else:
        print(f"  [ADVERTENCIA] No se encontraron archivos de referencia")
        
except Exception as e:
    print(f"  [ERROR] Error verificando archivos de referencia: {e}")

print()

# 6. Verificar sintaxis de archivos Python críticos
print("6. VERIFICANDO SINTAXIS DE ARCHIVOS PYTHON...")
archivos_python = [
    "etl/config.py",
    "etl/historicoProgramasNuevos.py",
    "etl/pipeline_logger.py",
    "app/main.py"
]

for archivo in archivos_python:
    archivo_path = Path(archivo)
    if archivo_path.exists():
        try:
            compile(open(archivo_path, encoding='utf-8').read(), archivo_path, 'exec')
            print(f"  [OK] {archivo} - Sintaxis correcta")
        except SyntaxError as e:
            print(f"  [ERROR] {archivo} - Error de sintaxis: {e}")
        except Exception as e:
            print(f"  [ADVERTENCIA] {archivo} - Error al verificar: {e}")
    else:
        print(f"  [ERROR] {archivo} - No existe")

print()

# 7. Verificar dependencias básicas
print("7. VERIFICANDO DEPENDENCIAS BÁSICAS...")
dependencias = [
    "pandas",
    "numpy",
    "openpyxl",
    "unidecode",
    "rapidfuzz",
    "sentence_transformers",
    "sklearn",
    "selenium",
    "webdriver_manager"
]

for dep in dependencias:
    try:
        __import__(dep)
        print(f"  [OK] {dep} instalado")
    except ImportError:
        print(f"  [ERROR] {dep} NO instalado")
    except Exception as e:
        print(f"  [ADVERTENCIA] {dep} - Error: {e}")

print()

# 8. Resumen
print("=" * 70)
print("RESUMEN DEL DIAGNOSTICO")
print("=" * 70)
print("Si todos los componentes muestran [OK], el sistema esta listo para usar.")
print("Si hay [ERROR], revisa esos componentes antes de ejecutar el sistema.")
print("Si hay [ADVERTENCIA], son advertencias que no impiden el funcionamiento.")
print("=" * 70)
