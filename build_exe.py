"""
Script para empaquetar el proyecto en un ejecutable .EXE usando PyInstaller.

Uso:
    python build_exe.py

El ejecutable se generará en la carpeta 'dist/'.
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Colores para la salida (opcional)
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'

def print_colored(message: str, color: str = Colors.RESET) -> None:
    """Imprime un mensaje con color (si está disponible)."""
    try:
        print(f"{color}{message}{Colors.RESET}")
    except:
        print(message)

# Metadatos del ejecutable
PRODUCT_NAME = "Snies Manager"
COMPANY_NAME = "EAFIT"
FILE_VERSION = "2.0.0"
PRODUCT_VERSION = "2.0.0"
INTERNAL_NAME = "SniesManager"
ORIGINAL_FILENAME = "SniesManager.exe"
FILE_DESCRIPTION = (
    "SniesManager es una aplicación de la Universidad EAFIT que automatiza la descarga y análisis "
    "semanal de programas académicos del SNIES, identifica programas nuevos y permite revisar/ajustar "
    "manualmente emparejamientos y resultados, además de reentrenar el modelo de clasificación y "
    "consolidar la información en archivos Excel para seguimiento y toma de decisiones."
)

def verificar_pyinstaller() -> bool:
    """Verifica si PyInstaller está instalado."""
    try:
        import PyInstaller
        return True
    except ImportError:
        return False

def instalar_pyinstaller() -> bool:
    """Instala PyInstaller si no está disponible."""
    print_colored("PyInstaller no está instalado. Instalando...", Colors.YELLOW)
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
        print_colored("PyInstaller instalado correctamente.", Colors.GREEN)
        return True
    except subprocess.CalledProcessError:
        print_colored("Error al instalar PyInstaller.", Colors.RED)
        return False

def limpiar_builds_anteriores() -> None:
    """Limpia builds anteriores de PyInstaller."""
    print_colored("Limpiando builds anteriores...", Colors.YELLOW)
    import time
    
    for carpeta in ["build", "dist"]:
        carpeta_path = Path(carpeta)
        if carpeta_path.exists():
            # Intentar eliminar varias veces con reintentos
            max_intentos = 3
            eliminado = False
            
            for intento in range(max_intentos):
                try:
                    # En Windows, a veces necesitamos esperar un poco
                    if intento > 0:
                        time.sleep(1)
                    
                    shutil.rmtree(carpeta_path)
                    print_colored(f"  -> Carpeta '{carpeta}' eliminada.", Colors.GREEN)
                    eliminado = True
                    break
                except PermissionError:
                    if intento < max_intentos - 1:
                        print_colored(f"  -> Reintentando eliminar '{carpeta}'... (intento {intento + 1}/{max_intentos})", Colors.YELLOW)
                    else:
                        print_colored(f"  -> [ADVERTENCIA] No se pudo eliminar '{carpeta}' (archivos en uso). Continuando...", Colors.YELLOW)
                        print_colored(f"     Cierre el Explorador de Windows si tiene esa carpeta abierta.", Colors.YELLOW)
                except Exception as e:
                    print_colored(f"  -> [ADVERTENCIA] Error al eliminar '{carpeta}': {e}. Continuando...", Colors.YELLOW)
                    break
            
            if not eliminado:
                # Si no se pudo eliminar, al menos limpiar el contenido más importante
                try:
                    # Intentar eliminar solo archivos .spec y __pycache__
                    for item in carpeta_path.rglob("*.spec"):
                        try:
                            item.unlink()
                        except:
                            pass
                    for item in carpeta_path.rglob("__pycache__"):
                        try:
                            shutil.rmtree(item)
                        except:
                            pass
                except:
                    pass

def crear_version_file() -> Path:
    """Crea un archivo de versión para Windows que usará PyInstaller."""
    version_content = f"""# UTF-8
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=({FILE_VERSION.replace('.', ',')}, 0),
    prodvers=({PRODUCT_VERSION.replace('.', ',')}, 0),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo([
      StringTable('040904E4', [
        StringStruct('CompanyName', '{COMPANY_NAME}'),
        StringStruct('FileDescription', '{FILE_DESCRIPTION}'),
        StringStruct('FileVersion', '{FILE_VERSION}'),
        StringStruct('InternalName', '{INTERNAL_NAME}'),
        StringStruct('OriginalFilename', '{ORIGINAL_FILENAME}'),
        StringStruct('ProductName', '{PRODUCT_NAME}'),
        StringStruct('ProductVersion', '{PRODUCT_VERSION}')
      ])
    ]),
    VarFileInfo([VarStruct('Translation', [1033, 1252])])
  ]
)
"""
    vf = Path("version_info.txt")
    with open(vf, "w", encoding="utf-8") as f:
        f.write(version_content)
    return vf

def resolver_icono():
    """Devuelve Path al icono si existe en assets/snies_manager.ico, o None."""
    icon_path = Path("assets") / "snies_manager.ico"
    return icon_path if icon_path.exists() else None

def validar_archivos_necesarios() -> tuple[bool, list[str]]:
    """Valida que existan los archivos necesarios antes de empaquetar."""
    base_path = Path(__file__).parent
    
    def buscar_archivo_referencia(nombre_base: str) -> bool:
        """
        Busca un archivo de referencia en ref/ o ref/backup/ con extensiones .xlsx o .csv.
        Retorna True si lo encuentra, False en caso contrario.
        """
        ref_dir = base_path / "ref"
        # Buscar en ref/ y luego en ref/backup/
        for carpeta in (ref_dir, ref_dir / "backup"):
            if not carpeta.exists():
                continue
            for ext in [".xlsx", ".csv", ".XLSX", ".CSV"]:
                archivo = carpeta / f"{nombre_base}{ext}"
                if archivo.exists():
                    return True
        return False
    
    faltantes = []
    advertencias = []
    
    # Verificar archivo principal de la aplicación
    archivo_main = base_path / "app" / "main.py"
    if not archivo_main.exists():
        faltantes.append("app/main.py - Archivo principal de la aplicación")
    
    # Verificar archivos de referencia (buscan en ref/ y ref/backup/)
    if not buscar_archivo_referencia("referentesUnificados"):
        faltantes.append("ref/referentesUnificados.xlsx o ref/referentesUnificados.csv (o en ref/backup/) - Archivo de referentes")
    
    if not buscar_archivo_referencia("catalogoOfertasEAFIT"):
        faltantes.append("ref/catalogoOfertasEAFIT.xlsx o ref/catalogoOfertasEAFIT.csv (o en ref/backup/) - Catálogo EAFIT")
    
    # Verificar archivos opcionales
    archivos_opcionales = [
        ("docs/normalizacionFinal.xlsx", "Archivo de normalización final"),
        ("models/clasificador_referentes.pkl", "Modelo ML (se entrenará si no existe)"),
    ]
    
    for archivo, descripcion in archivos_opcionales:
        archivo_path = base_path / archivo
        if not archivo_path.exists():
            advertencias.append(f"{archivo} - {descripcion}")
    
    return len(faltantes) == 0, faltantes, advertencias

def crear_spec_file(modo_onefile: bool = False) -> Path:
    """Crea un archivo .spec personalizado para PyInstaller.
    
    Args:
        modo_onefile: Si True, crea un ejecutable único (más lento inicio, más fácil distribuir).
                     Si False, crea carpeta con ejecutable + DLLs (más rápido inicio).
    """
    # Verificar que existan las carpetas necesarias
    base_path = Path(__file__).parent
    ref_path = base_path / "ref"
    models_path = base_path / "models"
    docs_path = base_path / "docs"
    
    datas = []
    if ref_path.exists():
        datas.append(f"('ref', 'ref')")
    if models_path.exists():
        datas.append(f"('models', 'models')")
    if docs_path.exists():
        datas.append(f"('docs', 'docs')")
    
    if datas:
        datas_str = ",\n        ".join(datas)
        datas_section = f"""    datas=[
        {datas_str}
    ],"""
    else:
        datas_section = "    datas=[],"
    
    # Preparar archivo de versión e icono
    version_file = crear_version_file()
    icon_file = resolver_icono()
    exe_name = "SniesManager"
    
    # Hidden imports mejorados y completos
    hidden_imports = [
        # Core Python
        'tkinter', 'tkinter.ttk', 'tkinter.filedialog', 'tkinter.messagebox',
        'json', 'pathlib', 'threading', 'time', 'shutil', 'os', 'sys',
        
        # Data processing
        'pandas', 'numpy', 'pandas._libs.tslibs.timedeltas',
        'pandas._libs.tslibs.nattype', 'pandas._libs.tslibs.np_datetime',
        'pandas._libs.tslibs.tzconversion',
        
        # Excel
        'openpyxl', 'openpyxl.cell._writer', 'openpyxl.workbook',
        'openpyxl.worksheet', 'openpyxl.styles',
        
        # ML
        'sklearn', 'sklearn.ensemble', 'sklearn.ensemble._forest',
        'sklearn.tree', 'sklearn.tree._tree', 'sklearn.utils._weight_vector',
        'sklearn.neighbors.typedefs', 'sklearn.neighbors.quad_tree',
        'sklearn.tree._utils', 'sklearn.metrics', 'sklearn.metrics.pairwise',
        'sklearn.model_selection', 'sklearn.preprocessing',
        
        # Sentence Transformers
        'sentence_transformers', 'sentence_transformers.models',
        'sentence_transformers.util', 'transformers', 'torch',
        'torch.nn', 'torch.nn.functional',
        
        # Text processing
        'unidecode', 'rapidfuzz', 'rapidfuzz.fuzz', 'rapidfuzz.utils',
        
        # Web automation
        'selenium', 'selenium.webdriver', 'selenium.webdriver.chrome',
        'selenium.webdriver.chrome.service', 'selenium.webdriver.common.by',
        'selenium.webdriver.support', 'selenium.webdriver.support.ui',
        'webdriver_manager', 'webdriver_manager.chrome',
        
        # Utilities
        'joblib', 'scipy', 'scipy.sparse.csgraph._validation',
        'pkg_resources.py2_warn', 'yaml',
        
        # ETL modules (imports explícitos para PyInstaller)
        'etl', 'etl.config', 'etl.descargaSNIES', 'etl.normalizacion',
        'etl.normalizacion_final', 'etl.procesamientoSNIES',
        'etl.clasificacionProgramas', 'etl.historicoProgramasNuevos',
        'etl.exportacionPowerBI', 'etl.pipeline_logger',
        'etl.exceptions_helpers',
    ]
    
    hidden_imports_str = ",\n        ".join([f"'{imp}'" for imp in hidden_imports])

    # Configurar según modo
    if modo_onefile:
        # Modo onefile: un solo ejecutable (más fácil distribuir, más lento inicio)
        spec_content = f"""# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['app/main.py'],
    pathex=[],
    binaries=[],
{datas_section}
    hiddenimports=[
{hidden_imports_str}
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=['matplotlib', 'IPython', 'jupyter', 'notebook', 'pytest', 'test'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='{exe_name}',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon={repr(str(icon_file)) if icon_file else 'None'},
    version={repr(str(version_file))},
)
"""
    else:
        # Modo onedir: carpeta con ejecutable + DLLs (más rápido inicio, más fácil debuggear)
        spec_content = f"""# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['app/main.py'],
    pathex=[],
    binaries=[],
{datas_section}
    hiddenimports=[
{hidden_imports_str}
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=['matplotlib', 'IPython', 'jupyter', 'notebook', 'pytest', 'test'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='{exe_name}',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon={repr(str(icon_file)) if icon_file else 'None'},
    version={repr(str(version_file))},
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='{exe_name}',
)
"""
    spec_file = Path("app.spec")
    with open(spec_file, "w", encoding="utf-8") as f:
        f.write(spec_content)
    return spec_file


def copiar_ejecutable_a_raiz(modo_onefile: bool) -> None:
    """
    Copia el ejecutable (y en onedir la carpeta _internal) a la raíz del proyecto
    para que el usuario pueda abrirlo desde ahí sin entrar en dist/.
    La copia es totalmente funcional: el .exe usa la carpeta donde está como base.
    """
    base_path = Path(__file__).parent  # raíz del proyecto
    src_exe = Path("dist") / "SniesManager.exe"
    dst_exe = base_path / "SniesManager.exe"

    if not src_exe.exists():
        print_colored("[ADVERTENCIA] No se encontró dist/SniesManager.exe, no se copia a la raíz.", Colors.YELLOW)
        return

    try:
        shutil.copy2(src_exe, dst_exe)
        print_colored(f"[OK] Copia del ejecutable en la raíz: {dst_exe}", Colors.GREEN)
    except Exception as e:
        print_colored(f"[ADVERTENCIA] No se pudo copiar el .exe a la raíz: {e}", Colors.YELLOW)
        return

    if not modo_onefile:
        src_internal = Path("dist") / "_internal"
        dst_internal = base_path / "_internal"
        if src_internal.exists():
            try:
                if dst_internal.exists():
                    shutil.rmtree(dst_internal)
                shutil.copytree(src_internal, dst_internal)
                print_colored(f"[OK] Carpeta _internal copiada a la raíz (necesaria para el .exe)", Colors.GREEN)
            except Exception as e:
                print_colored(f"[ADVERTENCIA] No se pudo copiar _internal a la raíz: {e}", Colors.YELLOW)
                print_colored("   Puedes ejecutar el .exe desde dist/ o copiar _internal manualmente.", Colors.YELLOW)
        else:
            print_colored("[ADVERTENCIA] No se encontró dist/_internal; la copia en la raíz puede no funcionar en modo onedir.", Colors.YELLOW)

    print_colored("   Puedes usar SniesManager.exe en la raíz del proyecto (ref/, models/, docs/ ya están ahí).", Colors.GREEN)


def construir_exe(modo_onefile: bool = False) -> bool:
    """Construye el ejecutable usando PyInstaller.
    
    Args:
        modo_onefile: Si True, crea ejecutable único. Si False, crea carpeta con DLLs.
    """
    print_colored("\n=== Construyendo ejecutable .EXE ===", Colors.GREEN)
    
    # Validar archivos necesarios
    print_colored("Validando archivos necesarios...", Colors.YELLOW)
    valido, faltantes, advertencias = validar_archivos_necesarios()
    
    if not valido:
        print_colored("\n[ERROR] Faltan archivos requeridos:", Colors.RED)
        for falta in faltantes:
            print_colored(f"  - {falta}", Colors.RED)
        print_colored("\nPor favor, asegúrese de que estos archivos existan antes de empaquetar.", Colors.YELLOW)
        return False
    
    if advertencias:
        print_colored("\n[ADVERTENCIA] Archivos opcionales no encontrados:", Colors.YELLOW)
        for adv in advertencias:
            print_colored(f"  - {adv}", Colors.YELLOW)
        print_colored("El empaquetado continuará, pero algunos features pueden no funcionar.", Colors.YELLOW)
    
    # Crear archivo .spec
    spec_file = crear_spec_file(modo_onefile=modo_onefile)
    modo_str = "onefile (ejecutable único)" if modo_onefile else "onedir (carpeta con DLLs)"
    print_colored(f"Archivo .spec creado: {spec_file} (modo: {modo_str})", Colors.GREEN)
    
    # Ejecutar PyInstaller
    print_colored("Ejecutando PyInstaller (esto puede tardar varios minutos)...", Colors.YELLOW)
    print_colored("Por favor, sea paciente...", Colors.YELLOW)
    try:
        cmd = [
            sys.executable, "-m", "PyInstaller",
            "--clean",
            "--noconfirm",
            str(spec_file)
        ]
        subprocess.check_call(cmd)
        
        # PyInstaller puede crear el ejecutable en diferentes ubicaciones según el modo
        final_exe = Path("dist") / "SniesManager.exe"
        dist_app_folder = Path("dist") / "SniesManager"
        dist_app_exe = dist_app_folder / "SniesManager.exe"
        
        # En modo onefile, el ejecutable está directamente en dist/
        # En modo onedir, está en dist/SniesManager/
        if modo_onefile:
            # Modo onefile: ejecutable único en dist/
            if final_exe.exists():
                print_colored("\n=== Ejecutable creado exitosamente ===", Colors.GREEN)
                print_colored(f"[OK] Ejecutable encontrado en: {final_exe}", Colors.GREEN)
                
                # Copiar carpetas de datos necesarias
                base_path = Path(__file__).parent
                for folder in ["ref", "models", "docs"]:
                    dst_in_dist = Path("dist") / folder
                    if not dst_in_dist.exists():
                        src_folder = base_path / folder
                        if src_folder.exists():
                            shutil.copytree(src_folder, dst_in_dist)
                            print_colored(f"[OK] Carpeta '{folder}' copiada desde proyecto", Colors.GREEN)
                
                print_colored("\n¡Ejecutable creado exitosamente!", Colors.GREEN)
                print_colored(f"Ubicación: {final_exe}", Colors.GREEN)
                print_colored("\n[IMPORTANTE] Para distribuir:", Colors.YELLOW)
                print_colored("  1. Copie el archivo SniesManager.exe", Colors.YELLOW)
                print_colored("  2. Copie las carpetas ref/, models/, docs/ junto al ejecutable", Colors.YELLOW)
                print_colored("  3. El ejecutable creará archivos temporales al ejecutarse", Colors.YELLOW)
                
                print_colored("\n=== Copiando ejecutable a la raíz del proyecto ===", Colors.GREEN)
                copiar_ejecutable_a_raiz(modo_onefile=True)
                return True
            else:
                print_colored(f"\n[ERROR] No se encontró el ejecutable en: {final_exe}", Colors.RED)
                return False
        else:
            # Modo onedir: carpeta con ejecutable + DLLs
            # Verificar ambas ubicaciones posibles
            if final_exe.exists():
                # El ejecutable ya está en dist/SniesManager.exe (caso más común)
                print_colored("\n=== Organizando archivos para distribución ===", Colors.GREEN)
                print_colored(f"[OK] Ejecutable encontrado en: {final_exe}", Colors.GREEN)
                
                # IMPORTANTE: Buscar carpeta _internal en todas las ubicaciones posibles
                internal_folder = Path("dist") / "_internal"
                internal_in_app = dist_app_folder / "_internal"
                
                # Si no está en dist/_internal, buscar en dist/app/_internal y copiarla
                if not internal_folder.exists() and internal_in_app.exists():
                    print_colored("Encontrada carpeta '_internal' en dist/app/, copiando a dist/...", Colors.YELLOW)
                    try:
                        shutil.copytree(internal_in_app, internal_folder)
                        print_colored("[OK] Carpeta '_internal' copiada (contiene DLLs de Python)", Colors.GREEN)
                    except Exception as e:
                        print_colored(f"[ADVERTENCIA] Error al copiar '_internal': {e}", Colors.YELLOW)
                elif internal_folder.exists():
                    print_colored("[OK] Carpeta '_internal' encontrada (contiene DLLs de Python)", Colors.GREEN)
                else:
                    print_colored("[ADVERTENCIA] ADVERTENCIA: No se encontró carpeta '_internal'", Colors.YELLOW)
                    print_colored("   El ejecutable puede estar en modo 'onefile' (todo empaquetado)", Colors.YELLOW)
                    print_colored("   Si el .exe no funciona, puede necesitar reconstruirse", Colors.YELLOW)
                
                # Copiar las carpetas de datos si no existen en dist/
                base_path = Path(__file__).parent
                for folder in ["ref", "models", "docs"]:
                    dst_in_dist = Path("dist") / folder
                    if not dst_in_dist.exists():
                        # Copiar desde el proyecto
                        src_folder = base_path / folder
                        if src_folder.exists():
                            shutil.copytree(src_folder, dst_in_dist)
                            print_colored(f"[OK] Carpeta '{folder}' copiada desde proyecto", Colors.GREEN)
                
                print_colored("\n¡Ejecutable creado exitosamente!", Colors.GREEN)
                print_colored(f"Ubicación: {final_exe}", Colors.GREEN)
                print_colored("\n[IMPORTANTE] Para distribuir, copie TODA la carpeta 'dist/'", Colors.YELLOW)
                print_colored("   El .exe necesita la carpeta '_internal/' junto a él", Colors.YELLOW)
                
                print_colored("\n=== Copiando ejecutable a la raíz del proyecto ===", Colors.GREEN)
                copiar_ejecutable_a_raiz(modo_onefile=False)
                return True
            elif dist_app_exe.exists():
                # El ejecutable está en dist/SniesManager/SniesManager.exe (estructura alternativa)
                print_colored("\n=== Organizando archivos para distribución ===", Colors.GREEN)
                
                # Copiar el ejecutable a dist/
                if final_exe.exists():
                    final_exe.unlink()
                shutil.copy2(str(dist_app_exe), str(final_exe))
                print_colored(f"Ejecutable copiado a: {final_exe}", Colors.GREEN)
                
                # IMPORTANTE: Copiar toda la carpeta _internal (contiene DLLs necesarias)
                internal_folder = dist_app_folder / "_internal"
                if internal_folder.exists():
                    dst_internal = Path("dist") / "_internal"
                    if dst_internal.exists():
                        shutil.rmtree(dst_internal)
                    shutil.copytree(internal_folder, dst_internal)
                    print_colored("[OK] Carpeta '_internal' copiada (contiene DLLs de Python)", Colors.GREEN)
                else:
                    print_colored("[ADVERTENCIA] ADVERTENCIA: No se encontró carpeta '_internal'", Colors.YELLOW)
                
                # Copiar las carpetas de datos si existen en dist/app/
                for folder in ["ref", "models", "docs"]:
                    src_in_app = dist_app_folder / folder
                    dst_in_dist = Path("dist") / folder
                    
                    if src_in_app.exists():
                        # Ya fueron copiadas por PyInstaller desde datas
                        if not dst_in_dist.exists():
                            shutil.copytree(src_in_app, dst_in_dist)
                            print_colored(f"[OK] Carpeta '{folder}' copiada desde app/", Colors.GREEN)
                    else:
                        # Si no están en app/, copiarlas desde el proyecto
                        base_path = Path(__file__).parent
                        src_folder = base_path / folder
                        if src_folder.exists():
                            if dst_in_dist.exists():
                                shutil.rmtree(dst_in_dist)
                            shutil.copytree(src_folder, dst_in_dist)
                            print_colored(f"[OK] Carpeta '{folder}' copiada desde proyecto", Colors.GREEN)
                
                print_colored("\n¡Ejecutable creado exitosamente!", Colors.GREEN)
                print_colored(f"Ubicación: {final_exe}", Colors.GREEN)
                print_colored("\n[IMPORTANTE] Para distribuir, copie TODA la carpeta 'dist/'", Colors.YELLOW)
                print_colored("   El .exe necesita la carpeta '_internal/' junto a él", Colors.YELLOW)
                
                print_colored("\n=== Copiando ejecutable a la raíz del proyecto ===", Colors.GREEN)
                copiar_ejecutable_a_raiz(modo_onefile=False)
                return True
            else:
                print_colored(f"\n[ERROR] No se encontró el ejecutable en ninguna ubicación esperada", Colors.RED)
                print_colored(f"  Buscado en: {final_exe}", Colors.RED)
                print_colored(f"  Buscado en: {dist_app_exe}", Colors.RED)
                print_colored("\nVerifique que PyInstaller se ejecutó correctamente.", Colors.YELLOW)
                return False
            
    except subprocess.CalledProcessError as e:
        print_colored(f"\nError al construir el ejecutable: {e}", Colors.RED)
        return False

def crear_instrucciones(modo_onefile: bool = False) -> None:
    """Crea un archivo README con instrucciones para usar el .EXE.
    
    Args:
        modo_onefile: Si True, ajusta instrucciones para modo onefile.
    """
    modo_texto = "ejecutable único" if modo_onefile else "carpeta con archivos"
    estructura_texto = """SniesManager.exe
ref/ (carpeta con archivos de referencia)
models/ (carpeta con modelos ML, opcional)
docs/ (carpeta con archivos de normalización)""" if modo_onefile else """SniesManager.exe
_internal/ (carpeta con DLLs de Python - REQUERIDA)
ref/ (carpeta con archivos de referencia)
models/ (carpeta con modelos ML)
docs/ (carpeta con archivos de normalización)"""
    
    instrucciones = f"""========================================
  INSTRUCCIONES DE USO - SniesManager
  Sistema de Clasificación de Programas SNIES - EAFIT
========================================

VERSIÓN: {FILE_VERSION}
MODO DE EMPAQUETADO: {modo_texto}

REQUISITOS PREVIOS
------------------
1. Google Chrome instalado: El programa necesita Chrome para descargar 
   los datos de SNIES desde el portal oficial.

2. Sistema Operativo: Windows 10 o superior

3. Estructura de archivos: Debe tener las siguientes carpetas junto al ejecutable:
{estructura_texto}

PRIMERA EJECUCIÓN
------------------
1. Haga doble clic en SniesManager.exe

2. La aplicación le pedirá que seleccione la carpeta raíz del proyecto.
   Esta es la carpeta que contiene:
   - ref/ (archivos de referencia)
   - models/ (modelos de ML, se crearán automáticamente si no existen)
   - docs/ (documentación y archivos de normalización)
   
   Ejemplo: C:\\Users\\usuario\\OneDrive - Universidad EAFIT\\trabajo\\proyectoMejora

3. Una vez seleccionada, la configuración se guardará automáticamente
   en config.json y no se volverá a pedir.

4. Si es la primera vez, el sistema entrenará automáticamente el modelo ML
   (esto puede tardar varios minutos).

USO DIARIO
----------
1. Haga doble clic en SniesManager.exe

2. En el menú principal, presione el botón "Ejecutar Pipeline"

3. Espere a que termine el proceso (puede tardar 3-7 minutos):
   - Descarga de datos SNIES (2-5 min)
   - Normalización y procesamiento (10-30 seg)
   - Clasificación ML (30-60 seg)
   - Exportación Power BI (5-10 seg)

4. Los archivos se guardarán automáticamente en:
   - outputs/Programas.xlsx (archivo principal con todos los programas)
   - outputs/Programas_PowerBI.xlsx (datos preparados para Power BI)
   - outputs/HistoricoProgramasNuevos.xlsx (histórico consolidado)
   - outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx (histórico con fecha)

FUNCIONALIDADES DISPONIBLES
---------------------------
1. Ejecutar Pipeline: Descarga y clasifica programas nuevos automáticamente

2. Ajuste Manual: Revisa y corrige clasificaciones manualmente
   - Edita ES_REFERENTE y programas EAFIT
   - Filtra por programas nuevos, referentes, o todos
   - Busca por código o nombre

3. Reentrenamiento: Edita referentes y reentrena el modelo ML
   - Sincroniza ajustes manuales con archivo de entrenamiento
   - Simula reentrenamiento antes de entrenar
   - Versionado de modelos con rollback

4. Consolidar Archivos: Combina archivos históricos con el actual

5. Ver Logs: Abre el archivo de log para revisar errores

SALIDA
------
Los archivos generados se guardan en la carpeta outputs/:
- Programas.xlsx: Archivo principal con todos los programas y clasificaciones
- Programas_PowerBI.xlsx: Datos preparados para visualización en Power BI
  - Hoja "Datos": Tabla detallada de programas nuevos
  - Hoja "Métricas": Métricas agregadas calculadas
- HistoricoProgramasNuevos.xlsx: Histórico consolidado de programas nuevos
- historico/Programas_YYYYMMDD_HHMMSS.xlsx: Archivos históricos con fecha

Los logs se guardan en: logs/pipeline.log

SOLUCIÓN DE PROBLEMAS
---------------------
❌ Error "Chrome no encontrado"
   → Solución: Instale Google Chrome desde chrome.google.com

❌ Error de permisos al guardar
   → Solución: 
     - Cierre Excel/Power BI si tienen archivos abiertos en outputs/
     - Verifique permisos de escritura en la carpeta del proyecto
     - Ejecute como administrador si es necesario

❌ La aplicación no inicia
   → Solución: 
     - Verifique que todas las carpetas (ref/, models/, docs/) estén presentes
     - En modo onedir, asegúrese de que _internal/ esté junto al .exe
     - Revise logs/pipeline.log para más detalles

❌ Error "ModuleNotFoundError" al ejecutar
   → Solución: 
     - Reconstruya el ejecutable con build_exe.py
     - Verifique que todas las dependencias estén en requirements.txt

❌ El ejecutable es muy grande (>500MB)
   → Es normal. Incluye Python completo, todas las dependencias y modelos ML.

❌ Antivirus bloquea el ejecutable
   → Solución:
     - Agregue excepción en su antivirus
     - En modo onefile, el antivirus puede ser más estricto (use onedir)

NOTAS IMPORTANTES
-----------------
✅ La aplicación NO requiere Python instalado
✅ La aplicación NO requiere instalar librerías manualmente
✅ El proceso se ejecuta típicamente una vez por semana
✅ Todos los archivos se guardan automáticamente en outputs/
✅ Puede cambiar la carpeta del proyecto desde el menú "Configuración"
✅ Los modelos ML se entrenan automáticamente la primera vez
✅ Los ajustes manuales se pueden sincronizar con el archivo de entrenamiento

CONFIGURACIÓN AVANZADA
----------------------
Puede crear un archivo config.json junto al ejecutable para personalizar:

{{
  "base_dir": "ruta/al/proyecto",
  "umbral_referente": 0.70,
  "log_level": "INFO",
  "headless": false,
  "max_wait_download_sec": 180
}}

CONTACTO Y SOPORTE
------------------
Para problemas técnicos, revise:
1. logs/pipeline.log para errores detallados
2. Los mensajes en la ventana de la aplicación
3. Verifique que todas las carpetas requeridas estén presentes

========================================
Versión: {FILE_VERSION}
Fecha de empaquetado: {time.strftime('%Y-%m-%d %H:%M:%S')}
========================================
"""
    
    readme_file = Path("dist") / "INSTRUCCIONES.txt"
    readme_file.parent.mkdir(exist_ok=True)
    with open(readme_file, "w", encoding="utf-8") as f:
        f.write(instrucciones)
    print_colored(f"Instrucciones creadas: {readme_file}", Colors.GREEN)

def main() -> int:
    """Función principal."""
    print_colored("=" * 60, Colors.GREEN)
    print_colored("  CONSTRUCTOR DE EJECUTABLE - Pipeline SNIES", Colors.GREEN)
    print_colored("=" * 60, Colors.RESET)
    
    # Preguntar modo de empaquetado
    print_colored("\nSeleccione el modo de empaquetado:", Colors.YELLOW)
    print_colored("  1. onedir (recomendado): Carpeta con ejecutable + DLLs", Colors.RESET)
    print_colored("     - Más rápido al iniciar", Colors.RESET)
    print_colored("     - Más fácil debuggear problemas", Colors.RESET)
    print_colored("     - Requiere copiar toda la carpeta para distribuir", Colors.RESET)
    print_colored("\n  2. onefile: Un solo ejecutable", Colors.RESET)
    print_colored("     - Más fácil distribuir (solo un archivo)", Colors.RESET)
    print_colored("     - Más lento al iniciar (extrae archivos temporales)", Colors.RESET)
    print_colored("     - Puede tener problemas con antivirus", Colors.RESET)
    
    respuesta = input("\nSeleccione (1=onedir, 2=onefile) [1]: ").strip()
    modo_onefile = respuesta == "2"
    
    if modo_onefile:
        print_colored("Modo seleccionado: onefile (ejecutable único)", Colors.GREEN)
    else:
        print_colored("Modo seleccionado: onedir (carpeta con DLLs) - RECOMENDADO", Colors.GREEN)
    
    # Verificar PyInstaller
    if not verificar_pyinstaller():
        if not instalar_pyinstaller():
            return 1
    
    # Verificar versión de PyInstaller
    try:
        import PyInstaller
        version = PyInstaller.__version__
        print_colored(f"PyInstaller versión: {version}", Colors.GREEN)
        # Recomendar versión mínima
        version_parts = [int(x) for x in version.split('.')[:2]]
        if version_parts < [6, 0]:
            print_colored("[ADVERTENCIA] Se recomienda PyInstaller >= 6.0", Colors.YELLOW)
            print_colored("  Actualice con: pip install --upgrade pyinstaller", Colors.YELLOW)
    except Exception:
        pass
    
    # Limpiar builds anteriores
    limpiar_builds_anteriores()
    
    # Construir ejecutable
    if not construir_exe(modo_onefile=modo_onefile):
        return 1
    
    # Crear instrucciones
    crear_instrucciones(modo_onefile=modo_onefile)
    
    print_colored("\n" + "=" * 60, Colors.GREEN)
    print_colored("  ¡PROCESO COMPLETADO!", Colors.GREEN)
    print_colored("=" * 60, Colors.RESET)
    
    if modo_onefile:
        exe_path = Path("dist") / "SniesManager.exe"
        print_colored(f"\nEl ejecutable está en: {exe_path}", Colors.GREEN)
        print_colored("También hay una copia en la raíz del proyecto; puedes abrirla desde ahí.", Colors.GREEN)
        print_colored("\n[IMPORTANTE] Para distribuir:", Colors.YELLOW)
        print_colored("  1. Copie el archivo SniesManager.exe", Colors.YELLOW)
        print_colored("  2. Asegúrese de que las carpetas ref/, models/, docs/ estén en la misma ubicación", Colors.YELLOW)
        print_colored("  3. El ejecutable creará archivos temporales al ejecutarse", Colors.YELLOW)
    else:
        exe_path = Path("dist") / "SniesManager.exe"
        print_colored(f"\nEl ejecutable está en: {exe_path}", Colors.GREEN)
        print_colored("También hay una copia en la raíz del proyecto (SniesManager.exe + _internal/)", Colors.GREEN)
        print_colored("  -> Puedes abrir SniesManager.exe desde la raíz; ref/, models/, docs/ ya están ahí.", Colors.GREEN)
        print_colored("\n[IMPORTANTE] Para distribuir:", Colors.YELLOW)
        print_colored("  1. Copie TODA la carpeta 'dist/' completa", Colors.YELLOW)
        print_colored("  2. Incluya: SniesManager.exe, _internal/, ref/, models/, docs/", Colors.YELLOW)
        print_colored("  3. Mantenga todas las carpetas juntas (el .exe necesita _internal/)", Colors.YELLOW)
    
    print_colored("\nRevisa dist/INSTRUCCIONES.txt para más información.", Colors.YELLOW)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

