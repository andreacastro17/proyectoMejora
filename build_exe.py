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
        'etl.pipeline_logger', 'etl.exceptions_helpers',
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


def _forzar_eliminacion(carpeta: Path) -> bool:
    """
    Elimina una carpeta de forma agresiva en Windows:
    fuerza permisos de escritura en cada archivo antes de borrar.
    Retorna True si tuvo éxito.
    """
    import stat

    def _on_error(func, path, exc_info):
        """Callback: quita readonly y reintenta."""
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception:
            pass

    max_intentos = 4
    for intento in range(max_intentos):
        try:
            if intento > 0:
                time.sleep(1.5)
                print_colored(
                    f"  Reintento {intento}/{max_intentos - 1} eliminando SniesManager/...",
                    Colors.YELLOW,
                )
            shutil.rmtree(carpeta, onerror=_on_error)
            if not carpeta.exists():
                return True
        except Exception:
            pass

    # Último recurso: eliminar archivo por archivo
    if carpeta.exists():
        try:
            import stat
            for f in carpeta.rglob("*"):
                try:
                    if f.is_file():
                        os.chmod(f, stat.S_IWRITE)
                        f.unlink(missing_ok=True)
                except Exception:
                    pass
            for d in sorted(carpeta.rglob("*"), reverse=True):
                try:
                    if d.is_dir():
                        d.rmdir()
                except Exception:
                    pass
            carpeta.rmdir()
        except Exception:
            pass

    return not carpeta.exists()


def crear_carpeta_distribucion(modo_onefile: bool) -> bool:
    """
    Crea la carpeta SniesManager/ lista para subir a OneDrive/SharePoint.
    Borra la versión anterior completamente antes de recrearla.
    """
    print_colored(
        "\n=== Creando carpeta de distribución SniesManager/ ===", Colors.GREEN
    )

    base_path = Path(__file__).parent
    dest = base_path / "SniesManager"

    # ── Eliminar versión anterior completamente ───────────────────────────────
    if dest.exists():
        print_colored(
            "  Eliminando versión anterior de SniesManager/ (esto puede tardar)...",
            Colors.YELLOW,
        )
        eliminado = _forzar_eliminacion(dest)
        if eliminado:
            print_colored("  [OK] Versión anterior eliminada.", Colors.GREEN)
        else:
            print_colored(
                "  [ERROR CRÍTICO] No se pudo eliminar SniesManager/.\n"
                "  → Cierra el Explorador de Windows si tienes esa carpeta abierta.\n"
                "  → Cierra OneDrive o pausa la sincronización.\n"
                "  → Cierra cualquier archivo de SniesManager/ abierto en Excel.\n"
                "  → Luego vuelve a ejecutar el build.",
                Colors.RED,
            )
            return False

    # ── Crear estructura limpia ───────────────────────────────────────────────
    dest.mkdir(parents=True)
    ok_count = 0
    err_count = 0

    # 1. Ejecutable
    src_exe = Path("dist") / "SniesManager.exe"
    if src_exe.exists():
        try:
            shutil.copy2(src_exe, dest / "SniesManager.exe")
            print_colored("  [OK] SniesManager.exe", Colors.GREEN)
            ok_count += 1
        except Exception as e:
            print_colored(f"  [ERROR] SniesManager.exe: {e}", Colors.RED)
            err_count += 1
    else:
        print_colored("  [ERROR] No se encontró dist/SniesManager.exe", Colors.RED)
        err_count += 1

    # 2. _internal/ (solo onedir)
    if not modo_onefile:
        src_internal = Path("dist") / "_internal"
        if src_internal.exists():
            try:
                shutil.copytree(src_internal, dest / "_internal")
                print_colored("  [OK] _internal/", Colors.GREEN)
                ok_count += 1
            except Exception as e:
                print_colored(f"  [ERROR] _internal/: {e}", Colors.RED)
                err_count += 1
        else:
            print_colored("  [ERROR] No se encontró dist/_internal/", Colors.RED)
            err_count += 1

    # 3. ref/, models/, docs/
    for carpeta in ["ref", "models", "docs"]:
        src = base_path / carpeta
        if src.exists():
            try:
                shutil.copytree(src, dest / carpeta)
                print_colored(f"  [OK] {carpeta}/", Colors.GREEN)
                ok_count += 1
            except Exception as e:
                print_colored(f"  [ERROR] {carpeta}/: {e}", Colors.RED)
                err_count += 1
        else:
            print_colored(
                f"  [OMITIDO] {carpeta}/ no existe en el proyecto", Colors.YELLOW
            )

    # 4. outputs/ — copiar contenido real excluyendo temp/ y _staging/
    src_outputs = base_path / "outputs"
    dst_outputs = dest / "outputs"
    if src_outputs.exists():
        try:
            shutil.copytree(
                src_outputs,
                dst_outputs,
                ignore=shutil.ignore_patterns(
                    "temp", "temp/*", "_staging", "_staging/*"
                ),
            )
            # Recrear temp/ y _staging/ vacíos (el pipeline los necesita)
            (dst_outputs / "temp").mkdir(exist_ok=True)
            (dst_outputs / "_staging").mkdir(exist_ok=True)
            print_colored(
                "  [OK] outputs/ (con contenido, sin temp/ ni _staging/)", Colors.GREEN
            )
            ok_count += 1
        except Exception as e:
            print_colored(f"  [ERROR] outputs/: {e}", Colors.RED)
            for sub in [
                "outputs",
                "outputs/estudio_de_mercado",
                "outputs/historico",
                "outputs/temp",
                "outputs/_staging",
            ]:
                (dest / sub).mkdir(parents=True, exist_ok=True)
            err_count += 1
    else:
        for sub in [
            "outputs",
            "outputs/estudio_de_mercado",
            "outputs/historico",
            "outputs/temp",
            "outputs/_staging",
        ]:
            (dest / sub).mkdir(parents=True, exist_ok=True)
        print_colored(
            "  [OK] outputs/ (vacío — no existía en el proyecto)", Colors.YELLOW
        )
        ok_count += 1

    # 5. logs/
    (dest / "logs").mkdir(exist_ok=True)
    print_colored("  [OK] logs/", Colors.GREEN)
    ok_count += 1

    # 6. config.json
    config_src = base_path / "config.json"
    config_example = base_path / "config.json.example"
    config_dst = dest / "config.json"
    if config_src.exists():
        try:
            shutil.copy2(config_src, config_dst)
            print_colored("  [OK] config.json", Colors.GREEN)
            ok_count += 1
        except Exception as e:
            print_colored(f"  [ERROR] config.json: {e}", Colors.RED)
            err_count += 1
    elif config_example.exists():
        try:
            shutil.copy2(config_example, config_dst)
            print_colored(
                "  [OK] config.json (desde config.json.example)", Colors.GREEN
            )
            ok_count += 1
        except Exception as e:
            print_colored(f"  [ERROR] config.json: {e}", Colors.RED)
            err_count += 1
    else:
        config_dst.write_text("{}", encoding="utf-8")
        print_colored("  [OK] config.json (creado vacío)", Colors.GREEN)
        ok_count += 1

    # ── Resumen ───────────────────────────────────────────────────────────────
    total_bytes = sum(f.stat().st_size for f in dest.rglob("*") if f.is_file())
    total_mb = total_bytes / (1024 * 1024)
    print_colored(f"\n  Tamaño total: {total_mb:.0f} MB", Colors.GREEN)
    print_colored(
        f"  Resultado: {ok_count} OK, {err_count} errores", Colors.GREEN
    )
    print_colored(f"  Carpeta lista en: {dest.resolve()}", Colors.GREEN)

    return err_count == 0


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
                
                return crear_carpeta_distribucion(modo_onefile=True)
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
                
                return crear_carpeta_distribucion(modo_onefile=False)
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
                
                return crear_carpeta_distribucion(modo_onefile=False)
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
    """Crea INSTRUCCIONES.txt dentro de SniesManager/ (paquete de distribución)."""
    nota_modo = (
        "Este paquete es modo onedir: la carpeta _internal/ es obligatoria junto a "
        "SniesManager.exe; sin ella el programa no inicia."
        if not modo_onefile
        else "Este paquete es modo onefile: no hay carpeta _internal/; las dependencias "
        "van dentro del ejecutable. Mantén igualmente ref/, models/ y docs/ disponibles "
        "localmente (prioridad alta/medio según corresponda)."
    )
    instrucciones = f"""
================================================================================
INSTRUCCIONES DE CONFIGURACIÓN - SNIESMANAGER v{FILE_VERSION}
Fecha de empaquetado: {time.strftime('%Y-%m-%d %H:%M:%S')}
================================================================================

Para que la aplicación funcione correctamente desde OneDrive/SharePoint,
configura la sincronización según las siguientes prioridades.

PASOS PARA CONFIGURAR:
1. Navega a esta carpeta SniesManager/ en el Explorador de Windows.
2. Selecciona los elementos indicados como PRIORIDAD ALTA.
3. Clic derecho → "Mantener siempre en este dispositivo".
   El ícono debe mostrar un círculo verde con marca blanca (✓), no una nube.

--------------------------------------------------------------------------------
[PRIORIDAD ALTA - "Mantener siempre en este dispositivo"]
Deben residir físicamente en tu equipo para que el ejecutable funcione.

    - SniesManager.exe    (Ejecutable principal)
    - _internal/          (Librerías y dependencias de Python - CRÍTICO en modo onedir)
    - ref/                (Archivos de referencia del pipeline)
    - models/             (Modelos ML entrenados)
    - outputs/            (Carpeta de resultados del pipeline)
    - config.json         (Configuración: SMLMV, benchmarks de costo)

[PRIORIDAD MEDIA - Preferiblemente local]
Se lee al iniciar la aplicación; si está solo en la nube puede causar
lentitud al arrancar.

    - docs/               (Archivo de normalización final)

[PRIORIDAD BAJA - Puede quedar solo en línea]
No bloquea la ejecución del pipeline.

    - logs/               (Registros en logs/pipeline.log; se generan al ejecutar)

--------------------------------------------------------------------------------
Nota sobre el modo de empaquetado:
{nota_modo}

PRIMERA EJECUCIÓN
-----------------
1. Doble clic en SniesManager.exe dentro de esta carpeta SniesManager/.
2. Cuando la app pida la carpeta del proyecto, puedes indicar la misma carpeta
   SniesManager/ (contiene ref/, models/, docs/, outputs/, config.json) o la
   raíz de tu copia de trabajo si prefieres separar datos del instalador.
3. Google Chrome debe estar instalado para las descargas desde el portal SNIES.

Más detalle técnico y solución de problemas: consulta la documentación del
proyecto o logs/pipeline.log si algo falla.

================================================================================
"""
    
    readme_file = Path("SniesManager") / "INSTRUCCIONES.txt"
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
    
    dist_exe = Path("dist") / "SniesManager.exe"
    paquete = Path("SniesManager")
    print_colored(f"\nArtefacto PyInstaller (referencia): {dist_exe}", Colors.GREEN)
    print_colored(f"Paquete listo para OneDrive/SharePoint: {paquete.resolve()}", Colors.GREEN)
    if modo_onefile:
        print_colored("\n[IMPORTANTE] Para distribuir:", Colors.YELLOW)
        print_colored("  1. Sube o copia toda la carpeta SniesManager/", Colors.YELLOW)
        print_colored("  2. Incluye SniesManager.exe, ref/, models/, docs/, outputs/, logs/, config.json", Colors.YELLOW)
        print_colored("  3. En onefile el .exe extrae dependencias al ejecutarse; puede ser más lento al abrir", Colors.YELLOW)
    else:
        print_colored("\n[IMPORTANTE] Para distribuir:", Colors.YELLOW)
        print_colored("  1. Sube o copia toda la carpeta SniesManager/", Colors.YELLOW)
        print_colored("  2. Debe incluir SniesManager.exe y _internal/ (obligatorio en onedir)", Colors.YELLOW)
        print_colored("  3. Mantén ref/, models/, docs/ y el resto junto al ejecutable", Colors.YELLOW)

    print_colored("\nRevisa SniesManager/INSTRUCCIONES.txt para sincronización en OneDrive.", Colors.YELLOW)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

