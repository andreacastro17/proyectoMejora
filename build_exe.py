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

def crear_spec_file() -> Path:
    """Crea un archivo .spec personalizado para PyInstaller."""
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
    
    spec_content = f"""# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['app/main.py'],
    pathex=[],
    binaries=[],
{datas_section}
    hiddenimports=[
        'selenium',
        'webdriver_manager',
        'pandas',
        'openpyxl',
        'sentence_transformers',
        'sklearn',
        'unidecode',
        'rapidfuzz',
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'joblib',
        'numpy',
        'scipy',
        'pkg_resources.py2_warn',
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[],
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
    name='app',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # Desactivar UPX puede ayudar con DLLs
    console=False,  # Sin consola (GUI)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='app',
)
"""
    spec_file = Path("app.spec")
    with open(spec_file, "w", encoding="utf-8") as f:
        f.write(spec_content)
    return spec_file

def construir_exe() -> bool:
    """Construye el ejecutable usando PyInstaller."""
    print_colored("\n=== Construyendo ejecutable .EXE ===", Colors.GREEN)
    
    # Crear archivo .spec
    spec_file = crear_spec_file()
    print_colored(f"Archivo .spec creado: {spec_file}", Colors.GREEN)
    
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
        
        # PyInstaller puede crear el ejecutable directamente en dist/app.exe
        # o en dist/app/app.exe dependiendo de la configuración
        final_exe = Path("dist") / "app.exe"
        dist_app_folder = Path("dist") / "app"
        dist_app_exe = dist_app_folder / "app.exe"
        
        # Verificar ambas ubicaciones posibles
        if final_exe.exists():
            # El ejecutable ya está en dist/app.exe (caso más común)
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
            print_colored("\n[ADVERTENCIA] IMPORTANTE: Para distribuir, copie TODA la carpeta 'dist/'", Colors.YELLOW)
            print_colored("   El .exe necesita la carpeta '_internal/' junto a él", Colors.YELLOW)
            
            return True
        elif dist_app_exe.exists():
            # El ejecutable está en dist/app/app.exe (estructura alternativa)
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
            print_colored("\n[ADVERTENCIA] IMPORTANTE: Para distribuir, copie TODA la carpeta 'dist/'", Colors.YELLOW)
            print_colored("   El .exe necesita la carpeta '_internal/' junto a él", Colors.YELLOW)
            
            return True
        else:
            print_colored(f"\nError: No se encontró el ejecutable en ninguna ubicación esperada", Colors.RED)
            print_colored(f"  Buscado en: {final_exe}", Colors.RED)
            print_colored(f"  Buscado en: {dist_app_exe}", Colors.RED)
            return False
            
    except subprocess.CalledProcessError as e:
        print_colored(f"\nError al construir el ejecutable: {e}", Colors.RED)
        return False

def crear_instrucciones() -> None:
    """Crea un archivo README con instrucciones para usar el .EXE."""
    instrucciones = """========================================
  INSTRUCCIONES DE USO - Pipeline SNIES
========================================

REQUISITOS PREVIOS
------------------
1. Google Chrome instalado: El programa necesita Chrome para descargar 
   los datos de SNIES.

2. Carpeta del proyecto: Debe tener las siguientes carpetas:
   - ref/ (archivos de referencia)
   - models/ (modelos de ML)
   - docs/ (documentación y archivos de normalización)

PRIMERA EJECUCIÓN
------------------
1. Haga doble clic en app.exe

2. La aplicación le pedirá que seleccione la carpeta raíz del proyecto.
   Esta es la carpeta que contiene:
   - ref/
   - models/
   - docs/
   
   Ejemplo: C:\\Users\\usuario\\OneDrive - Universidad EAFIT\\trabajo\\proyectoMejora

3. Una vez seleccionada, la configuración se guardará automáticamente
   y no se volverá a pedir.

USO DIARIO
----------
1. Haga doble clic en app.exe

2. Presione el botón "Ejecutar Pipeline"

3. Espere a que termine el proceso (puede tardar varios minutos)

4. Los archivos se guardarán automáticamente en:
   - outputs/HistoricoProgramasNuevos.xlsx
   - outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx

SALIDA
------
Los archivos generados se guardan en:
- outputs/HistoricoProgramasNuevos.xlsx (archivo principal)
- outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx (histórico con fecha)

Los logs se guardan en: logs/pipeline.log

SOLUCIÓN DE PROBLEMAS
---------------------
- Error "Chrome no encontrado": Asegúrese de tener Google Chrome instalado

- Error de permisos: Verifique que tenga permisos de escritura en la 
  carpeta del proyecto

- La aplicación no inicia: Asegúrese de que todas las carpetas (ref/, 
  models/, docs/) estén en la misma ubicación que app.exe

NOTAS IMPORTANTES
-----------------
- La aplicación NO requiere Python instalado
- La aplicación NO requiere instalar librerías
- El proceso se ejecuta una vez por semana típicamente
- Todos los archivos se guardan automáticamente en la carpeta outputs/
- Puede cambiar la carpeta del proyecto usando el botón "Cambiar Carpeta"
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
    
    # Verificar PyInstaller
    if not verificar_pyinstaller():
        if not instalar_pyinstaller():
            return 1
    
    # Limpiar builds anteriores
    limpiar_builds_anteriores()
    
    # Construir ejecutable
    if not construir_exe():
        return 1
    
    # Crear instrucciones
    crear_instrucciones()
    
    print_colored("\n" + "=" * 60, Colors.GREEN)
    print_colored("  ¡PROCESO COMPLETADO!", Colors.GREEN)
    print_colored("=" * 60, Colors.RESET)
    print_colored("\nEl ejecutable está en: dist/app.exe", Colors.GREEN)
    print_colored("Carpetas copiadas: _internal/, ref/, models/, docs/", Colors.GREEN)
    print_colored("\n[ADVERTENCIA] IMPORTANTE - Para distribuir:", Colors.YELLOW)
    print_colored("  1. Copie TODA la carpeta 'dist/' completa", Colors.YELLOW)
    print_colored("  2. Incluya: app.exe, _internal/, ref/, models/, docs/", Colors.YELLOW)
    print_colored("  3. Mantenga todas las carpetas juntas (el .exe necesita _internal/)", Colors.YELLOW)
    print_colored("  4. Ejecute app.exe desde esa carpeta", Colors.YELLOW)
    print_colored("\nRevisa dist/INSTRUCCIONES.txt para más información.", Colors.YELLOW)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

