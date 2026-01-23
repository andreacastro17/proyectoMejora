import os
import time
import glob
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from etl.pipeline_logger import log_error, log_info
from etl.config import (
    SNIES_URL,
    DOWNLOAD_DIR,
    HISTORIC_DIR,
    HEADLESS,
    MAX_WAIT_DOWNLOAD_SEC,
    RENAME_TO,
)

def _configure_chrome(download_dir: str, headless: bool = True):
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    if headless:
        options.add_argument("--headless=new")
    # Flags de robustez
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # PageLoadStrategy más rápida
    options.page_load_strategy = "eager"
    return options

def _mover_archivo_existente() -> None:
    """
    Verifica si existe Programas.xlsx en el directorio de outputs.
    Si existe, lo mueve al directorio histórico con la fecha de descarga original.
    Si no existe, no hace nada.
    Este proceso SIEMPRE se ejecuta antes del web scraping.
    """
    # Convertir Path a string para compatibilidad con os.path.join
    download_dir_str = str(DOWNLOAD_DIR)
    historic_dir_str = str(HISTORIC_DIR)
    
    archivo_programas = os.path.join(download_dir_str, f"{RENAME_TO}.xlsx")
    
    if not os.path.exists(archivo_programas):
        print(f"No existe archivo {RENAME_TO}.xlsx en el directorio. Continuando con descarga...")
        return
    
    print(f"Archivo {RENAME_TO}.xlsx encontrado. Moviendo a histórico...")
    
    # Obtener la fecha de modificación del archivo (fecha de descarga original)
    fecha_modificacion = os.path.getmtime(archivo_programas)
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime(fecha_modificacion))
    
    ext = Path(archivo_programas).suffix
    base_name = f"{RENAME_TO}_{timestamp}{ext}"
    destination = os.path.join(historic_dir_str, base_name)
    
    # Si ya existe un archivo con ese nombre, agregar contador
    counter = 1
    while os.path.exists(destination):
        destination = os.path.join(historic_dir_str, f"{RENAME_TO}_{timestamp}_{counter}{ext}")
        counter += 1
    
    try:
        os.replace(archivo_programas, destination)
        nombre_archivo_historico = os.path.basename(destination)
        print(f"  -> {RENAME_TO}.xlsx movido a histórico: {nombre_archivo_historico}")
        log_info(f"Archivo movido a histórico: {nombre_archivo_historico}")
    except OSError as e:
        error_msg = f"No se pudo mover {RENAME_TO}.xlsx al histórico: {e}"
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        raise


def _wait_for_download(dirpath: str, before_set: set[str], timeout_sec: int) -> str:
    """
    Espera a que aparezca un nuevo archivo no .crdownload y a que su tamaño se estabilice.
    Devuelve la ruta final. Detecta también archivos con nombres numerados.
    """
    end_time = time.time() + timeout_sec
    candidate: str | None = None

    def size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return -1

    while time.time() < end_time:
        after = set(os.listdir(dirpath))
        new_files = list(after - before_set)
        if new_files:
            # Preferir Excel o CSV
            candidates = [os.path.join(dirpath, f) for f in new_files]
            # Si hay .crdownload, espero a que desaparezca
            finalized = [c for c in candidates if not c.endswith(".crdownload")]
            
            # Priorizar archivos que empiezan con RENAME_TO (incluyendo variantes numeradas)
            prioritized = [c for c in finalized if os.path.basename(c).startswith("Programas")]
            if prioritized:
                finalized = prioritized
            
            if finalized:
                # Tomar el más reciente
                candidate = max(finalized, key=os.path.getmtime)
                # Esperar estabilización de tamaño (2 ciclos)
                last_size = size(candidate)
                time.sleep(1.5)
                curr_size = size(candidate)
                if curr_size > 0 and curr_size == last_size:
                    return candidate
        time.sleep(1.0)
    raise TimeoutError("No se detectó un archivo descargado listo a tiempo.")


def main() -> str | None:
    """
    Proceso principal:
    1. Verificar si existe Programas.xlsx → moverlo a histórico (si existe)
    2. Limpiar descargas parciales
    3. Iniciar web scraping y descargar nuevo archivo
    """
    final_path: str | None = None
    
    # === PASO 1: Verificar y mover archivo existente (ANTES del web scraping) ===
    print("=== Paso 1: Verificando archivo existente ===")
    try:
        _mover_archivo_existente()
    except Exception as e:
        print(f"[ERROR] Falló el proceso de mover archivo existente: {e}")
        return None
    
    # Convertir Path a string una sola vez para usar en toda la función
    # Esto evita problemas de serialización JSON con Selenium y compatibilidad con os.path
    download_dir_str = str(DOWNLOAD_DIR)
    
    # Limpiar descargas parciales (archivos temporales .crdownload) antes de iniciar el navegador
    # NOTA: Esto NO elimina Programas.xlsx, solo archivos temporales de descargas incompletas
    print("Limpiando descargas parciales (archivos temporales .crdownload)...")
    for f in glob.glob(os.path.join(download_dir_str, "*.crdownload")):
        try:
            os.remove(f)
        except OSError:
            pass
    
    # === PASO 2: Web scraping y descarga ===
    print("=== Paso 2: Iniciando web scraping ===")
    options = _configure_chrome(download_dir_str, HEADLESS)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 120)

    try:
        print("Abriendo la página...")
        driver.get(SNIES_URL)

        print("Esperando a que cargue la tabla o el contenedor principal...")
        wait.until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='table']")),
                EC.presence_of_element_located((By.XPATH, "//table"))
            )
        )

        before = set(os.listdir(download_dir_str))

        print("Buscando botón de descarga...")
        # Variantes del botón (texto y/o clases)
        try:
            download_btn = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        "//*[self::button or self::a][contains(translate(., 'DESCARGARPROGRAMAS', 'descargarprogramas'), 'descargar')"
                        " and contains(translate(., 'DESCARGARPROGRAMAS', 'descargarprogramas'), 'programas')]"
                    )
                )
            )
        except Exception:
            # Fallback por clase típica
            try:
                download_btn = wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[contains(@class,'btn') and contains(@class,'success')]")
                    )
                )
            except Exception as e:
                error_msg = "No se encontró el botón de descarga."
                print(f"[ERROR] {error_msg}")
                log_error(error_msg)
                raise

        print("Haciendo click en descargar...")
        driver.execute_script("arguments[0].click();", download_btn)

        print("Esperando la descarga...")
        downloaded_file = _wait_for_download(download_dir_str, before, MAX_WAIT_DOWNLOAD_SEC)
        
        downloaded_name = os.path.basename(downloaded_file)
        print(f"Archivo descargado detectado: {downloaded_name}")
        log_info(f"Archivo descargado detectado: {downloaded_name}")

        ext = Path(downloaded_file).suffix
        expected_path = os.path.join(download_dir_str, f"{RENAME_TO}{ext}")
        final_path = expected_path

        # Si el archivo descargado ya tiene el nombre correcto, no necesitamos hacer nada
        if downloaded_file == expected_path:
            nombre_final = os.path.basename(expected_path)
            print(f"  -> El archivo ya tiene el nombre correcto: {nombre_final}")
        else:
            # Si Chrome creó un archivo numerado (ej: "Programas (1).xlsx"), renombrarlo
            print(f"  -> Renombrando {downloaded_name} a {RENAME_TO}{ext}...")
            # Si por alguna razón el archivo objetivo todavía existe, eliminarlo primero
            if os.path.exists(expected_path):
                try:
                    os.remove(expected_path)
                    print(f"  -> Eliminado archivo existente antes de renombrar: {os.path.basename(expected_path)}")
                except OSError as e:
                    print(f"[WARN] No se pudo eliminar el archivo existente {expected_path}: {e}")
            
            # Intentar reemplazo directo
            try:
                os.replace(downloaded_file, expected_path)
                print(f"  -> Archivo renombrado exitosamente a {os.path.basename(expected_path)}")
            except OSError as e:
                print(f"[WARN] Al renombrar el archivo descargado: {e}")
                # Intentar copia manual y eliminación del original
                try:
                    # Si el archivo objetivo todavía existe, eliminarlo primero
                    if os.path.exists(expected_path):
                        os.remove(expected_path)
                    with open(downloaded_file, "rb") as src, open(expected_path, "wb") as dst:
                        dst.write(src.read())
                    os.remove(downloaded_file)
                    print(f"  -> Archivo copiado exitosamente a {os.path.basename(expected_path)}")
                except Exception as copy_err:
                    print(f"[ERROR] No se pudo copiar el archivo descargado sobre {expected_path}: {copy_err}")
                    final_path = downloaded_file  # usar el archivo tal como quedó

        if final_path:
            nombre_final = os.path.basename(final_path)
            print(f"Archivo descargado: {nombre_final}")
            print("¡Descarga exitosa! Archivo actualizado:")
            print(f"  - {nombre_final}")
            log_info(f"Descarga finalizada: {nombre_final}")
        else:
            error_msg = "La descarga finalizó, pero no se pudo determinar el archivo final."
            print(error_msg)
            log_error(error_msg)
        return final_path

    except Exception as e:
        error_msg = str(e)
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        try:
            screenshot_path = os.path.join(download_dir_str, "error_screenshot.png")
            driver.save_screenshot(screenshot_path)
            print(f"Screenshot guardado en: {screenshot_path}")
            log_info(f"Screenshot de error guardado: {screenshot_path}")
        except Exception:
            pass
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return final_path


if __name__ == "__main__":
    main()
