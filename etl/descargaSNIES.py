import glob
import os
import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd

from etl.pipeline_logger import log_error, log_info
from etl.config import (
    HOJA_PROGRAMAS,
    SNIES_URL,
    DOWNLOAD_DIR,
    HISTORIC_DIR,
    HEADLESS,
    MAX_WAIT_DOWNLOAD_SEC,
    RENAME_TO,
    DOWNLOAD_RETRIES,
    SELENIUM_PAGE_LOAD_TIMEOUT_SEC,
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

def _mover_archivo_existente(archivo_programas: str) -> None:
    """
    Mueve un Programas.xlsx existente al directorio histórico, usando la fecha de
    modificación como timestamp.

    Este método NO debe llamarse antes de confirmar que existe una "nueva" versión
    lista para reemplazarla. Así evitamos modificar archivos si fallan todas las fuentes.
    
    Raises:
        PermissionError: Si el archivo está abierto en otro programa (Excel, etc.)
        OSError: Si hay otro error al mover el archivo
    """
    historic_dir_str = str(HISTORIC_DIR)
    
    # Asegurar que el directorio histórico existe
    Path(historic_dir_str).mkdir(parents=True, exist_ok=True)

    if not os.path.exists(archivo_programas):
        print(f"No existe archivo {os.path.basename(archivo_programas)} en el directorio. Continuando...")
        return
    
    print(f"Archivo {os.path.basename(archivo_programas)} encontrado. Moviendo a histórico...")
    log_info(f"Movimiento a histórico iniciado: {os.path.basename(archivo_programas)}")
    
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
        # Intentar mover el archivo
        os.replace(archivo_programas, destination)
        nombre_archivo_historico = os.path.basename(destination)
        print(f"  ✓ Archivo movido exitosamente a histórico: {nombre_archivo_historico}")
        print(f"  → Ubicación: {destination}")
        log_info(f"Archivo movido a histórico: {nombre_archivo_historico} → {destination}")
    except PermissionError as e:
        # Re-lanzar PermissionError para que el llamador pueda manejarlo (intentar copiar)
        error_msg = (
            f"No se pudo mover archivo al histórico ({os.path.basename(archivo_programas)}): "
            f"El archivo está abierto en otro programa (Excel, etc.). "
            f"Por favor, ciérralo e intenta de nuevo. Detalle: {e}"
        )
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        raise PermissionError(error_msg) from e
    except OSError as e:
        error_msg = f"No se pudo mover archivo al histórico ({os.path.basename(archivo_programas)}): {e}"
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        raise


def _commit_programas(staged_file: Path, fuente: str) -> str | None:
    """
    "Commit" transaccional del Programas.xlsx:
    - Si todo está OK, reemplaza outputs/Programas.xlsx por staged_file
    - Si existía un Programas.xlsx previo, lo mueve a histórico
    - Si algo falla al reemplazar, restaura el archivo previo

    Si falla ANTES del commit (SNIES), NO modifica nada.
    """
    try:
        if not staged_file.exists():
            raise FileNotFoundError(f"Staged file no existe: {staged_file}")

        dest = Path(DOWNLOAD_DIR) / f"{RENAME_TO}.xlsx"
        dest_str = str(dest)

        # Backup reversible con extensión .xlsx para que el histórico siga siendo detectable
        # por los módulos que buscan *.xlsx en outputs/historico/.
        backup_path = dest.parent / f"{dest.stem}__previous{dest.suffix}"
        backup_str = str(backup_path)

        # Paso 1: si hay archivo actual, moverlo a backup (reversible)
        if dest.exists():
            # Limpiar backup viejo si quedó por un error anterior
            if backup_path.exists():
                try:
                    os.remove(backup_str)
                except OSError:
                    pass
            os.replace(dest_str, backup_str)

        # Paso 2: mover staged -> destino (reversible si falla)
        try:
            os.replace(str(staged_file), dest_str)
        except Exception as e:
            # Restaurar el backup si existía (CRÍTICO: asegurar que se restaure)
            if backup_path.exists():
                try:
                    os.replace(backup_str, dest_str)
                    log_error(f"Archivo restaurado desde backup tras fallo en commit: {e}")
                except Exception as restore_err:
                    log_error(f"ERROR CRÍTICO: No se pudo restaurar backup tras fallo en commit: {restore_err}")
                    # Intentar copiar en lugar de reemplazar como último recurso
                    try:
                        import shutil
                        shutil.copy2(backup_str, dest_str)
                        log_error("Backup restaurado usando copia (último recurso)")
                    except Exception:
                        pass
            raise

        # Paso 3: si había backup, enviarlo a histórico (si falla, lo dejamos al lado y avisamos)
        if backup_path.exists():
            try:
                _mover_archivo_existente(backup_str)
            except Exception as exc:
                log_error(f"No se pudo mover backup a histórico: {exc}. Se dejó como {backup_path.name}")

        log_info(f"Fuente usada: {fuente}")
        return str(dest)
    except Exception as exc:
        log_error(
            "Fallo al confirmar actualización de Programas.xlsx. "
            f"No se aplicaron cambios permanentes (o se intentó restaurar). Detalle: {exc}"
        )
        return None


def _wait_for_download(dirpath: str, before_set: set[str], timeout_sec: int, cancel_event=None) -> str:
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
        # Verificar cancelación periódicamente
        if cancel_event and cancel_event.is_set():
            raise RuntimeError("Cancelado por el usuario")
        
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
                # Esperar estabilización de tamaño (2 ciclos) con verificación de cancelación
                last_size = size(candidate)
                # Sleep en chunks pequeños para verificar cancelación
                for _ in range(3):  # 1.5s = 3 * 0.5s
                    if cancel_event and cancel_event.is_set():
                        raise RuntimeError("Cancelado por el usuario")
                    time.sleep(0.5)
                curr_size = size(candidate)
                if curr_size > 0 and curr_size == last_size:
                    return candidate
        # Sleep en chunks pequeños para verificar cancelación más frecuentemente
        if cancel_event and cancel_event.is_set():
            raise RuntimeError("Cancelado por el usuario")
        time.sleep(1.0)
    raise TimeoutError("No se detectó un archivo descargado listo a tiempo.")


def main(log_callback=None, cancel_event=None) -> str | None:
    """
    Proceso principal:
    Estrategia "sin cambios si todo falla":
    - Primero intenta obtener una nueva versión en un directorio temporal (staging).
    - Solo si la descarga SNIES produce un archivo listo, reemplaza outputs/Programas.xlsx
      y envía el anterior a histórico.
    - Si falla la descarga SNIES, NO modifica ningún archivo.
    
    Args:
        log_callback: Función opcional para recibir mensajes de log (para mostrar en GUI).
                      Si se proporciona, se llamará con cada mensaje de progreso.
        cancel_event: threading.Event opcional. Si está establecido (set), cancela la descarga.
    """
    def log(msg: str):
        if log_callback:
            try:
                log_callback(msg)
            except Exception:
                pass
        print(msg)
    
    final_path: str | None = None

    # Directorio temporal para staging (evita tocar outputs/Programas.xlsx si todo falla)
    staging_dir = Path(DOWNLOAD_DIR) / "_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Limpiar descargas parciales SOLO en staging (no en outputs)
    staging_dir_str = str(staging_dir)
    log("Limpiando descargas parciales en staging (archivos temporales .crdownload)...")
    for f in glob.glob(os.path.join(staging_dir_str, "*.crdownload")):
        try:
            os.remove(f)
        except OSError:
            pass
    
    selenium_error: str | None = None
    retry_delay_sec = 5

    # Verificar cancelación antes de empezar
    if cancel_event and cancel_event.is_set():
        log("[CANCELADO] Descarga cancelada por el usuario.")
        return None

    # === PASO 2: Web scraping y descarga (con reintentos) ===
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        # Verificar cancelación antes de cada intento
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Descarga cancelada por el usuario.")
            return None
            
        if DOWNLOAD_RETRIES > 1:
            log(f"=== NIVEL 1: Descarga Web (SNIES) — Intento {attempt}/{DOWNLOAD_RETRIES} ===")
        else:
            log("=== NIVEL 1: Descarga Web (SNIES) ===")
        driver = None
        try:
            # Configurar Chrome y crear driver con manejo de errores específicos
            options = _configure_chrome(staging_dir_str, HEADLESS)
            try:
                log("Inicializando ChromeDriver...")
                driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
                driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT_SEC)
                wait = WebDriverWait(driver, max(120, SELENIUM_PAGE_LOAD_TIMEOUT_SEC))
            except WebDriverException as e:
                error_msg = (
                    f"Error al inicializar ChromeDriver: {e}\n\n"
                    "Verifica que:\n"
                    "1. Google Chrome esté instalado\n"
                    "2. Tengas conexión a internet (para descargar ChromeDriver)\n"
                    "3. Tengas permisos suficientes en la carpeta de descarga"
                )
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                raise RuntimeError(error_msg) from e
            except Exception as e:
                error_msg = f"Error inesperado al inicializar Chrome: {e}"
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                raise RuntimeError(error_msg) from e

            log("Abriendo la página...")
            try:
                driver.get(SNIES_URL)
                # Verificar cancelación después de cargar la página
                if cancel_event and cancel_event.is_set():
                    log("[CANCELADO] Descarga cancelada por el usuario.")
                    raise RuntimeError("Cancelado por el usuario")
            except TimeoutException:
                error_msg = (
                    f"Timeout al cargar la página {SNIES_URL} (más de {SELENIUM_PAGE_LOAD_TIMEOUT_SEC}s).\n\n"
                    "Posibles causas:\n"
                    "1. Conexión a internet lenta o intermitente\n"
                    "2. El sitio SNIES está temporalmente no disponible\n"
                    "3. Problemas de red o firewall\n\n"
                    "Intenta:\n"
                    "1. Verificar tu conexión a internet\n"
                    "2. Abrir la página manualmente en Chrome para verificar que funciona\n"
                    "3. Ejecutar el pipeline nuevamente"
                )
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                raise TimeoutException(error_msg) from None

            log("Esperando a que cargue la tabla o el contenedor principal...")
            # Esperar con verificación periódica de cancelación
            try:
                wait.until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table.table")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[role='table']")),
                        EC.presence_of_element_located((By.XPATH, "//table"))
                    )
                )
                # Verificar cancelación después de esperar
                if cancel_event and cancel_event.is_set():
                    log("[CANCELADO] Descarga cancelada por el usuario.")
                    raise RuntimeError("Cancelado por el usuario")
            except TimeoutException:
                # Si hay timeout, verificar si fue por cancelación
                if cancel_event and cancel_event.is_set():
                    log("[CANCELADO] Descarga cancelada por el usuario.")
                    raise RuntimeError("Cancelado por el usuario")
                raise

            before = set(os.listdir(staging_dir_str))

            log("Buscando botón de descarga...")
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
                try:
                    download_btn = wait.until(
                        EC.element_to_be_clickable(
                            (By.XPATH, "//button[contains(@class,'btn') and contains(@class,'success')]")
                        )
                    )
                except Exception as e:
                    error_msg = "No se encontró el botón de descarga."
                    log(f"[ERROR] {error_msg}")
                    log_error(error_msg)
                    raise

            log("Haciendo click en descargar...")
            driver.execute_script("arguments[0].click();", download_btn)

            log("Esperando la descarga...")
            # Esperar descarga con verificación periódica de cancelación
            downloaded_file = _wait_for_download(staging_dir_str, before, MAX_WAIT_DOWNLOAD_SEC, cancel_event=cancel_event)
            
            downloaded_name = os.path.basename(downloaded_file)
            log(f"Archivo descargado detectado: {downloaded_name}")
            log_info(f"Archivo descargado detectado: {downloaded_name}")

            ext = Path(downloaded_file).suffix
            expected_path = os.path.join(staging_dir_str, f"{RENAME_TO}{ext}")
            final_path = expected_path

            if downloaded_file == expected_path:
                nombre_final = os.path.basename(expected_path)
                log(f"  -> El archivo ya tiene el nombre correcto: {nombre_final}")
            else:
                log(f"  -> Renombrando {downloaded_name} a {RENAME_TO}{ext}...")
                if os.path.exists(expected_path):
                    try:
                        os.remove(expected_path)
                        log(f"  -> Eliminado archivo existente antes de renombrar: {os.path.basename(expected_path)}")
                    except OSError as e:
                        log(f"[WARN] No se pudo eliminar el archivo existente {expected_path}: {e}")
                try:
                    os.replace(downloaded_file, expected_path)
                    log(f"  -> Archivo renombrado exitosamente a {os.path.basename(expected_path)}")
                except OSError as e:
                    log(f"[WARN] Al renombrar el archivo descargado: {e}")
                    try:
                        if os.path.exists(expected_path):
                            os.remove(expected_path)
                        with open(downloaded_file, "rb") as src, open(expected_path, "wb") as dst:
                            dst.write(src.read())
                        os.remove(downloaded_file)
                        log(f"  -> Archivo copiado exitosamente a {os.path.basename(expected_path)}")
                    except Exception as copy_err:
                        log(f"[ERROR] No se pudo copiar el archivo descargado sobre {expected_path}: {copy_err}")
                        final_path = downloaded_file

            if final_path:
                nombre_final = os.path.basename(final_path)
                log(f"Archivo descargado: {nombre_final}")
                log("¡Descarga exitosa! Archivo actualizado:")
                log(f"  - {nombre_final}")
                log_info(f"Descarga finalizada: {nombre_final}")
                
                # Verificar cancelación antes de commit
                if cancel_event and cancel_event.is_set():
                    log("[CANCELADO] Cancelación detectada antes de commit. Limpiando...")
                    # Limpiar archivo descargado en staging
                    try:
                        if Path(final_path).exists():
                            os.remove(final_path)
                    except Exception:
                        pass
                    raise RuntimeError("Cancelado por el usuario")
                
                committed = _commit_programas(Path(final_path), "WEB_SNIES")
                
                # Si commit falla, limpiar staging
                if not committed:
                    log_error("Fallo al hacer commit del archivo descargado. Limpiando staging...")
                    try:
                        if Path(final_path).exists():
                            os.remove(final_path)
                    except Exception:
                        pass
                
                try:
                    driver.quit()
                    log_info("Driver Chrome cerrado correctamente.")
                except Exception:
                    pass
                return committed
            else:
                error_msg = "La descarga finalizó, pero no se pudo determinar el archivo final."
                log(error_msg)
                log_error(error_msg)

        except RuntimeError as e:
            # Cancelación por usuario
            if "Cancelado" in str(e):
                selenium_error = None  # No es un error real
                log("[CANCELADO] Limpiando recursos...")
                # Cerrar driver primero
                if driver:
                    try:
                        driver.quit()
                        log_info("Driver Chrome cerrado por cancelación.")
                    except Exception:
                        pass
                # Limpiar archivos temporales de staging (incluyendo archivos descargados)
                try:
                    # Limpiar .crdownload
                    for f in glob.glob(os.path.join(staging_dir_str, "*.crdownload")):
                        try:
                            os.remove(f)
                        except OSError:
                            pass
                    # Limpiar archivos descargados que puedan haber quedado
                    for pattern in ["Programas*.xlsx", "Programas*.csv"]:
                        for f in glob.glob(os.path.join(staging_dir_str, pattern)):
                            try:
                                # No eliminar si es el archivo que ya se movió a outputs
                                if "staging" in f.lower() or "_staging" in f:
                                    os.remove(f)
                            except OSError:
                                pass
                except Exception as e_clean:
                    log_error(f"Error al limpiar archivos temporales: {e_clean}")
                return None
            raise
        except TimeoutException as e:
            selenium_error = str(e)
            log(f"[ERROR] {selenium_error}")
            log_error(selenium_error)
            if driver:
                try:
                    driver.save_screenshot(os.path.join(staging_dir_str, "error_screenshot.png"))
                    log(f"Screenshot guardado en: {staging_dir_str}")
                    log_info("Screenshot de error guardado.")
                except Exception:
                    pass
        except WebDriverException as e:
            selenium_error = (
                f"Error de WebDriver: {e}\n\n"
                "Verifica que:\n"
                "1. Google Chrome esté instalado y actualizado\n"
                "2. No haya otros procesos de Chrome ejecutándose\n"
                "3. Tengas permisos suficientes"
            )
            log(f"[ERROR] {selenium_error}")
            log_error(selenium_error)
            if driver:
                try:
                    driver.save_screenshot(os.path.join(staging_dir_str, "error_screenshot.png"))
                    log(f"Screenshot guardado en: {staging_dir_str}")
                    log_info("Screenshot de error guardado.")
                except Exception:
                    pass
        except Exception as e:
            error_msg = str(e)
            selenium_error = error_msg
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            if driver:
                try:
                    driver.save_screenshot(os.path.join(staging_dir_str, "error_screenshot.png"))
                    log(f"Screenshot guardado en: {staging_dir_str}")
                    log_info("Screenshot de error guardado.")
                except Exception:
                    pass
        finally:
            if driver:
                try:
                    driver.quit()
                    log_info("Driver Chrome cerrado.")
                except Exception:
                    pass

        if attempt < DOWNLOAD_RETRIES:
            log(f"Reintento {attempt + 1}/{DOWNLOAD_RETRIES} en {retry_delay_sec}s...")
            time.sleep(retry_delay_sec)

    if selenium_error:
        # Si SNIES falla, abortamos sin modificar outputs/Programas.xlsx.
        log_error(
            "Fallo en descarga WEB_SNIES. No se realizaron modificaciones sobre los archivos existentes. "
            f"Detalle: {selenium_error}"
        )
    return None


if __name__ == "__main__":
    main()
