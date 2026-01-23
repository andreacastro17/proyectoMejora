"""
Aplicación GUI para ejecutar el pipeline de análisis SNIES.

Esta aplicación proporciona una interfaz gráfica simple para usuarios no técnicos.
En la primera ejecución, solicita al usuario seleccionar la carpeta raíz del proyecto.
"""

from __future__ import annotations

import json
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

# Añadir el directorio raíz al path si es necesario
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.config import get_base_dir, set_base_dir, update_paths_for_base_dir
from etl.descargaSNIES import HISTORIC_DIR, main as descargar_programas
from etl.historicoProgramasNuevos import actualizar_historico_programas_nuevos
from etl.normalizacion import ARCHIVO_PROGRAMAS, normalizar_programas
from etl.normalizacion_final import aplicar_normalizacion_final
from etl.pipeline_logger import (
    log_error,
    log_etapa_completada,
    log_etapa_iniciada,
    log_fin,
    log_inicio,
    log_warning,
)
from etl.procesamientoSNIES import procesar_programas_nuevos
from etl.clasificacionProgramas import clasificar_programas_nuevos


class PipelineGUI:
    """Interfaz gráfica para el pipeline de análisis SNIES."""
    
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Pipeline SNIES - Análisis de Mercados")
        self.root.geometry("600x400")
        self.root.resizable(False, False)
        
        # Estado del pipeline
        self.is_running = False
        self.base_dir = None
        
        # Configurar el estilo
        self._setup_ui()
        
        # Verificar configuración inicial
        self._check_initial_config()
    
    def _setup_ui(self):
        """Configura la interfaz de usuario."""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(
            main_frame,
            text="Pipeline de Análisis SNIES",
            font=("Arial", 16, "bold")
        )
        title_label.pack(pady=(0, 20))
        
        # Información del directorio base
        self.dir_frame = ttk.LabelFrame(main_frame, text="Carpeta del Proyecto", padding="10")
        self.dir_frame.pack(fill=tk.X, pady=(0, 20))
        
        self.dir_label = ttk.Label(
            self.dir_frame,
            text="No configurado",
            foreground="gray"
        )
        self.dir_label.pack(anchor=tk.W)
        
        btn_change_dir = ttk.Button(
            self.dir_frame,
            text="Cambiar Carpeta",
            command=self._select_base_directory
        )
        btn_change_dir.pack(anchor=tk.W, pady=(5, 0))
        
        # Botón de ejecución
        self.btn_execute = ttk.Button(
            main_frame,
            text="Ejecutar Pipeline",
            command=self._on_execute_clicked,
            state=tk.DISABLED
        )
        self.btn_execute.pack(pady=10)
        
        # Frame de estado
        status_frame = ttk.LabelFrame(main_frame, text="Estado", padding="10")
        status_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.status_label = ttk.Label(
            status_frame,
            text="Listo",
            font=("Arial", 11),
            foreground="green"
        )
        self.status_label.pack(anchor=tk.W, pady=5)
        
        # Área de mensajes
        messages_frame = ttk.LabelFrame(main_frame, text="Mensajes", padding="10")
        messages_frame.pack(fill=tk.BOTH, expand=True)
        
        # Scrollbar para el área de texto
        scrollbar = ttk.Scrollbar(messages_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.messages_text = tk.Text(
            messages_frame,
            height=8,
            wrap=tk.WORD,
            yscrollcommand=scrollbar.set,
            state=tk.DISABLED,
            font=("Consolas", 9)
        )
        self.messages_text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.messages_text.yview)
    
    def _check_initial_config(self):
        """Verifica si hay una configuración inicial y solicita la carpeta si es necesario."""
        try:
            base_dir = get_base_dir()
        except Exception:
            base_dir = None
        
        # Determinar la ruta del config.json
        if getattr(sys, 'frozen', False):
            # Ejecutándose como .EXE
            config_file = Path(sys.executable).parent / "config.json"
            default_base = Path(sys.executable).parent
        else:
            # Ejecutándose como script
            config_file = Path(__file__).resolve().parents[1] / "config.json"
            default_base = Path(__file__).resolve().parents[1]
        
        # Si no hay base_dir configurado o es el mismo que el default, pedirlo
        config_exists = config_file.exists()
        if config_exists:
            try:
                import json
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    base_dir_str = config.get("base_dir", "").strip()
                    if base_dir_str:
                        base_dir_path = Path(base_dir_str)
                        if base_dir_path.exists() and base_dir_path.is_dir():
                            base_dir = base_dir_path
            except Exception:
                pass
        
        if not base_dir or not base_dir.exists() or base_dir == default_base:
            self._log_message("⚠️ Primera ejecución: Seleccione la carpeta raíz del proyecto")
            self._log_message("Esta carpeta debe contener las carpetas: outputs/, ref/, models/, docs/")
            messagebox.showinfo(
                "Configuración Inicial",
                "Esta es la primera vez que ejecuta la aplicación.\n\n"
                "Por favor, seleccione la carpeta raíz del proyecto.\n"
                "Esta carpeta debe contener:\n"
                "- outputs/ (se creará automáticamente)\n"
                "- ref/\n"
                "- models/\n"
                "- docs/\n\n"
                "Esta configuración se guardará y no se volverá a pedir."
            )
            self._select_base_directory()
        else:
            self.base_dir = base_dir
            self._update_dir_label()
            self.btn_execute.config(state=tk.NORMAL)
            self._log_message(f"✓ Carpeta configurada: {base_dir}")
            self._log_message("Listo para ejecutar el pipeline")
    
    def _select_base_directory(self):
        """Abre un diálogo para seleccionar la carpeta raíz del proyecto."""
        selected_dir = filedialog.askdirectory(
            title="Seleccionar carpeta raíz del proyecto",
            initialdir=str(Path.home())
        )
        
        if selected_dir:
            base_dir = Path(selected_dir)
            
            # Verificar que sea una carpeta válida
            if not self._validate_base_directory(base_dir):
                messagebox.showerror(
                    "Error de Validación",
                    "La carpeta seleccionada no contiene las estructuras necesarias.\n\n"
                    "Asegúrese de que la carpeta contenga (o pueda contener):\n"
                    "- ref/\n"
                    "- models/\n"
                    "- docs/\n"
                )
                return
            
            # Guardar la configuración
            if set_base_dir(base_dir):
                self.base_dir = base_dir
                self._update_dir_label()
                self.btn_execute.config(state=tk.NORMAL)
                self._log_message(f"✓ Carpeta configurada: {base_dir}")
                self._log_message("Listo para ejecutar el pipeline")
            else:
                messagebox.showerror("Error", "No se pudo guardar la configuración.")
    
    def _validate_base_directory(self, base_dir: Path) -> bool:
        """
        Valida que el directorio base tenga la estructura esperada.
        
        Args:
            base_dir: Directorio a validar
            
        Returns:
            True si es válido, False en caso contrario
        """
        # Las carpetas ref/, models/, docs/ deben existir o poder crearse
        required_dirs = ["ref", "models", "docs"]
        for dir_name in required_dirs:
            dir_path = base_dir / dir_name
            # Si no existe, intentar crearlo (solo para verificar permisos)
            if not dir_path.exists():
                try:
                    dir_path.mkdir(exist_ok=True)
                except Exception:
                    return False
        return True
    
    def _update_dir_label(self):
        """Actualiza la etiqueta que muestra el directorio base."""
        if self.base_dir:
            # Mostrar ruta truncada si es muy larga
            dir_str = str(self.base_dir)
            if len(dir_str) > 60:
                dir_str = "..." + dir_str[-57:]
            self.dir_label.config(text=dir_str, foreground="black")
        else:
            self.dir_label.config(text="No configurado", foreground="gray")
    
    def _log_message(self, message: str):
        """Agrega un mensaje al área de texto."""
        self.messages_text.config(state=tk.NORMAL)
        timestamp = time.strftime("%H:%M:%S")
        self.messages_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.messages_text.see(tk.END)
        self.messages_text.config(state=tk.DISABLED)
        self.root.update_idletasks()
    
    def _update_status(self, status: str, color: str = "black"):
        """Actualiza el estado mostrado en la interfaz."""
        self.status_label.config(text=status, foreground=color)
        self.root.update_idletasks()
    
    def _on_execute_clicked(self):
        """Maneja el evento de clic en el botón de ejecutar."""
        if self.is_running:
            messagebox.showwarning("Atención", "El pipeline ya se está ejecutando.")
            return
        
        if not self.base_dir:
            messagebox.showerror("Error", "Debe configurar la carpeta del proyecto primero.")
            return
        
        # Confirmar ejecución
        result = messagebox.askyesno(
            "Confirmar Ejecución",
            "¿Desea ejecutar el pipeline ahora?\n\n"
            "Este proceso puede tardar varios minutos."
        )
        
        if result:
            self._execute_pipeline()
    
    def _execute_pipeline(self):
        """Ejecuta el pipeline en un hilo separado."""
        self.is_running = True
        self.btn_execute.config(state=tk.DISABLED)
        self._update_status("Procesando...", "orange")
        self._log_message("=" * 50)
        self._log_message("Iniciando ejecución del pipeline...")
        
        # Ejecutar en un hilo separado para no bloquear la GUI
        thread = threading.Thread(target=self._run_pipeline_thread, daemon=True)
        thread.start()
    
    def _run_pipeline_thread(self):
        """Ejecuta el pipeline en un hilo separado."""
        try:
            # Actualizar rutas para usar el base_dir configurado
            update_paths_for_base_dir(self.base_dir)
            
            # Ejecutar el pipeline
            resultado = run_pipeline(self.base_dir, log_callback=self._log_message)
            
            # Actualizar UI en el hilo principal
            self.root.after(0, self._on_pipeline_completed, resultado == 0)
            
        except Exception as e:
            error_msg = f"Error inesperado: {str(e)}"
            self.root.after(0, self._on_pipeline_error, error_msg)
    
    def _on_pipeline_completed(self, success: bool):
        """Maneja la finalización del pipeline."""
        self.is_running = False
        self.btn_execute.config(state=tk.NORMAL)
        
        if success:
            self._update_status("Completado", "green")
            self._log_message("=" * 50)
            self._log_message("✓ Pipeline completado exitosamente")
            messagebox.showinfo(
                "Éxito",
                "El pipeline se ejecutó correctamente.\n\n"
                f"Los archivos se guardaron en:\n{self.base_dir / 'outputs'}"
            )
        else:
            self._update_status("Error", "red")
            self._log_message("=" * 50)
            self._log_message("✗ El pipeline finalizó con errores")
            messagebox.showerror(
                "Error",
                "El pipeline finalizó con errores.\n\n"
                "Revise los mensajes para más detalles."
            )
    
    def _on_pipeline_error(self, error_msg: str):
        """Maneja errores durante la ejecución del pipeline."""
        self.is_running = False
        self.btn_execute.config(state=tk.NORMAL)
        self._update_status("Error", "red")
        self._log_message(f"✗ ERROR: {error_msg}")
        messagebox.showerror("Error", f"Error durante la ejecución:\n\n{error_msg}")


def run_pipeline(base_dir: Path, log_callback=None) -> int:
    """
    Ejecuta el pipeline completo de análisis SNIES.
    
    Args:
        base_dir: Directorio raíz del proyecto
        log_callback: Función opcional para enviar mensajes de log (recibe str)
        
    Returns:
        0 si el pipeline se completó exitosamente, 1 en caso de error
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)
    
    tiempo_inicio = time.time()
    log_inicio()
    log("=== Paso 1: Resguardo de históricos ===")
    log(f"Los archivos existentes se trasladarán a: {HISTORIC_DIR}")
    
    log("=== Paso 2: Descarga de Programas SNIES ===")
    log_etapa_iniciada("Descarga SNIES")
    log("Descargando archivo desde SNIES...")
    ruta_descargada = descargar_programas()
    if not ruta_descargada:
        error_msg = "No se obtuvo una ruta de descarga válida."
        log(f"[ERROR] {error_msg}")
        log_error(error_msg)
        return 1
    
    ruta_descargada = Path(ruta_descargada)
    if not ruta_descargada.exists():
        error_msg = f"El archivo descargado no existe: {ruta_descargada}"
        log(f"[ERROR] {error_msg}")
        log_error(error_msg)
        return 1
    
    nombre_archivo = ruta_descargada.name
    log(f"✓ Archivo descargado: {nombre_archivo}")
    log_etapa_completada("Descarga SNIES", nombre_archivo)
    
    if ruta_descargada != ARCHIVO_PROGRAMAS:
        warning_msg = (
            f"El archivo descargado está en {ruta_descargada}, "
            f"pero la normalización usará {ARCHIVO_PROGRAMAS}."
        )
        log(f"[WARN] {warning_msg}")
        log_warning(warning_msg)
    
    log("=== Paso 3: Normalización de columnas ===")
    log_etapa_iniciada("Normalización")
    try:
        log("Normalizando columnas del archivo...")
        normalizar_programas()
        log("✓ Normalización completada")
        log_etapa_completada("Normalización")
    except Exception as exc:
        error_msg = f"Falló la normalización: {exc}"
        log(f"[ERROR] {error_msg}")
        log_error(error_msg)
        return 1
    
    log("=== Paso 4: Procesamiento de programas nuevos ===")
    log_etapa_iniciada("Procesamiento de programas nuevos")
    try:
        log("Procesando programas nuevos...")
        procesar_programas_nuevos()
        log("✓ Procesamiento completado")
        log_etapa_completada("Procesamiento de programas nuevos")
    except Exception as exc:
        error_msg = f"Falló el procesamiento de programas nuevos: {exc}"
        log(f"[ERROR] {error_msg}")
        log_error(error_msg)
        return 1
    
    log("=== Paso 5: Clasificación de programas nuevos ===")
    log_etapa_iniciada("Clasificación de programas nuevos")
    try:
        log("Clasificando programas nuevos...")
        clasificar_programas_nuevos()
        log("✓ Clasificación completada")
        log_etapa_completada("Clasificación de programas nuevos")
    except Exception as exc:
        error_msg = f"Falló la clasificación de programas nuevos: {exc}"
        log(f"[WARN] {error_msg}")
        log("Continuando sin clasificación...")
        log_error(error_msg)
        log_warning("Continuando sin clasificación...")
        # No retornamos error aquí porque la clasificación es opcional
    
    log("=== Paso 6: Normalización final de ortografía y formato ===")
    log_etapa_iniciada("Normalización final")
    try:
        log("Aplicando normalización final...")
        aplicar_normalizacion_final()
        log("✓ Normalización final completada")
        log_etapa_completada("Normalización final")
    except Exception as exc:
        error_msg = f"Falló la normalización final: {exc}"
        log(f"[ERROR] {error_msg}")
        log_error(error_msg)
        return 1
    
    log("=== Paso 7: Actualización de histórico de programas nuevos ===")
    log_etapa_iniciada("Actualización de histórico de programas nuevos")
    try:
        log("Actualizando histórico...")
        actualizar_historico_programas_nuevos()
        log("✓ Histórico actualizado")
        log_etapa_completada("Actualización de histórico de programas nuevos")
    except Exception as exc:
        error_msg = f"Falló la actualización del histórico: {exc}"
        log(f"[WARN] {error_msg}")
        log_error(error_msg)
        # No retornamos error aquí porque el histórico es complementario
    
    tiempo_fin = time.time()
    duracion_minutos = (tiempo_fin - tiempo_inicio) / 60.0
    log("Pipeline completado exitosamente.")
    log_fin(duracion_minutos)
    
    return 0


def main():
    """Función principal que inicia la aplicación GUI."""
    root = tk.Tk()
    app = PipelineGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
