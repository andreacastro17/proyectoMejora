"""
Aplicación GUI para ejecutar el pipeline de análisis SNIES.

Esta aplicación proporciona una interfaz gráfica simple para usuarios no técnicos.
En la primera ejecución, solicita al usuario seleccionar la carpeta raíz del proyecto.
"""

from __future__ import annotations

import json
import datetime
import os
import shutil
import sys
import threading
import time
import tkinter as tk
import pandas as pd
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

# Dependencia ya usada por el resto del proyecto (ETL)
# pandas se importa lazy donde se necesita (no al inicio para acelerar arranque)
from typing import Callable

# Añadir el directorio raíz al path si es necesario
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Imports ligeros al inicio (solo config básico)
from etl.config import (
    get_base_dir,
    set_base_dir,
    update_paths_for_base_dir,
    HISTORIC_DIR,
    ARCHIVO_NORMALIZACION,
    MODELS_DIR,
)

# Imports pesados se hacen lazy (solo cuando se ejecuta el pipeline)
# Esto acelera el arranque de la aplicación


def _open_in_excel(path: Path) -> None:
    """Abre un archivo con la app por defecto (Excel en Windows)."""
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    # Windows: abre con la aplicación predeterminada
    os.startfile(str(path))  # type: ignore[attr-defined]


def _open_default_app(path: Path) -> None:
    """Abre un archivo con la app predeterminada del sistema."""
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    os.startfile(str(path))  # type: ignore[attr-defined]


def _open_text_file(path: Path) -> None:
    """Abre un archivo de texto (fallback a app por defecto si no hay notepad)."""
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    try:
        os.startfile(str(path))  # type: ignore[attr-defined]
    except Exception:
        # Último fallback
        _open_default_app(path)


def _ask_yes_no(title: str, msg: str, parent: tk.Misc | None = None) -> bool:
    try:
        return bool(messagebox.askyesno(title, msg, parent=parent))
    except Exception:
        return False


# Usar etl.config como única fuente de verdad para rutas exe/script
def _get_config_file_for_gui() -> Path:
    """Ruta de config.json (delega en etl.config)."""
    from etl.config import get_config_file_path
    return get_config_file_path()


def get_pipeline_lock_file() -> Path:
    from etl.normalizacion import ARCHIVO_PROGRAMAS  # Lazy import
    return ARCHIVO_PROGRAMAS.parent / ".pipeline.lock"


def get_lock_age_seconds(lock_file: Path) -> float | None:
    if not lock_file.exists():
        return None
    try:
        return time.time() - lock_file.stat().st_mtime
    except Exception:
        return None


LOCK_STALE_SECONDS = 60 * 30  # 30 minutos


def explain_file_in_use() -> str:
    return (
        "No se pudo escribir el archivo porque está abierto o bloqueado.\n\n"
        "Cierra Excel / Power BI (y cualquier visor del archivo) y vuelve a intentarlo."
    )


def safe_messagebox_error(title: str, msg: str, parent: tk.Misc | None = None) -> None:
    try:
        messagebox.showerror(title, msg, parent=parent)
    except Exception:
        # fallback
        print(f"[ERROR] {title}: {msg}")


def can_write_file(path: Path) -> bool:
    """
    Retorna True si el archivo puede abrirse en modo escritura (append).
    Útil para detectar Excel/PowerBI bloqueando el archivo en Windows.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8"):
            return True
    except PermissionError:
        return False
    except Exception:
        # Si no existe o hay otro error, no bloqueamos por defecto.
        return True


def validar_entorno_pipeline() -> tuple[bool, list[str]]:
    """
    Comprueba que el entorno esté listo para ejecutar el pipeline (archivos, permisos).
    Returns:
        (ok, lista de mensajes: faltantes o errores si not ok; vacía o ["Todo listo"] si ok)
    """
    from etl.config import (
        ARCHIVO_NORMALIZACION,
        OUTPUTS_DIR,
        get_archivo_referentes,
        get_archivo_catalogo_eafit,
    )
    mensajes: list[str] = []
    if not ARCHIVO_NORMALIZACION.exists():
        mensajes.append(f"Falta archivo de normalización: {ARCHIVO_NORMALIZACION}")
    try:
        if not get_archivo_referentes().exists():
            mensajes.append("Falta archivo de referentes (ref/referentesUnificados.xlsx o .csv)")
    except FileNotFoundError:
        mensajes.append("Falta archivo de referentes (ref/referentesUnificados.xlsx o .csv)")
    try:
        if not get_archivo_catalogo_eafit().exists():
            mensajes.append("Falta archivo de catálogo EAFIT (ref/catalogoOfertasEAFIT.xlsx o .csv)")
    except FileNotFoundError:
        mensajes.append("Falta archivo de catálogo EAFIT (ref/catalogoOfertasEAFIT.xlsx o .csv)")
    test_file = OUTPUTS_DIR / ".write_test"
    try:
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        test_file.write_text("", encoding="utf-8")
        test_file.unlink(missing_ok=True)
    except Exception as e:
        mensajes.append(f"No se puede escribir en la carpeta outputs: {e}")
    ok = len(mensajes) == 0
    if ok:
        mensajes.append("Todo listo para ejecutar el pipeline.")
    return ok, mensajes


def validate_programas_schema(path_xlsx: Path) -> tuple[bool, str]:
    """
    Valida el "schema mínimo" requerido para que el pipeline funcione.
    """
    import pandas as pd  # Lazy import
    
    required_cols = [
        "CÓDIGO_SNIES_DEL_PROGRAMA",
        "NOMBRE_DEL_PROGRAMA",
        "NOMBRE_INSTITUCIÓN",
        "NIVEL_DE_FORMACIÓN",
    ]
    try:
        df_head = pd.read_excel(path_xlsx, sheet_name="Programas", nrows=5)
    except Exception as exc:
        return False, f"No se pudo leer la hoja 'Programas' en {path_xlsx.name}: {exc}"
    missing = [c for c in required_cols if c not in df_head.columns]
    if missing:
        return (
            False,
            "El archivo descargado no tiene las columnas mínimas esperadas.\n"
            f"Faltan: {', '.join(missing)}\n\n"
            "Esto puede indicar que SNIES cambió el formato o que la descarga no es válida.",
        )
    return True, "OK"


def get_configured_base_dir() -> Path | None:
    """
    Lee el base_dir configurado (si existe) sin mostrar diálogos.
    Retorna None si no hay configuración válida.
    """
    try:
        base_dir = get_base_dir()
    except Exception:
        base_dir = None

    config_file = _get_config_file_for_gui()
    if config_file.exists():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            base_dir_str = str(config.get("base_dir", "")).strip()
            if base_dir_str:
                p = Path(base_dir_str)
                if p.exists() and p.is_dir():
                    base_dir = p
        except Exception:
            pass

    if base_dir and base_dir.exists() and base_dir.is_dir():
        return base_dir
    return None


def ensure_base_dir(parent_window: tk.Misc | None = None, prompt_if_missing: bool = True) -> Path | None:
    """
    Asegura que exista un base_dir configurado. Si no, solicita una carpeta al usuario.
    Retorna el base_dir o None si el usuario cancela.
    """
    base_dir = get_configured_base_dir()

    # Si no hay base_dir válido, pedirlo (solo si está permitido)
    if not base_dir:
        if not prompt_if_missing:
            return None
        if parent_window is not None:
            messagebox.showinfo(
                "Configuración Inicial",
                "Seleccione la carpeta raíz del proyecto.\n\n"
                "Debe contener (o poder contener):\n"
                "- outputs/\n- ref/\n- models/\n- docs/\n",
                parent=parent_window,
            )
        selected_dir = filedialog.askdirectory(
            title="Seleccionar carpeta raíz del proyecto",
            initialdir=str(Path.home()),
            parent=parent_window,
        )
        if not selected_dir:
            return None
        base_dir = Path(selected_dir)
        if not base_dir.exists() or not base_dir.is_dir():
            messagebox.showerror("Error", "La carpeta seleccionada no es válida.", parent=parent_window)
            return None
        if not set_base_dir(base_dir):
            messagebox.showerror("Error", "No se pudo guardar la configuración.", parent=parent_window)
            return None

    try:
        update_paths_for_base_dir(base_dir)
    except Exception as exc:
        messagebox.showerror(
            "Error",
            f"No se pudo configurar el directorio base:\n\n{exc}",
            parent=parent_window,
        )
        return None

    return base_dir


# ========== COLORIMETRÍA EAFIT (paleta base oficial mejorada) ==========
# Blanco puro #FFFFFF | Negro puro #000000
# Azul Zafre oscuro #000066 (Pantone 2738C) | Azul Azure claro #00A9E0 (Pantone 306C)
# Combinaciones sugeridas: texto azul zafre sobre blanco, blanco sobre azul zafre, azul azure como acento.
EAFIT = {
    "white": "#FFFFFF",
    "black": "#000000",
    "azul_zafre": "#000066",   # Azul Zafre oscuro - principal, botones primarios, títulos
    "azul_azure": "#00A9E0",   # Azul Azure claro - acentos, enlaces, secundario
    "azul_azure_light": "#4DC8F0",  # Azul Azure más claro para hover
    "bg": "#F5F7FA",           # Fondo general mejorado (gris muy claro, más moderno)
    "card_bg": "#FFFFFF",
    "card_border": "#E1E8ED",  # Borde sutil para tarjetas
    "card_shadow": "#D1D9E0",  # Color para sombra simulada
    "text": "#1A1A1A",         # Negro suavizado para mejor legibilidad
    "text_on_dark": "#FFFFFF", # Texto sobre azul zafre
    "text_muted": "#6B7280",   # Gris mejorado para texto secundario
    "text_light": "#9CA3AF",   # Gris muy claro para hints
    "success": "#10B981",      # Verde mejorado
    "success_light": "#D1FAE5", # Verde claro para fondos
    "warning": "#F59E0B",      # Amarillo mejorado
    "warning_light": "#FEF3C7", # Amarillo claro para fondos
    "danger": "#EF4444",       # Rojo mejorado
    "danger_light": "#FEE2E2", # Rojo claro para fondos
    "info": "#3B82F6",         # Azul info
    "info_light": "#DBEAFE",   # Azul claro para fondos
    "separator": "#E5E7EB",    # Separadores sutiles
}
THEME = EAFIT  # alias para no romper referencias


def apply_modern_style(root: tk.Tk) -> None:
    """
    Aplica la colorimetría EAFIT mejorada y estilos ttk modernos en toda la aplicación.
    Paleta: Blanco #FFFFFF, Negro #000000, Azul Zafre #000066, Azul Azure #00A9E0.
    Incluye mejoras estéticas: mejor espaciado, bordes sutiles, tipografía mejorada.
    """
    style = ttk.Style(root)
    # "clam" permite personalizar colores de botones (vista en Windows no siempre)
    for theme in ("clam", "vista", "default"):
        try:
            if theme in style.theme_names():
                style.theme_use(theme)
                break
        except Exception:
            continue

    base_bg = EAFIT["bg"]
    root.configure(bg=base_bg)

    # Frames mejorados con mejor diseño
    style.configure("App.TFrame", background=base_bg)
    style.configure(
        "Card.TFrame",
        background=EAFIT["card_bg"],
        relief="flat",
        borderwidth=1,
    )
    # Intentar agregar borde sutil a las tarjetas (si el tema lo permite)
    try:
        style.map("Card.TFrame", 
                  bordercolor=[("", EAFIT["card_border"])],
                  lightcolor=[("", EAFIT["card_border"])],
                  darkcolor=[("", EAFIT["card_border"])])
    except tk.TclError:
        pass
    style.configure("Page.TFrame", background=base_bg)
    
    # Frame con borde superior para separadores elegantes
    # Nota: height puede causar problemas en algunos temas, lo removemos
    try:
        style.configure("Separator.TFrame", background=EAFIT["separator"])
    except tk.TclError:
        # Fallback si el estilo no funciona
        pass

    # Labels mejorados con mejor tipografía y espaciado
    style.configure(
        "Header.TLabel",
        background=base_bg,
        font=("Segoe UI", 24, "bold"),
        foreground=EAFIT["azul_zafre"],
    )
    style.configure(
        "SubHeader.TLabel",
        background=base_bg,
        foreground=EAFIT["text_muted"],
        font=("Segoe UI", 11),
    )
    style.configure(
        "SectionTitle.TLabel",
        background=EAFIT["card_bg"],
        font=("Segoe UI", 13, "bold"),
        foreground=EAFIT["azul_zafre"],
    )
    style.configure(
        "Muted.TLabel",
        background=EAFIT["card_bg"],
        foreground=EAFIT["text_muted"],
        font=("Segoe UI", 9),
    )
    style.configure(
        "Light.TLabel",
        background=EAFIT["card_bg"],
        foreground=EAFIT["text_light"],
        font=("Segoe UI", 8),
    )
    style.configure("Path.TLabel", background=base_bg, foreground=EAFIT["text"], font=("Segoe UI", 9))
    style.configure("Status.TLabel", background=base_bg, foreground=EAFIT["text_muted"], font=("Segoe UI", 9))
    style.configure("Help.TLabel", background=base_bg, foreground=EAFIT["text_muted"], font=("Segoe UI", 9))
    
    # Labels de estado con colores
    style.configure("Success.TLabel", background=base_bg, foreground=EAFIT["success"], font=("Segoe UI", 9, "bold"))
    style.configure("Warning.TLabel", background=base_bg, foreground=EAFIT["warning"], font=("Segoe UI", 9, "bold"))
    style.configure("Danger.TLabel", background=base_bg, foreground=EAFIT["danger"], font=("Segoe UI", 9, "bold"))
    style.configure("Info.TLabel", background=base_bg, foreground=EAFIT["info"], font=("Segoe UI", 9, "bold"))

    # Botones mejorados con mejor diseño y padding
    style.configure(
        "Primary.TButton",
        font=("Segoe UI", 11, "bold"),
        padding=(20, 12),
    )
    try:
        style.configure(
            "Primary.TButton",
            background=EAFIT["azul_zafre"],
            foreground=EAFIT["text_on_dark"],
        )
        style.map("Primary.TButton", 
                  background=[("active", EAFIT["azul_azure"]), 
                             ("pressed", EAFIT["azul_zafre"])])
    except tk.TclError:
        pass  # tema vista no permite background en TButton
    style.configure(
        "Secondary.TButton",
        font=("Segoe UI", 10),
        padding=(16, 10),
    )
    try:
        style.configure("Secondary.TButton", foreground=EAFIT["azul_zafre"])
        style.map("Secondary.TButton", 
                  foreground=[("active", EAFIT["azul_azure"])])
    except tk.TclError:
        pass
    # Botón de peligro (cancelar)
    style.configure("Danger.TButton", font=("Segoe UI", 10), padding=(16, 10))
    try:
        style.configure("Danger.TButton", foreground=EAFIT["white"], background=EAFIT["danger"])
    except tk.TclError:
        style.configure("Danger.TButton", foreground=EAFIT["danger"])
    try:
        style.map("Danger.TButton", foreground=[("active", EAFIT["danger_light"])])
    except tk.TclError:
        pass
    style.configure("Back.TButton", font=("Segoe UI", 10), padding=(12, 8))
    try:
        style.configure("Back.TButton", foreground=EAFIT["azul_azure"])
        style.map("Back.TButton", 
                  foreground=[("active", EAFIT["azul_zafre"])])
    except tk.TclError:
        pass
    
    # Botón pequeño para utilidades
    style.configure("Small.TButton", font=("Segoe UI", 9), padding=(10, 6))
    try:
        style.configure("Small.TButton", foreground=EAFIT["azul_zafre"])
    except tk.TclError:
        pass


class EditableTable(ttk.Frame):
    """
    Tabla editable simple basada en ttk.Treeview.
    Pensada para ediciones puntuales (no millones de filas).
    """

    def __init__(
        self,
        master: tk.Misc,
        columns: list[str],
        height: int = 15,
        editable_columns: set[str] | None = None,
        on_change: Callable[[int, str, str], None] | None = None,
    ):
        super().__init__(master)
        self.columns = columns
        self._data: list[dict] = []
        self._item_to_index: dict[str, int] = {}
        self.editable_columns = editable_columns if editable_columns is not None else set(columns)
        self.on_change = on_change

        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=height)
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        for c in columns:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=160, anchor="w", minwidth=100)

        # Configurar grid con scrollbars
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        # Configurar pesos para que se expandan correctamente
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=0)  # Fila del scrollbar horizontal
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)  # Columna del scrollbar vertical

        self._editor: tk.Entry | ttk.Combobox | None = None
        self.tree.bind("<Double-1>", self._begin_edit)
        self._min_height_rows = 5
        self._row_height_px = 24

    def set_height_from_pixels(self, pixels: int) -> None:
        """Ajusta la altura del Treeview en número de filas según el espacio en píxeles (responsive)."""
        if pixels < 50:
            return
        try:
            rows = max(self._min_height_rows, pixels // self._row_height_px)
            self.tree.configure(height=rows)
        except (tk.TclError, ValueError):
            pass

    def set_rows(self, rows: list[dict]) -> None:
        self._data = rows
        self._item_to_index.clear()
        for item in self.tree.get_children():
            self.tree.delete(item)
        for idx, row in enumerate(rows):
            values = [row.get(c, "") for c in self.columns]
            item_id = self.tree.insert("", "end", values=values)
            self._item_to_index[item_id] = idx

    def get_rows(self) -> list[dict]:
        return self._data

    def get_selected_index(self) -> int | None:
        sel = self.tree.selection()
        if not sel:
            return None
        return self._item_to_index.get(sel[0])

    def delete_selected(self) -> None:
        idx = self.get_selected_index()
        if idx is None:
            return
        # borrar del modelo
        self._data.pop(idx)
        # reconstruir todo para mantener índices consistentes
        self.set_rows(self._data)
    
    def set_cell_value(self, row_idx: int, column: str, value: str) -> None:
        """Establece el valor de una celda específica y actualiza la visualización."""
        if row_idx < 0 or row_idx >= len(self._data):
            return
        if column not in self.columns:
            return
        
        # Actualizar datos
        self._data[row_idx][column] = value
        
        # Actualizar visualización
        item_ids = list(self.tree.get_children())
        if row_idx < len(item_ids):
            item_id = item_ids[row_idx]
            col_index = self.columns.index(column)
            current_vals = list(self.tree.item(item_id, "values"))
            if col_index < len(current_vals):
                current_vals[col_index] = value
                self.tree.item(item_id, values=current_vals)

    def add_row(self, default_row: dict | None = None) -> None:
        row = {c: "" for c in self.columns}
        if default_row:
            row.update(default_row)
        self._data.append(row)
        self.set_rows(self._data)

    def _begin_edit(self, event):
        # Identificar celda
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)  # e.g. "#1"
        if not row_id or not col_id:
            return
        col_index = int(col_id.replace("#", "")) - 1
        if col_index < 0 or col_index >= len(self.columns):
            return
        column = self.columns[col_index]
        if column not in self.editable_columns:
            return
        bbox = self.tree.bbox(row_id, col_id)
        if not bbox:
            return

        idx = self._item_to_index.get(row_id)
        if idx is None:
            return

        # destruir editor anterior
        if self._editor is not None:
            try:
                self._editor.destroy()
            except Exception:
                pass
            self._editor = None

        x, y, w, h = bbox
        value = str(self._data[idx].get(column, "") or "")

        # Para ES_REFERENTE, usar Combobox en lugar de Entry para mejor UX
        if column == "ES_REFERENTE":
            editor = ttk.Combobox(self.tree, values=["Sí", "No"], state="readonly", width=w//8)
            editor.set(value if value in ["Sí", "No"] else "No")
            editor.focus_set()
            editor.place(x=x, y=y, width=w, height=h)
            
            def commit(_evt=None):
                new_val = editor.get()
                self._data[idx][column] = new_val
                # actualizar visualmente
                current_vals = list(self.tree.item(row_id, "values"))
                current_vals[col_index] = new_val
                self.tree.item(row_id, values=current_vals)
                if self.on_change is not None:
                    try:
                        self.on_change(idx, column, new_val)
                    except Exception:
                        pass
                editor.destroy()
                self._editor = None
            
            editor.bind("<<ComboboxSelected>>", commit)
            editor.bind("<FocusOut>", commit)
            editor.bind("<Return>", commit)
            editor.bind("<Escape>", lambda e: (editor.destroy(), setattr(self, '_editor', None)))
        else:
            # Para otras columnas, usar Entry normal
            editor = tk.Entry(self.tree)
            editor.insert(0, value)
            editor.select_range(0, tk.END)
            editor.focus_set()
            editor.place(x=x, y=y, width=w, height=h)

            def commit(_evt=None):
                new_val = editor.get()
                self._data[idx][column] = new_val
                # actualizar visualmente
                current_vals = list(self.tree.item(row_id, "values"))
                current_vals[col_index] = new_val
                self.tree.item(row_id, values=current_vals)
                if self.on_change is not None:
                    try:
                        self.on_change(idx, column, new_val)
                    except Exception:
                        pass
                editor.destroy()
                self._editor = None

            editor.bind("<Return>", commit)
            editor.bind("<FocusOut>", commit)
            editor.bind("<Escape>", lambda e: (editor.destroy(), setattr(self, '_editor', None)))
        
        self._editor = editor


class ManualReviewPage(ttk.Frame):
    """Edición manual de emparejamientos (falsos positivos) en Programas.xlsx."""

    def __init__(self, parent: tk.Misc, on_back=None):
        super().__init__(parent)
        self.on_back = on_back
        
        # Import lazy de pandas y ARCHIVO_PROGRAMAS (solo cuando se abre esta página)
        import pandas as pd
        from etl.normalizacion import ARCHIVO_PROGRAMAS
        
        self.base_dir = ensure_base_dir(self)
        if not self.base_dir:
            if on_back:
                on_back()
            return

        self.file_path = ARCHIVO_PROGRAMAS
        # Columnas principales (vista resumida)
        self.main_columns = [
            "CÓDIGO_SNIES_DEL_PROGRAMA",
            "NOMBRE_INSTITUCIÓN",
            "NOMBRE_DEL_PROGRAMA",
            "NIVEL_DE_FORMACIÓN",
            "PROGRAMA_NUEVO",
            "ES_REFERENTE",
            "PROBABILIDAD",
            "PROGRAMA_EAFIT_CODIGO",
            "PROGRAMA_EAFIT_NOMBRE",
        ]
        # Columnas visibles actuales (por defecto todas, se actualiza al cargar)
        self.display_columns = self.main_columns.copy()
        # Todas las columnas disponibles (se establece al cargar el archivo)
        self.all_columns: list[str] = []
        # Estado de la vista: True = completa (todas), False = principal (9 columnas)
        self.view_complete = True
        self.editable_columns = {
            "ES_REFERENTE",
            "PROGRAMA_EAFIT_CODIGO",
            "PROGRAMA_EAFIT_NOMBRE",
            "PROBABILIDAD",
        }
        # df_view: solo columnas necesarias para mostrar/filtrar (no se usa para guardar en disco)
        # Type hint usa string para evitar importar pandas al inicio
        self.df_view = None  # type: ignore
        self._filtered_df: pd.DataFrame | None = None
        self.page_size = 200
        self.page_index = 0
        # Cambios pendientes persistentes entre páginas (por código SNIES normalizado)
        self.pending_updates: dict[str, dict[str, object]] = {}
        # Backup oculto antes de guardar (para restaurar si es necesario)
        self.last_backup_path: Path | None = None

        header = ttk.Frame(self, padding=12, style="Page.TFrame")
        header.pack(fill=tk.X)
        header_left = ttk.Frame(header, style="Page.TFrame")
        header_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(header_left, text="✏️ Ajuste manual de emparejamientos", style="Header.TLabel").pack(anchor="w")
        self.subheader_label = ttk.Label(header_left, text="Edita ES_REFERENTE y programa EAFIT. Los cambios se guardan en Programas.xlsx.", style="SubHeader.TLabel")
        self.subheader_label.pack(anchor="w", pady=(4, 0), fill=tk.X)
        if on_back:
            ttk.Button(header, text="← Volver al menú", command=lambda: on_back() if on_back else None, style="Back.TButton").pack(side=tk.RIGHT)

        # Botones principales en grid para mejor organización responsive
        btns = ttk.Frame(self, padding="10")
        btns.pack(fill=tk.X)
        btns.grid_columnconfigure(0, weight=1, uniform="btn_col")
        btns.grid_columnconfigure(1, weight=1, uniform="btn_col")
        btns.grid_columnconfigure(2, weight=1, uniform="btn_col")
        
        # Fila 1: Acciones principales
        row1 = ttk.Frame(btns, style="App.TFrame")
        row1.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 6))
        ttk.Button(row1, text="Recargar Programas.xlsx", command=self._load).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(row1, text="Abrir en Excel", command=self._open_excel).pack(side=tk.LEFT, padx=6)
        self.btn_save = ttk.Button(row1, text="Guardar cambios (todo)", command=self._save)
        self.btn_save.pack(side=tk.LEFT, padx=6)
        self.btn_delete = ttk.Button(row1, text="Descartar cambios (fila)", command=self._discard_row_changes)
        self.btn_delete.pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="Descartar todo", command=self._discard_all_changes).pack(side=tk.LEFT, padx=6)
        self.btn_toggle_view = ttk.Button(row1, text="Vista principal", command=self._toggle_view, state=tk.DISABLED)
        self.btn_toggle_view.pack(side=tk.LEFT, padx=6)
        
        # Fila 2: Marcado y restauración
        row2 = ttk.Frame(btns, style="App.TFrame")
        row2.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(0, 6))
        ttk.Button(row2, text="Marcar SÍ referente", command=self._mark_si_referente).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(row2, text="Marcar NO referente", command=self._mark_no_referente).pack(side=tk.LEFT, padx=6)
        self.btn_restore = ttk.Button(row2, text="Restaurar estado anterior", command=self._restore_backup, state=tk.DISABLED)
        self.btn_restore.pack(side=tk.LEFT, padx=6)
        
        # Fila 3: Filtros y búsqueda
        row3 = ttk.Frame(btns, style="App.TFrame")
        row3.grid(row=2, column=0, columnspan=3, sticky="ew")
        ttk.Label(row3, text="Filtro:").pack(side=tk.LEFT, padx=(0, 6))
        self.filter_var = tk.StringVar(value="SOLO_NUEVOS")
        filter_combo = ttk.Combobox(
            row3,
            textvariable=self.filter_var,
            state="readonly",
            values=["SOLO_NUEVOS", "SOLO_REFERENTES", "TODOS"],
            width=18,
        )
        filter_combo.pack(side=tk.LEFT)
        filter_combo.bind("<<ComboboxSelected>>", lambda e: self._apply_filter())
        ttk.Button(row3, text="Aplicar filtro", command=self._apply_filter).pack(side=tk.LEFT, padx=6)
        ttk.Label(row3, text="Buscar:").pack(side=tk.LEFT, padx=(14, 6))
        self.search_var = tk.StringVar(value="")
        self.search_entry = ttk.Entry(row3, textvariable=self.search_var, width=22)
        self.search_entry.pack(side=tk.LEFT)
        ttk.Button(row3, text="Buscar", command=self._apply_filter).pack(side=tk.LEFT, padx=6)

        pager = ttk.Frame(self, padding=(10, 0, 10, 10))
        pager.pack(fill=tk.X)
        self.page_label = ttk.Label(pager, text="Página: -", foreground=EAFIT["text_muted"])
        self.page_label.pack(side=tk.LEFT)
        self.selection_label = ttk.Label(pager, text="Selección: -", foreground=EAFIT["text_muted"])
        self.selection_label.pack(side=tk.LEFT, padx=(12, 0))
        self.pending_label = ttk.Label(pager, text="Cambios pendientes: 0", foreground=EAFIT["text_muted"])
        self.pending_label.pack(side=tk.LEFT, padx=(12, 0))
        ttk.Button(pager, text="Anterior", command=self._prev_page).pack(side=tk.RIGHT)
        ttk.Button(pager, text="Siguiente", command=self._next_page).pack(side=tk.RIGHT, padx=6)

        # Banner de solo lectura
        self.readonly_banner = ttk.Label(
            self,
            text="",
            foreground=EAFIT["danger"],
            font=("Segoe UI", 9, "bold"),
        )
        self.readonly_banner.pack(fill=tk.X, padx=10, pady=(0, 6))

        self.msg = tk.Text(self, height=6, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9), bg=EAFIT["card_bg"], fg=EAFIT["text"])
        self.msg.pack(fill=tk.X, padx=10, pady=(0, 10))

        self.table = EditableTable(
            self,
            columns=self.display_columns,
            height=18,
            editable_columns=self.editable_columns,
            on_change=self._on_cell_change,
        )
        self.table.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        # Actualizar estado de selección
        self.table.tree.bind("<<TreeviewSelect>>", self._on_select)

        self._log("Tip: edita una celda con doble clic. Ajusta ES_REFERENTE y PROGRAMA_EAFIT_* si hay falsos positivos.")
        # Auto-cargar si existe el archivo (mejor UX).
        if self.file_path.exists():
            self._load()
        else:
            self._log("No se encontró outputs/Programas.xlsx. Ejecuta primero el análisis SNIES (Pipeline).")

        # Monitor de lock del pipeline
        self._poll_lock()
        # Atajos de teclado (bind al root)
        root = self.winfo_toplevel()
        root.bind("<Control-f>", lambda _e: self._focus_search())
        root.bind("<Control-s>", lambda _e: self._save())
        root.bind("<Delete>", lambda _e: self._discard_row_changes())
        self._log("Nota: solo puedes editar ES_REFERENTE / PROGRAMA_EAFIT_* / PROBABILIDAD. Los demás campos son de SNIES (solo lectura).")

    def _on_resize(self, w: int, h: int) -> None:
        """Responsive: ajusta la altura de la tabla y wraplengths al espacio disponible."""
        # Aproximado: header + botones + paginador + banner + msg ~ 320 px
        table_pixels = max(120, h - 320)
        self.table.set_height_from_pixels(table_pixels)
        
        # Ajustar wraplength del subheader dinámicamente
        if hasattr(self, 'subheader_label'):
            wraplength = max(400, w - 100)
            self.subheader_label.config(wraplength=wraplength)

    def _focus_search(self):
        try:
            self.search_entry.focus_set()
            self.search_entry.selection_range(0, tk.END)
        except Exception:
            pass

    def _norm_codigo(self, v: object) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    def _now_iso(self) -> str:
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _touch_pending(self):
        try:
            self.pending_label.config(text=f"Cambios pendientes: {len(self.pending_updates)}")
        except Exception:
            pass

    def _on_cell_change(self, row_idx: int, column: str, new_val: str):
        # Guardar cambios en buffer global (persisten entre páginas)
        rows = self.table.get_rows()
        if row_idx < 0 or row_idx >= len(rows):
            return
        codigo = self._norm_codigo(rows[row_idx].get("CÓDIGO_SNIES_DEL_PROGRAMA"))
        if not codigo:
            return
        
        # Validación inteligente: Si intenta marcar ES_REFERENTE='Sí', validar niveles
        if column == "ES_REFERENTE" and new_val.upper() in ("SÍ", "SI", "YES", "1", "TRUE"):
            nivel_programa = rows[row_idx].get("NIVEL_DE_FORMACIÓN", "")
            programa_eafit_codigo = rows[row_idx].get("PROGRAMA_EAFIT_CODIGO", "")
            programa_eafit_nombre = rows[row_idx].get("PROGRAMA_EAFIT_NOMBRE", "")
            
            # Si hay programa EAFIT asignado, validar niveles
            if programa_eafit_codigo or programa_eafit_nombre:
                nivel_valido = self._validar_niveles_coinciden(nivel_programa, programa_eafit_codigo, programa_eafit_nombre)
                if not nivel_valido[0]:
                    # Mostrar alerta
                    respuesta = messagebox.askyesno(
                        "Validación de Niveles",
                        f"⚠️ Advertencia: Los niveles de formación no coinciden.\n\n"
                        f"Programa SNIES: {nivel_programa or '(sin nivel)'}\n"
                        f"Programa EAFIT: {nivel_valido[1] or '(sin nivel)'}\n\n"
                        f"Regla del sistema: Solo se consideran referentes programas del mismo nivel.\n\n"
                        f"¿Deseas continuar de todas formas?",
                        parent=self
                    )
                    if not respuesta:
                        # Revertir cambio en la tabla
                        self.table.set_cell_value(row_idx, column, rows[row_idx].get(column, ""))
                        return
        
        upd = self.pending_updates.setdefault(codigo, {})
        upd[column] = new_val
        upd["AJUSTE_MANUAL"] = True
        upd["FECHA_AJUSTE"] = self._now_iso()
        self._touch_pending()
    
    def _validar_niveles_coinciden(self, nivel_programa: str, programa_eafit_codigo: str, programa_eafit_nombre: str) -> tuple[bool, str]:
        """
        Valida si los niveles de formación coinciden entre programa SNIES y programa EAFIT.
        
        Returns:
            (coinciden: bool, nivel_eafit: str)
        """
        from etl.clasificacionProgramas import normalizar_nivel_formacion, cargar_catalogo_eafit
        
        if not nivel_programa or pd.isna(nivel_programa):
            return (False, "")
        
        nivel_programa_norm = normalizar_nivel_formacion(str(nivel_programa))
        if not nivel_programa_norm:
            return (False, "")
        
        # Cargar catálogo EAFIT para obtener nivel del programa EAFIT
        try:
            df_catalogo = cargar_catalogo_eafit()
            
            # Buscar programa EAFIT por código o nombre
            nivel_eafit = ""
            if programa_eafit_codigo:
                # Intentar buscar por código si existe la columna
                if "Código Programa" in df_catalogo.columns:
                    mask = df_catalogo["Código Programa"] == programa_eafit_codigo
                    if mask.any():
                        idx = df_catalogo[mask].index[0]
                        nivel_eafit = df_catalogo.loc[idx, "NIVEL_DE_FORMACIÓN"] if "NIVEL_DE_FORMACIÓN" in df_catalogo.columns else ""
            
            if not nivel_eafit and programa_eafit_nombre:
                mask = df_catalogo["Nombre Programa EAFIT"] == programa_eafit_nombre
                if mask.any():
                    idx = df_catalogo[mask].index[0]
                    nivel_eafit = df_catalogo.loc[idx, "NIVEL_DE_FORMACIÓN"] if "NIVEL_DE_FORMACIÓN" in df_catalogo.columns else ""
            
            if not nivel_eafit:
                return (False, "")
            
            nivel_eafit_norm = normalizar_nivel_formacion(str(nivel_eafit))
            coinciden = nivel_programa_norm == nivel_eafit_norm
            
            return (coinciden, nivel_eafit)
        except Exception:
            # Si hay error al cargar catálogo, permitir pero advertir
            return (False, "")

    def _on_select(self, _evt=None):
        idx = self.table.get_selected_index()
        if idx is None:
            self.selection_label.config(text="Selección: -")
            return
        rows = self.table.get_rows()
        if 0 <= idx < len(rows):
            codigo = self._norm_codigo(rows[idx].get("CÓDIGO_SNIES_DEL_PROGRAMA"))
            self.selection_label.config(text=f"Selección: {codigo}")
        else:
            self.selection_label.config(text="Selección: -")

    def _pipeline_running(self) -> bool:
        lock_file = get_pipeline_lock_file()
        return lock_file.exists()

    def _poll_lock(self):
        lock_file = get_pipeline_lock_file()
        age = get_lock_age_seconds(lock_file)
        running = age is not None and age < LOCK_STALE_SECONDS
        stale = age is not None and age >= LOCK_STALE_SECONDS

        if running:
            # Deshabilitar acciones que escriben mientras el pipeline reescribe Programas.xlsx
            try:
                self.btn_save.config(state=tk.DISABLED)
                self.btn_delete.config(state=tk.DISABLED)
            except Exception:
                pass
            self.readonly_banner.config(
                text="Modo solo lectura: el pipeline está en ejecución. Espera a que termine para guardar cambios."
            )
        elif stale:
            try:
                self.btn_save.config(state=tk.DISABLED)
                self.btn_delete.config(state=tk.DISABLED)
            except Exception:
                pass
            self.readonly_banner.config(
                text="Lock antiguo detectado. Si el pipeline NO está corriendo, usa 'Desbloquear' en el Menú Principal."
            )
        else:
            try:
                self.btn_save.config(state=tk.NORMAL)
                self.btn_delete.config(state=tk.NORMAL)
            except Exception:
                pass
            self.readonly_banner.config(text="")
        # Repetir cada 1s
        try:
            root = self.winfo_toplevel()
            root.after(1000, self._poll_lock)
        except Exception:
            pass

    def _log(self, s: str):
        self.msg.config(state=tk.NORMAL)
        ts = time.strftime("%H:%M:%S")
        self.msg.insert(tk.END, f"[{ts}] {s}\n")
        self.msg.see(tk.END)
        self.msg.config(state=tk.DISABLED)

    def _open_excel(self):
        try:
            _open_in_excel(self.file_path)
        except Exception as exc:
            messagebox.showerror("Error", str(exc), parent=self)

    def _load(self):
        import pandas as pd  # Lazy import
        
        if not self.file_path.exists():
            messagebox.showerror(
                "Error",
                f"No existe {self.file_path}. Primero ejecuta el análisis SNIES.",
                parent=self,
            )
            return
        ok, msg = validate_programas_schema(self.file_path)
        if not ok:
            messagebox.showerror("Error", msg, parent=self)
            return
        try:
            if self._pipeline_running():
                self._log("⚠️ El pipeline está en ejecución. Programas.xlsx puede estar cambiando. Puedes recargar cuando termine.")
            
            # Leer todas las columnas usando función con reintentos
            from etl.exceptions_helpers import leer_excel_con_reintentos
            df_full = leer_excel_con_reintentos(self.file_path, sheet_name="Programas")
            
            # Columnas que debe tener el archivo: datos SNIES + resultado de la clasificación contra EAFIT
            required_base = [
                "CÓDIGO_SNIES_DEL_PROGRAMA",
                "NOMBRE_INSTITUCIÓN",
                "NOMBRE_DEL_PROGRAMA",
                "NIVEL_DE_FORMACIÓN",
            ]
            # Estas columnas las genera el pipeline al comparar SNIES con el catálogo EAFIT (referente sí/no y programa EAFIT asignado)
            required_classification = [
                "PROGRAMA_NUEVO",
                "ES_REFERENTE",
                "PROBABILIDAD",
                "PROGRAMA_EAFIT_CODIGO",
                "PROGRAMA_EAFIT_NOMBRE",
            ]
            missing_base = [c for c in required_base if c not in df_full.columns]
            if missing_base:
                messagebox.showerror(
                    "Error",
                    f"El archivo no tiene las columnas mínimas del SNIES:\n{', '.join(missing_base)}\n\n"
                    "Ejecuta primero el análisis SNIES (Pipeline).",
                    parent=self,
                )
                return
            
            missing_classification = [c for c in required_classification if c not in df_full.columns]
            if missing_classification:
                # PROPUESTA: Ejecutar clasificación automáticamente si faltan las columnas
                respuesta = messagebox.askyesno(
                    "Clasificación requerida",
                    "Este archivo aún no tiene la clasificación de referentes.\n\n"
                    "El sistema debe comparar cada programa del SNIES con el catálogo EAFIT (catalogoOfertasEAFIT) "
                    "y generar para cada uno: si es referente o no, y el programa EAFIT asignado (código y nombre).\n\n"
                    "¿Deseas ejecutar la clasificación ahora? (Esto puede tardar varios minutos)",
                    parent=self,
                )
                if not respuesta:
                    return
                
                # Ejecutar clasificación en hilo separado
                self._log("Ejecutando clasificación de programas nuevos...")
                self._log("Esto puede tardar varios minutos. Por favor espera...")
                
                # Obtener referencia a la ventana raíz antes de entrar al hilo
                root_window = self.winfo_toplevel()
                
                def ejecutar_clasificacion():
                    try:
                        from etl.clasificacionProgramas import clasificar_programas_nuevos
                        clasificar_programas_nuevos()
                        root_window.after(0, lambda: self._log("✓ Clasificación completada. Recargando datos..."))
                        root_window.after(0, self._load)  # Recargar después de clasificar
                    except FileNotFoundError as exc:
                        # Error específico cuando faltan modelos entrenados
                        error_msg = (
                            "No se encontraron los modelos de Machine Learning entrenados.\n\n"
                            "Para poder clasificar programas nuevos, primero debes entrenar el modelo:\n"
                            "1. Ve al menú principal\n"
                            "2. Selecciona 'Reentrenamiento del modelo'\n"
                            "3. Guarda los cambios y ejecuta el entrenamiento\n\n"
                            f"Detalle técnico: {exc}"
                        )
                        root_window.after(0, lambda: self._log(f"✗ {error_msg}"))
                        root_window.after(0, lambda: messagebox.showerror("Modelos no encontrados", error_msg, parent=self))
                    except Exception as exc:
                        error_msg = f"Error al ejecutar clasificación: {exc}"
                        root_window.after(0, lambda: self._log(f"✗ {error_msg}"))
                        root_window.after(0, lambda: messagebox.showerror("Error", error_msg, parent=self))
                
                threading.Thread(target=ejecutar_clasificacion, daemon=True).start()
                return  # Salir aquí, se recargará cuando termine la clasificación
            
            # Guardar todas las columnas disponibles
            self.all_columns = list(df_full.columns)
            # Por defecto mostrar todas las columnas (vista completa)
            self.view_complete = True
            self.display_columns = self.all_columns.copy()
            # Recrear la tabla con todas las columnas (el Treeview se creó con un subconjunto en __init__)
            self._recreate_table()
            
            # Actualizar texto del botón y habilitarlo
            self.btn_toggle_view.config(text="Vista principal", state=tk.NORMAL)

            self.df_view = df_full[self.all_columns].copy()  # Guardar todas las columnas en df_view
            self._log(f"Cargado: {self.file_path.name} ({len(self.df_view)} filas, {len(self.all_columns)} columnas disponibles).")
            self._apply_filter()
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo leer el Excel: {exc}", parent=self)

    def _recreate_table(self):
        """Recrea la tabla con las columnas actuales en display_columns."""
        self.table.destroy()
        self.table = EditableTable(
            self,
            columns=self.display_columns,
            height=18,
            editable_columns=self.editable_columns,
            on_change=self._on_cell_change,
        )
        self.table.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        self.table.tree.bind("<<TreeviewSelect>>", self._on_select)

    def _toggle_view(self):
        """Alterna entre vista completa (todas las columnas) y vista principal (9 columnas)."""
        if not self.all_columns:
            self._log("⚠️ Primero carga el archivo Programas.xlsx")
            return
        
        # Alternar estado
        self.view_complete = not self.view_complete
        
        if self.view_complete:
            # Cambiar a vista completa (todas las columnas)
            self.display_columns = self.all_columns.copy()
            self.btn_toggle_view.config(text="Vista principal")
            self._log(f"Vista completa activada ({len(self.display_columns)} columnas)")
        else:
            # Cambiar a vista principal (solo las 9 columnas principales)
            # Asegurar que las columnas principales existan en el archivo
            self.display_columns = [c for c in self.main_columns if c in self.all_columns]
            self.btn_toggle_view.config(text="Vista completa")
            self._log(f"Vista principal activada ({len(self.display_columns)} columnas)")
        
        # Recrear la tabla con las nuevas columnas
        self._recreate_table()
        
        # Reaplicar filtros con las nuevas columnas
        self._apply_filter()

    def _render_page(self):
        if self._filtered_df is None:
            self.table.set_rows([])
            self.page_label.config(text="Página: -")
            return
        total = len(self._filtered_df)
        if total == 0:
            self.table.set_rows([])
            self.page_label.config(text="Página: 0/0")
            return
        max_pages = max(1, (total + self.page_size - 1) // self.page_size)
        self.page_index = max(0, min(self.page_index, max_pages - 1))
        start = self.page_index * self.page_size
        end = min(total, start + self.page_size)
        df_page = self._filtered_df.iloc[start:end].copy()

        # Aplicar cambios pendientes a la vista (persisten entre páginas)
        if self.pending_updates:
            for i in range(len(df_page)):
                codigo = self._norm_codigo(df_page.iloc[i].get("CÓDIGO_SNIES_DEL_PROGRAMA"))
                if codigo in self.pending_updates:
                    for k, v in self.pending_updates[codigo].items():
                        if k in df_page.columns:
                            df_page.at[df_page.index[i], k] = v

        rows = df_page.to_dict(orient="records")
        self.table.set_rows(rows)
        self.page_label.config(text=f"Página: {self.page_index + 1}/{max_pages}  (filas {start + 1}-{end} de {total})")
        self._touch_pending()

    def _prev_page(self):
        if self._filtered_df is None:
            return
        if self.page_index > 0:
            self.page_index -= 1
            self._render_page()

    def _next_page(self):
        if self._filtered_df is None:
            return
        total = len(self._filtered_df)
        max_pages = max(1, (total + self.page_size - 1) // self.page_size)
        if self.page_index < (max_pages - 1):
            self.page_index += 1
            self._render_page()

    def _apply_filter(self):
        if self.df_view is None:
            return
        df = self.df_view.copy()
        mode = self.filter_var.get()
        
        # Aplicar filtro por modo
        if mode == "SOLO_NUEVOS":
            if "PROGRAMA_NUEVO" in df.columns:
                # Filtrar por programas nuevos (puede ser "Sí", "Sí ", " Sí", etc.)
                df = df[df["PROGRAMA_NUEVO"].astype(str).str.strip().str.upper() == "SÍ"]
            else:
                self._log("⚠️ Advertencia: No se encontró la columna PROGRAMA_NUEVO. Mostrando todos los programas.")
        elif mode == "SOLO_REFERENTES":
            if "ES_REFERENTE" in df.columns:
                # Filtrar por referentes (puede ser "Sí", "Sí ", " Sí", etc.)
                df = df[df["ES_REFERENTE"].astype(str).str.strip().str.upper() == "SÍ"]
            else:
                self._log("⚠️ Advertencia: No se encontró la columna ES_REFERENTE. Mostrando todos los programas.")
        # mode == "TODOS" no filtra nada

        # Aplicar búsqueda de texto
        q = (self.search_var.get() or "").strip().lower()
        if q:
            # buscar por código o por texto en nombre del programa/institución
            for col in ("CÓDIGO_SNIES_DEL_PROGRAMA", "NOMBRE_DEL_PROGRAMA", "NOMBRE_INSTITUCIÓN"):
                if col not in df.columns:
                    df[col] = ""
            mask = (
                df["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.lower().str.contains(q, na=False)
                | df["NOMBRE_DEL_PROGRAMA"].astype(str).str.lower().str.contains(q, na=False)
                | df["NOMBRE_INSTITUCIÓN"].astype(str).str.lower().str.contains(q, na=False)
            )
            df = df[mask]

        # asegurar columnas
        for c in self.display_columns:
            if c not in df.columns:
                df[c] = ""

        df_view = df[self.display_columns].fillna("")
        self._filtered_df = df_view
        self.page_index = 0
        self._render_page()
        self._log(f"Filtro aplicado ({mode}). Total filas: {len(df_view)}")

    def _mark_si_referente(self):
        """Marca la fila seleccionada como referente (ES_REFERENTE = 'Sí')."""
        idx = self.table.get_selected_index()
        if idx is None:
            messagebox.showinfo("Info", "Selecciona una fila para marcarla.", parent=self)
            return
        rows = self.table.get_rows()
        if idx < 0 or idx >= len(rows):
            return
        row = rows[idx]
        
        # Validar niveles antes de marcar como referente
        nivel_programa = row.get("NIVEL_DE_FORMACIÓN", "")
        programa_eafit_codigo = row.get("PROGRAMA_EAFIT_CODIGO", "")
        programa_eafit_nombre = row.get("PROGRAMA_EAFIT_NOMBRE", "")
        
        # Si hay programa EAFIT asignado, validar niveles
        if programa_eafit_codigo or programa_eafit_nombre:
            nivel_valido = self._validar_niveles_coinciden(nivel_programa, programa_eafit_codigo, programa_eafit_nombre)
            if not nivel_valido[0]:
                respuesta = messagebox.askyesno(
                    "Validación de Niveles",
                    f"⚠️ Advertencia: Los niveles de formación no coinciden.\n\n"
                    f"Programa SNIES: {nivel_programa or '(sin nivel)'}\n"
                    f"Programa EAFIT: {nivel_valido[1] or '(sin nivel)'}\n\n"
                    f"Regla del sistema: Solo se consideran referentes programas del mismo nivel.\n\n"
                    f"¿Deseas marcarlo como referente de todas formas?",
                    parent=self
                )
                if not respuesta:
                    return
        
        # Si no hay programa EAFIT asignado, advertir
        if not programa_eafit_codigo and not programa_eafit_nombre:
            respuesta = messagebox.askyesno(
                "Programa EAFIT no asignado",
                "No hay un programa EAFIT asignado para este programa.\n\n"
                "¿Deseas marcarlo como referente de todas formas?\n"
                "(Puedes asignar el programa EAFIT después editando las columnas PROGRAMA_EAFIT_*)",
                parent=self
            )
            if not respuesta:
                return
        
        row["ES_REFERENTE"] = "Sí"
        codigo = self._norm_codigo(row.get("CÓDIGO_SNIES_DEL_PROGRAMA"))
        if codigo:
            upd = self.pending_updates.setdefault(codigo, {})
            upd["ES_REFERENTE"] = "Sí"
            upd["AJUSTE_MANUAL"] = True
            upd["FECHA_AJUSTE"] = self._now_iso()
            self._touch_pending()
        self.table.set_rows(rows)
        self._log("Fila marcada como SÍ referente.")
    
    def _mark_no_referente(self):
        """Marca la fila seleccionada como NO referente (ES_REFERENTE = 'No')."""
        idx = self.table.get_selected_index()
        if idx is None:
            messagebox.showinfo("Info", "Selecciona una fila para marcarla.", parent=self)
            return
        rows = self.table.get_rows()
        if idx < 0 or idx >= len(rows):
            return
        row = rows[idx]
        row["ES_REFERENTE"] = "No"
        row["PROGRAMA_EAFIT_CODIGO"] = ""
        row["PROGRAMA_EAFIT_NOMBRE"] = ""
        codigo = self._norm_codigo(row.get("CÓDIGO_SNIES_DEL_PROGRAMA"))
        if codigo:
            upd = self.pending_updates.setdefault(codigo, {})
            upd["ES_REFERENTE"] = "No"
            upd["PROGRAMA_EAFIT_CODIGO"] = ""
            upd["PROGRAMA_EAFIT_NOMBRE"] = ""
            upd["AJUSTE_MANUAL"] = True
            upd["FECHA_AJUSTE"] = self._now_iso()
            self._touch_pending()
        self.table.set_rows(rows)
        self._log("Fila marcada como NO referente (se limpió programa EAFIT asignado).")

    def _discard_row_changes(self):
        """
        Mitigación UX/negocio:
        Antes existía "Eliminar fila" (solo quitaba de la vista y podía confundir).
        Ahora esta acción descarta los cambios pendientes (buffer) de la fila seleccionada.
        """
        idx = self.table.get_selected_index()
        if idx is None:
            messagebox.showinfo("Info", "Selecciona una fila.", parent=self)
            return
        rows = self.table.get_rows()
        if not (0 <= idx < len(rows)):
            return
        codigo = self._norm_codigo(rows[idx].get("CÓDIGO_SNIES_DEL_PROGRAMA"))
        if not codigo:
            messagebox.showwarning("Atención", "La fila no tiene CÓDIGO_SNIES_DEL_PROGRAMA válido.", parent=self)
            return
        if codigo not in self.pending_updates:
            messagebox.showinfo("Info", "Esa fila no tiene cambios pendientes.", parent=self)
            return
        if not _ask_yes_no("Confirmar", "¿Descartar los cambios pendientes de esta fila?"):
            return
        try:
            self.pending_updates.pop(codigo, None)
        finally:
            self._touch_pending()
            self._render_page()
        self._log(f"Cambios pendientes descartados para: {codigo}")

    def _discard_all_changes(self):
        if not self.pending_updates:
            messagebox.showinfo("Info", "No hay cambios pendientes.", parent=self)
            return
        if not _ask_yes_no("Confirmar", "¿Descartar TODOS los cambios pendientes (todas las páginas)?"):
            return
        self.pending_updates.clear()
        self._touch_pending()
        self._render_page()
        self._log("Se descartaron todos los cambios pendientes.")

    def _save(self):
        import pandas as pd  # Lazy import
        
        if self._pipeline_running():
            messagebox.showwarning(
                "Atención",
                "El pipeline está en ejecución y está reescribiendo Programas.xlsx.\n\n"
                "Espera a que termine y vuelve a intentar guardar.",
                parent=self,
            )
            return
        if not self.pending_updates:
            messagebox.showinfo("Info", "No hay cambios pendientes para guardar.", parent=self)
            return
        self._log("Guardando cambios (todas las páginas) por CÓDIGO_SNIES_DEL_PROGRAMA...")

        # MITIGACIÓN P0 (CRÍTICA): NO perder columnas SNIES al guardar.
        # Leemos el Excel COMPLETO y aplicamos SOLO los cambios en columnas editables.
        try:
            from etl.exceptions_helpers import leer_excel_con_reintentos
            base_full = leer_excel_con_reintentos(self.file_path, sheet_name="Programas")
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo leer Programas.xlsx completo: {exc}", parent=self)
            return

        if "CÓDIGO_SNIES_DEL_PROGRAMA" not in base_full.columns:
            safe_messagebox_error("Error", "El archivo no tiene CÓDIGO_SNIES_DEL_PROGRAMA.", parent=self)
            return

        base_full["_CODIGO_NORM"] = base_full["CÓDIGO_SNIES_DEL_PROGRAMA"].apply(self._norm_codigo)

        for c in ["AJUSTE_MANUAL", "FECHA_AJUSTE"]:
            if c not in base_full.columns:
                base_full[c] = None

        for codigo, changes in self.pending_updates.items():
            mask = base_full["_CODIGO_NORM"] == codigo
            if not mask.any():
                continue
            for col, val in changes.items():
                if col in self.editable_columns or col in ("AJUSTE_MANUAL", "FECHA_AJUSTE"):
                    if col not in base_full.columns:
                        base_full[col] = None
                    base_full.loc[mask, col] = val

        try:
            with pd.ExcelWriter(self.file_path, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
                base_full.drop(columns=["_CODIGO_NORM"]).to_excel(writer, sheet_name="Programas", index=False)
            self.pending_updates.clear()
            self._touch_pending()
            self._load()
            self._log("✓ Cambios guardados en Programas.xlsx")
            messagebox.showinfo(
                "Guardado",
                "Los cambios se guardaron correctamente en Programas.xlsx.",
                parent=self
            )
        except PermissionError:
            safe_messagebox_error("Error", explain_file_in_use(), parent=self)
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo guardar: {exc}", parent=self)
    
    def _restore_backup(self):
        """Restaura el estado anterior desde el backup oculto."""
        if not self.last_backup_path or not self.last_backup_path.exists():
            messagebox.showwarning(
                "No hay backup",
                "No hay un backup disponible para restaurar.\n\n"
                "El backup solo está disponible después de guardar cambios.",
                parent=self
            )
            return
        
        if not _ask_yes_no(
            "Confirmar Restauración",
            f"¿Restaurar el estado anterior desde {self.last_backup_path.name}?\n\n"
            "Esto sobrescribirá todos los cambios guardados en Programas.xlsx.\n"
            "Los cambios pendientes (no guardados) se perderán.",
            parent=self
        ):
            return
        
        import pandas as pd
        
        try:
            # Leer backup usando función con reintentos
            from etl.exceptions_helpers import leer_excel_con_reintentos
            df_backup = leer_excel_con_reintentos(self.last_backup_path, sheet_name="Programas")
            
            # Escribir sobre Programas.xlsx
            with pd.ExcelWriter(self.file_path, mode="w", engine="openpyxl") as writer:
                df_backup.to_excel(writer, sheet_name="Programas", index=False)
            
            # Limpiar cambios pendientes
            self.pending_updates.clear()
            self._touch_pending()
            
            # Recargar
            self._load()
            
            # Deshabilitar botón de restaurar
            self.btn_restore.config(state=tk.DISABLED)
            self.last_backup_path = None
            
            self._log("✓ Estado anterior restaurado desde backup")
            messagebox.showinfo(
                "Restaurado",
                "El estado anterior se restauró correctamente desde el backup.",
                parent=self
            )
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo restaurar: {exc}", parent=self)


class RetrainPage(ttk.Frame):
    """Edición de referentes + reentrenamiento del modelo."""

    def __init__(self, parent: tk.Misc, on_back=None):
        super().__init__(parent)
        self.on_back = on_back
        
        # Import lazy de pandas y módulos ETL (solo cuando se abre esta página)
        import pandas as pd
        from etl.config import get_archivo_referentes, leer_datos_flexible, REF_DIR
        
        self.base_dir = ensure_base_dir(self)
        if not self.base_dir:
            if on_back:
                on_back()
            return

        self._get_referentes = get_archivo_referentes
        self._leer = leer_datos_flexible
        self.file_path = self._get_referentes()
        self.df = None  # type: ignore

        header = ttk.Frame(self, padding=12, style="Page.TFrame")
        header.pack(fill=tk.X)
        header_left = ttk.Frame(header, style="Page.TFrame")
        header_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(header_left, text="🎯 Reentrenamiento del modelo", style="Header.TLabel").pack(anchor="w")
        self.subheader_label = ttk.Label(header_left, text="Edita referentesUnificados (ref/). Guarda cambios y luego reentrena para que el modelo use los nuevos referentes.", style="SubHeader.TLabel")
        self.subheader_label.pack(anchor="w", pady=(4, 0), fill=tk.X)
        if on_back:
            ttk.Button(header, text="← Volver al menú", command=lambda: on_back() if on_back else None, style="Back.TButton").pack(side=tk.RIGHT)

        # Botones en grid para mejor organización responsive
        btns = ttk.Frame(self, padding="10")
        btns.pack(fill=tk.X)
        btns.grid_columnconfigure(0, weight=1, uniform="btn_col")
        btns.grid_columnconfigure(1, weight=1, uniform="btn_col")
        
        # Fila 1: Archivo y edición
        row1 = ttk.Frame(btns, style="App.TFrame")
        row1.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(row1, text="Cargar archivo de referentes", command=self._load).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(row1, text="Abrir en Excel", command=self._open_excel).pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="Agregar fila", command=self._add_row).pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="Eliminar fila", command=self._delete_row).pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="Guardar cambios", command=self._save).pack(side=tk.LEFT, padx=6)
        
        # Fila 2: Sincronización y entrenamiento
        row2 = ttk.Frame(btns, style="App.TFrame")
        row2.grid(row=1, column=0, columnspan=2, sticky="ew")
        ttk.Button(row2, text="Sincronizar ajustes manuales", command=self._sync_manual_adjustments).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(row2, text="Simular reentrenamiento", command=self._dry_run_train).pack(side=tk.LEFT, padx=6)
        ttk.Button(row2, text="Reentrenar modelo", command=self._train).pack(side=tk.LEFT, padx=6)
        
        # Selector de versión y rollback
        version_frame = ttk.Frame(self, padding="10")
        version_frame.pack(fill=tk.X)
        ttk.Label(version_frame, text="Versión del modelo:").pack(side=tk.LEFT, padx=(0, 6))
        self.version_var = tk.StringVar(value="actual")
        self.version_combo = ttk.Combobox(
            version_frame,
            textvariable=self.version_var,
            state="readonly",
            width=15
        )
        self.version_combo.pack(side=tk.LEFT, padx=6)
        # NO llamar _update_version_list() aquí porque self.msg aún no existe
        # Se llamará después de crear self.msg
        ttk.Button(version_frame, text="Usar esta versión", command=self._switch_version).pack(side=tk.LEFT, padx=6)
        ttk.Button(version_frame, text="Rollback a versión anterior", command=self._rollback_version).pack(side=tk.LEFT, padx=6)

        self.msg = tk.Text(self, height=7, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9), bg=EAFIT["card_bg"], fg=EAFIT["text"])
        self.msg.pack(fill=tk.X, padx=10, pady=(0, 10))

        cols = [
            "NOMBRE_DEL_PROGRAMA",
            "NombrePrograma EAFIT",
            "CAMPO_AMPLIO",
            "CAMPO_AMPLIO_EAFIT",
            "NIVEL_DE_FORMACIÓN",
            "NIVEL_DE_FORMACIÓN EAFIT",
            "label",
        ]
        self.table = EditableTable(self, columns=cols, height=20)
        self.table.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self._log(f"Archivo actual detectado: {self.file_path}")
        self._log("Tip: mantén label=1 para referentes confirmados (es lo que usa el entrenamiento).")
        self._update_version_list()
    
    def _update_version_list(self):
        """Actualiza la lista de versiones disponibles en el combobox."""
        try:
            from etl.clasificacionProgramas import listar_versiones_modelos
            
            versiones = listar_versiones_modelos()
            valores = ["actual"] + [f"v{v}" for v in versiones]
            self.version_combo['values'] = valores
            
            if versiones:
                self._log(f"Versiones disponibles: actual, {', '.join([f'v{v}' for v in versiones])}")
            else:
                self._log("No hay versiones guardadas. El próximo entrenamiento creará v1.")
        except Exception as e:
            self._log(f"Error al listar versiones: {e}")
    
    def _switch_version(self):
        """Cambia la versión actual del modelo."""
        version_str = self.version_var.get()
        
        if version_str == "actual":
            messagebox.showinfo("Info", "Ya estás usando la versión actual.", parent=self)
            return
        
        try:
            version_num = int(version_str.replace("v", ""))
            from etl.clasificacionProgramas import obtener_rutas_modelo_version, MODELS_DIR
            import shutil
            
            ruta_clasificador, ruta_embeddings, ruta_encoder = obtener_rutas_modelo_version(version_num)
            
            # Verificar que existan
            if not all([ruta_clasificador.exists(), ruta_embeddings.exists(), ruta_encoder.exists()]):
                messagebox.showerror("Error", f"La versión {version_str} no existe completamente.", parent=self)
                return
            
            # Hacer backup de versión actual si existe
            from etl.clasificacionProgramas import MODELO_CLASIFICADOR, MODELO_EMBEDDINGS_OBJ, ENCODER_PROGRAMAS_EAFIT
            if MODELO_CLASIFICADOR.exists():
                backup_version = version_num - 1 if version_num > 1 else 1
                ruta_backup_clasificador, ruta_backup_embeddings, ruta_backup_encoder = obtener_rutas_modelo_version(backup_version)
                try:
                    shutil.copy2(MODELO_CLASIFICADOR, ruta_backup_clasificador)
                    shutil.copy2(MODELO_EMBEDDINGS_OBJ, ruta_backup_embeddings)
                    shutil.copy2(ENCODER_PROGRAMAS_EAFIT, ruta_backup_encoder)
                except Exception:
                    pass
            
            # Copiar versión seleccionada a versión actual
            shutil.copy2(ruta_clasificador, MODELO_CLASIFICADOR)
            shutil.copy2(ruta_embeddings, MODELO_EMBEDDINGS_OBJ)
            shutil.copy2(ruta_encoder, ENCODER_PROGRAMAS_EAFIT)
            
            self._log(f"✓ Versión {version_str} establecida como versión actual")
            messagebox.showinfo("Versión cambiada", f"La versión {version_str} ahora es la versión actual.", parent=self)
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo cambiar la versión: {exc}", parent=self)
    
    def _rollback_version(self):
        """Hace rollback a la versión anterior."""
        try:
            from etl.clasificacionProgramas import listar_versiones_modelos, obtener_rutas_modelo_version, MODELO_CLASIFICADOR
            import shutil
            
            versiones = listar_versiones_modelos()
            if not versiones or len(versiones) < 2:
                messagebox.showwarning(
                    "No hay versión anterior",
                    "No hay suficientes versiones para hacer rollback.\n\n"
                    "Necesitas al menos 2 versiones guardadas.",
                    parent=self
                )
                return
            
            # Obtener versión anterior (la penúltima)
            version_anterior = versiones[-2]
            
            if not _ask_yes_no(
                "Confirmar Rollback",
                f"¿Hacer rollback a la versión v{version_anterior}?\n\n"
                f"Esto sobrescribirá la versión actual con v{version_anterior}.",
                parent=self
            ):
                return
            
            ruta_clasificador, ruta_embeddings, ruta_encoder = obtener_rutas_modelo_version(version_anterior)
            
            # Verificar que existan
            if not all([ruta_clasificador.exists(), ruta_embeddings.exists(), ruta_encoder.exists()]):
                messagebox.showerror("Error", f"La versión v{version_anterior} no existe completamente.", parent=self)
                return
            
            # Copiar versión anterior a versión actual
            from etl.clasificacionProgramas import MODELO_CLASIFICADOR, MODELO_EMBEDDINGS_OBJ, ENCODER_PROGRAMAS_EAFIT
            shutil.copy2(ruta_clasificador, MODELO_CLASIFICADOR)
            shutil.copy2(ruta_embeddings, MODELO_EMBEDDINGS_OBJ)
            shutil.copy2(ruta_encoder, ENCODER_PROGRAMAS_EAFIT)
            
            self._log(f"✓ Rollback completado: versión actual ahora es v{version_anterior}")
            messagebox.showinfo("Rollback completado", f"La versión actual ahora es v{version_anterior}.", parent=self)
            self._update_version_list()
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo hacer rollback: {exc}", parent=self)
    
    def _dry_run_train(self):
        """Simula el reentrenamiento y muestra métricas estimadas."""
        if self.df is None:
            messagebox.showwarning("Atención", "Primero carga el archivo de referentes.", parent=self)
            return
        
        self._log("Simulando reentrenamiento...")
        
        def run():
            try:
                import pandas as pd
                from etl.clasificacionProgramas import (
                    cargar_referentes,
                    entrenar_modelo,
                    preparar_features_entrenamiento,
                    _get_sentence_transformer
                )
                
                # Cargar datos actuales
                df_actual = self._leer(self.file_path)
                
                # Validar
                ok, msg = self._validate_referentes(df_actual.copy())
                if not ok:
                    self.after(0, lambda: safe_messagebox_error("Error", f"No se puede simular: {msg}", parent=self))
                    return
                
                # Cargar modelo de embeddings
                SentenceTransformer = _get_sentence_transformer()
                modelo_embeddings = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
                
                # Preparar features
                features, labels, encoder = preparar_features_entrenamiento(df_actual, modelo_embeddings)
                
                # Entrenar modelo temporal
                modelo_temp, metricas = entrenar_modelo(features, labels)
                
                # Comparar con modelo actual si existe
                accuracy_actual = None
                try:
                    from etl.clasificacionProgramas import cargar_modelos
                    modelo_actual, _, _ = cargar_modelos()
                    # Evaluar modelo actual con mismo test set
                    from sklearn.model_selection import train_test_split
                    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)
                    accuracy_actual = modelo_actual.score(X_test, y_test)
                except Exception:
                    pass
                
                # Mostrar resultados
                accuracy_nuevo = metricas['accuracy']
                
                mensaje = f"Simulación completada:\n\n"
                if accuracy_actual is not None:
                    mensaje += f"Precisión actual: {accuracy_actual:.2%}\n"
                    mensaje += f"Precisión nueva (estimada): {accuracy_nuevo:.2%}\n\n"
                    diferencia = accuracy_nuevo - accuracy_actual
                    if diferencia > 0:
                        mensaje += f"✅ Mejora: +{diferencia:.2%}"
                    elif diferencia < 0:
                        mensaje += f"⚠️ Empeora: {diferencia:.2%}"
                    else:
                        mensaje += "➡️ Sin cambios"
                else:
                    mensaje += f"Precisión nueva (estimada): {accuracy_nuevo:.2%}\n\n"
                    mensaje += "(No hay modelo actual para comparar)"
                
                self.after(0, lambda: self._log(f"Simulación: {mensaje}"))
                self.after(0, lambda: messagebox.showinfo("Simulación de Reentrenamiento", mensaje, parent=self))
            except Exception as exc:
                self.after(0, lambda: self._log(f"✗ Error en simulación: {exc}"))
                self.after(0, lambda: safe_messagebox_error("Error", f"No se pudo simular: {exc}", parent=self))
        
        threading.Thread(target=run, daemon=True).start()

    def _log(self, s: str):
        # Verificar que self.msg existe antes de usarlo (para evitar errores durante inicialización)
        if not hasattr(self, 'msg') or self.msg is None:
            return
        try:
            self.msg.config(state=tk.NORMAL)
            ts = time.strftime("%H:%M:%S")
            self.msg.insert(tk.END, f"[{ts}] {s}\n")
            self.msg.see(tk.END)
            self.msg.config(state=tk.DISABLED)
        except (tk.TclError, AttributeError):
            # Si el widget fue destruido o no está disponible, ignorar silenciosamente
            pass

    def _open_excel(self):
        try:
            _open_in_excel(self.file_path)
        except Exception as exc:
            messagebox.showerror("Error", str(exc), parent=self)

    def _load(self):
        if not self.file_path.exists():
            messagebox.showerror("Error", f"No existe el archivo: {self.file_path}", parent=self)
            return
        try:
            self.df = self._leer(self.file_path)
            self._log(f"Cargado: {self.file_path.name} ({len(self.df)} filas)")
            # asegurar columnas
            for c in self.table.columns:
                if c not in self.df.columns:
                    self.df[c] = ""
            df_view = self.df[self.table.columns].fillna("")
            self.table.set_rows(df_view.to_dict(orient="records"))
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo cargar: {exc}", parent=self)

    def _add_row(self):
        self.table.add_row({"label": 1})

    def _delete_row(self):
        if not _ask_yes_no("Confirmar", "¿Eliminar la fila seleccionada? (No guarda hasta Guardar cambios)"):
            return
        self.table.delete_selected()

    def _validate_referentes(self, df_out) -> tuple[bool, str]:
        import pandas as pd  # Lazy import
        required = [
            "NOMBRE_DEL_PROGRAMA",
            "NombrePrograma EAFIT",
            "label",
        ]
        missing = [c for c in required if c not in df_out.columns]
        if missing:
            return False, f"Faltan columnas requeridas: {', '.join(missing)}"

        # Normalizar label
        try:
            df_out["label"] = pd.to_numeric(df_out["label"], errors="coerce").fillna(0).astype(int)
        except Exception:
            return False, "La columna 'label' debe ser numérica (0/1)."

        n_pos = int((df_out["label"] == 1).sum())
        if n_pos == 0:
            return False, "No hay registros con label=1. El entrenamiento quedaría sin referentes confirmados."

        # Nombres mínimos
        if df_out["NOMBRE_DEL_PROGRAMA"].astype(str).str.strip().eq("").all():
            return False, "NOMBRE_DEL_PROGRAMA está vacío en todas las filas."
        if df_out["NombrePrograma EAFIT"].astype(str).str.strip().eq("").all():
            return False, "NombrePrograma EAFIT está vacío en todas las filas."

        return True, f"OK (label=1: {n_pos})"

    def _save(self):
        import pandas as pd  # Lazy import
        
        rows = self.table.get_rows()
        if not rows:
            messagebox.showwarning("Atención", "No hay filas para guardar.", parent=self)
            return
        df_out = pd.DataFrame(rows)
        ok, msg = self._validate_referentes(df_out)
        if not ok:
            safe_messagebox_error("Error", msg, parent=self)
            return
        self._log(f"Validación de referentes: {msg}")
        # backup
        try:
            backup = self.file_path.parent / f"{self.file_path.stem}__backup_{time.strftime('%Y%m%d_%H%M%S')}{self.file_path.suffix}"
            shutil.copy2(self.file_path, backup)
            self._log(f"Backup creado: {backup.name}")
        except Exception:
            pass

        try:
            if self.file_path.suffix.lower() == ".csv":
                df_out.to_csv(self.file_path, index=False, encoding="utf-8")
            else:
                with pd.ExcelWriter(self.file_path, mode="w", engine="openpyxl") as writer:
                    df_out.to_excel(writer, index=False)
            self._log("Cambios guardados en archivo de referentes.")
            messagebox.showinfo("OK", "Cambios guardados.", parent=self)
        except PermissionError:
            safe_messagebox_error("Error", explain_file_in_use(), parent=self)
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo guardar: {exc}", parent=self)

    def _train(self):
        import pandas as pd  # Lazy import
        
        if not _ask_yes_no("Confirmar", "¿Reentrenar el modelo ahora? Esto puede tardar varios minutos."):
            return
        # Validar archivo antes de entrenar
        try:
            df_tmp = self._leer(self.file_path)
        except Exception as exc:
            safe_messagebox_error("Error", f"No se pudo leer el archivo de referentes: {exc}", parent=self)
            return
        for c in self.table.columns:
            if c not in df_tmp.columns:
                df_tmp[c] = ""
        df_tmp = df_tmp[self.table.columns].fillna("")
        ok, msg = self._validate_referentes(df_tmp.copy())
        if not ok:
            safe_messagebox_error("Error", f"No se puede reentrenar: {msg}", parent=self)
            return
        self._log(f"Validación previa a entrenamiento: {msg}")
        self._log("Iniciando reentrenamiento...")

        def run():
            try:
                from etl.clasificacionProgramas import entrenar_y_guardar_modelo

                entrenar_y_guardar_modelo()
                self.after(0, lambda: self._log("✓ Reentrenamiento completado"))
                self.after(0, lambda: messagebox.showinfo("OK", "Modelo reentrenado y guardado en models/.", parent=self))
            except Exception as exc:
                self.after(0, lambda: self._log(f"✗ Error reentrenando: {exc}"))
                self.after(0, lambda: messagebox.showerror("Error", f"No se pudo reentrenar: {exc}", parent=self))

        threading.Thread(target=run, daemon=True).start()
    
    def _sync_manual_adjustments(self):
        """
        Sincroniza los ajustes manuales de Programas.xlsx con referentesUnificados.csv.
        
        Esto previene que falsos positivos corregidos manualmente entrenen el modelo.
        """
        import pandas as pd
        from etl.normalizacion import ARCHIVO_PROGRAMAS
        from etl.config import get_archivo_referentes
        from etl.exceptions_helpers import leer_excel_con_reintentos
        
        if not _ask_yes_no(
            "Sincronizar Ajustes Manuales",
            "Esta función sincronizará los ajustes manuales de Programas.xlsx con referentesUnificados.csv.\n\n"
            "Acciones que se realizarán:\n"
            "1. Programas marcados como ES_REFERENTE='No' con AJUSTE_MANUAL=True → cambiarán label=1 a label=0 en referentes\n"
            "2. Programas marcados como ES_REFERENTE='Sí' con AJUSTE_MANUAL=True → se agregarán como label=1 si no existen\n\n"
            "¿Deseas continuar?",
            parent=self
        ):
            return
        
        self._log("=== Sincronizando ajustes manuales ===")
        
        try:
            # 1. Leer Programas.xlsx y filtrar ajustes manuales
            self._log("Leyendo ajustes manuales desde Programas.xlsx...")
            df_programas = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name="Programas")
            
            # Normalizar código SNIES
            def norm_codigo(v):
                s = str(v).strip() if v is not None else ""
                return s[:-2] if s.endswith(".0") else s
            
            df_programas["_CODIGO_NORM"] = df_programas["CÓDIGO_SNIES_DEL_PROGRAMA"].apply(norm_codigo)
            
            # Filtrar solo ajustes manuales
            if "AJUSTE_MANUAL" not in df_programas.columns:
                self._log("⚠️ No se encontró columna AJUSTE_MANUAL. No hay ajustes para sincronizar.")
                messagebox.showinfo("Info", "No se encontraron ajustes manuales para sincronizar.", parent=self)
                return
            
            # Convertir AJUSTE_MANUAL a bool
            def _to_bool(v):
                if v is None:
                    return False
                if isinstance(v, bool):
                    return v
                s = str(v).strip().lower()
                return s in ("1", "true", "t", "yes", "y", "si", "sí")
            
            df_programas["AJUSTE_MANUAL"] = df_programas["AJUSTE_MANUAL"].apply(_to_bool)
            df_ajustes = df_programas[df_programas["AJUSTE_MANUAL"] == True].copy()
            
            if len(df_ajustes) == 0:
                self._log("No hay ajustes manuales para sincronizar.")
                messagebox.showinfo("Info", "No se encontraron ajustes manuales para sincronizar.", parent=self)
                return
            
            self._log(f"Encontrados {len(df_ajustes)} ajustes manuales")
            
            # 2. Leer referentesUnificados
            archivo_referentes = get_archivo_referentes()
            if not archivo_referentes.exists():
                safe_messagebox_error("Error", f"No se encontró el archivo de referentes: {archivo_referentes}", parent=self)
                return
            
            self._log(f"Leyendo referentes desde {archivo_referentes.name}...")
            df_referentes = self._leer(archivo_referentes)
            
            # Normalizar código en referentes
            if "CÓDIGO_SNIES_DEL_PROGRAMA" in df_referentes.columns:
                df_referentes["_CODIGO_NORM"] = df_referentes["CÓDIGO_SNIES_DEL_PROGRAMA"].apply(norm_codigo)
            else:
                df_referentes["_CODIGO_NORM"] = ""
            
            # Asegurar que existe columna label
            if "label" not in df_referentes.columns:
                df_referentes["label"] = 1
            
            # Normalizar label
            df_referentes["label"] = pd.to_numeric(df_referentes["label"], errors="coerce").fillna(0).astype(int)
            
            cambios_realizados = 0
            nuevos_referentes = 0
            
            # 3. Procesar cada ajuste manual
            for idx, row_ajuste in df_ajustes.iterrows():
                codigo = row_ajuste["_CODIGO_NORM"]
                es_referente = str(row_ajuste.get("ES_REFERENTE", "")).strip().upper() in ("SÍ", "SI", "YES", "1", "TRUE")
                
                if not codigo:
                    continue
                
                # Buscar en referentes por código SNIES
                mask_codigo = df_referentes["_CODIGO_NORM"] == codigo
                
                if mask_codigo.any():
                    # Existe en referentes
                    if es_referente:
                        # Marcar como referente (label=1)
                        if df_referentes.loc[mask_codigo, "label"].iloc[0] != 1:
                            df_referentes.loc[mask_codigo, "label"] = 1
                            cambios_realizados += 1
                            self._log(f"  ✓ {codigo}: Actualizado a label=1 (referente confirmado)")
                    else:
                        # Marcar como NO referente (label=0) - FALSO POSITIVO CORREGIDO
                        if df_referentes.loc[mask_codigo, "label"].iloc[0] != 0:
                            df_referentes.loc[mask_codigo, "label"] = 0
                            cambios_realizados += 1
                            self._log(f"  ✗ {codigo}: Cambiado a label=0 (falso positivo corregido)")
                else:
                    # No existe en referentes - solo agregar si es referente confirmado
                    if es_referente:
                        # Agregar nuevo referente
                        nuevo_referente = {
                            "CÓDIGO_SNIES_DEL_PROGRAMA": codigo,
                            "NOMBRE_DEL_PROGRAMA": str(row_ajuste.get("NOMBRE_DEL_PROGRAMA", "")),
                            "NombrePrograma EAFIT": str(row_ajuste.get("PROGRAMA_EAFIT_NOMBRE", "")),
                            "CAMPO_AMPLIO": str(row_ajuste.get("CINE_F_2013_AC_CAMPO_AMPLIO", "")),
                            "CAMPO_AMPLIO_EAFIT": "",
                            "NIVEL_DE_FORMACIÓN": str(row_ajuste.get("NIVEL_DE_FORMACIÓN", "")),
                            "NIVEL_DE_FORMACIÓN EAFIT": "",
                            "label": 1,
                            "_CODIGO_NORM": codigo
                        }
                        # Agregar otras columnas si existen en referentes
                        for col in df_referentes.columns:
                            if col not in nuevo_referente:
                                nuevo_referente[col] = ""
                        
                        df_referentes = pd.concat([df_referentes, pd.DataFrame([nuevo_referente])], ignore_index=True)
                        nuevos_referentes += 1
                        self._log(f"  + {codigo}: Agregado como nuevo referente (label=1)")
            
            # 4. Guardar referentes actualizados
            if cambios_realizados > 0 or nuevos_referentes > 0:
                # Eliminar columna temporal
                if "_CODIGO_NORM" in df_referentes.columns:
                    df_referentes = df_referentes.drop(columns=["_CODIGO_NORM"])
                
                # Backup antes de guardar
                try:
                    backup = archivo_referentes.parent / f"{archivo_referentes.stem}__backup_sync_{time.strftime('%Y%m%d_%H%M%S')}{archivo_referentes.suffix}"
                    shutil.copy2(archivo_referentes, backup)
                    self._log(f"Backup creado: {backup.name}")
                except Exception as e:
                    self._log(f"Advertencia: No se pudo crear backup: {e}")
                
                # Guardar
                if archivo_referentes.suffix.lower() == ".csv":
                    df_referentes.to_csv(archivo_referentes, index=False, encoding="utf-8")
                else:
                    with pd.ExcelWriter(archivo_referentes, mode="w", engine="openpyxl") as writer:
                        df_referentes.to_excel(writer, index=False)
                
                self._log(f"✓ Sincronización completada:")
                self._log(f"  - Referentes actualizados: {cambios_realizados}")
                self._log(f"  - Nuevos referentes agregados: {nuevos_referentes}")
                self._log(f"  - Total cambios: {cambios_realizados + nuevos_referentes}")
                
                messagebox.showinfo(
                    "Sincronización Completada",
                    f"Sincronización exitosa:\n\n"
                    f"Referentes actualizados: {cambios_realizados}\n"
                    f"Nuevos referentes agregados: {nuevos_referentes}\n\n"
                    f"Los falsos positivos corregidos ahora tienen label=0 y NO entrenarán el modelo.",
                    parent=self
                )
                
                # Recargar tabla
                self._load()
            else:
                self._log("No se realizaron cambios (los ajustes ya estaban sincronizados)")
                messagebox.showinfo("Info", "No se realizaron cambios. Los ajustes ya estaban sincronizados.", parent=self)
                
        except Exception as exc:
            error_msg = f"Error al sincronizar: {exc}"
            self._log(f"✗ {error_msg}")
            safe_messagebox_error("Error", error_msg, parent=self)

    def _on_resize(self, w: int, h: int) -> None:
        """Responsive: ajusta la altura de la tabla y wraplengths al espacio disponible."""
        table_pixels = max(120, h - 340)
        self.table.set_height_from_pixels(table_pixels)
        
        # Ajustar wraplength del subheader dinámicamente
        if hasattr(self, 'subheader_label'):
            wraplength = max(400, w - 100)
            self.subheader_label.config(wraplength=wraplength)


class MergePage(ttk.Frame):
    """Consolidación (merge) de Programas.xlsx con un archivo histórico."""

    def __init__(self, parent: tk.Misc, on_back=None):
        super().__init__(parent)
        self.on_back = on_back
        
        # Import lazy de pandas y módulos ETL (solo cuando se abre esta página)
        import pandas as pd
        from etl.config import ARCHIVO_HISTORICO, OUTPUTS_DIR
        from etl.normalizacion import ARCHIVO_PROGRAMAS
        
        self.base_dir = ensure_base_dir(self)
        if not self.base_dir:
            if on_back:
                on_back()
            return

        self.default_current = ARCHIVO_PROGRAMAS
        self.default_hist = ARCHIVO_HISTORICO
        self.outputs_dir = OUTPUTS_DIR

        frame = ttk.Frame(self, padding=14, style="Page.TFrame")
        frame.pack(fill=tk.BOTH, expand=True)

        header_frame = ttk.Frame(frame, style="Page.TFrame")
        header_frame.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 4))
        ttk.Label(header_frame, text="🔀 Consolidar archivos (Merge)", style="Header.TLabel").pack(side=tk.LEFT)
        if on_back:
            ttk.Button(header_frame, text="← Volver al menú", command=lambda: on_back() if on_back else None, style="Back.TButton").pack(side=tk.RIGHT)
        self.subheader_label = ttk.Label(frame, text="Combina el archivo actual con un histórico. Los registros con ajuste manual tienen prioridad.", style="SubHeader.TLabel")
        self.subheader_label.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(4, 12))

        ttk.Label(frame, text="Archivo actual (Programas.xlsx):").grid(row=2, column=0, sticky="w")
        self.current_var = tk.StringVar(value=str(self.default_current))
        self.entry_current = ttk.Entry(frame, textvariable=self.current_var, width=80)
        self.entry_current.grid(row=2, column=1, sticky="ew")
        ttk.Button(frame, text="Buscar...", command=self._pick_current).grid(row=2, column=2, padx=6)

        ttk.Label(frame, text="Archivo histórico:").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.hist_var = tk.StringVar(value=str(self.default_hist))
        self.entry_hist = ttk.Entry(frame, textvariable=self.hist_var, width=80)
        self.entry_hist.grid(row=3, column=1, sticky="ew", pady=(8, 0))
        ttk.Button(frame, text="Buscar...", command=self._pick_hist).grid(row=3, column=2, padx=6, pady=(8, 0))

        ttk.Label(frame, text="Archivo de salida:").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.out_var = tk.StringVar(value=str(self.outputs_dir / "ProgramasConsolidado.xlsx"))
        self.entry_out = ttk.Entry(frame, textvariable=self.out_var, width=80)
        self.entry_out.grid(row=4, column=1, sticky="ew", pady=(8, 0))
        ttk.Button(frame, text="Elegir...", command=self._pick_out).grid(row=4, column=2, padx=6, pady=(8, 0))

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=5, column=0, columnspan=3, sticky="w", pady=(14, 0))
        ttk.Button(btn_row, text="Consolidar", command=self._merge, style="Primary.TButton").pack(side=tk.LEFT)
        ttk.Button(btn_row, text="Abrir salida", command=self._open_out, style="Secondary.TButton").pack(side=tk.LEFT, padx=8)

        self.msg = tk.Text(frame, height=8, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9), bg=EAFIT["card_bg"], fg=EAFIT["text"])
        self.msg.grid(row=6, column=0, columnspan=3, sticky="nsew", pady=(12, 0))

        frame.grid_columnconfigure(1, weight=1)
        frame.grid_rowconfigure(6, weight=1)

    def _on_resize(self, w: int, h: int) -> None:
        """Responsive: ajusta ancho en caracteres de los Entry y wraplengths para que no desborden en ventanas estrechas."""
        char_width = max(20, min(80, (w - 280) // 8))
        for entry in (self.entry_current, self.entry_hist, self.entry_out):
            try:
                entry.config(width=char_width)
            except (tk.TclError, AttributeError):
                pass
        
        # Ajustar wraplength del subheader dinámicamente
        if hasattr(self, 'subheader_label'):
            wraplength = max(400, w - 100)
            self.subheader_label.config(wraplength=wraplength)

    def _log(self, s: str):
        self.msg.config(state=tk.NORMAL)
        ts = time.strftime("%H:%M:%S")
        self.msg.insert(tk.END, f"[{ts}] {s}\n")
        self.msg.see(tk.END)
        self.msg.config(state=tk.DISABLED)

    def _pick_current(self):
        p = filedialog.askopenfilename(title="Seleccionar Programas.xlsx", filetypes=[("Excel", "*.xlsx")], parent=self)
        if p:
            self.current_var.set(p)

    def _pick_hist(self):
        p = filedialog.askopenfilename(title="Seleccionar archivo histórico", filetypes=[("Excel", "*.xlsx")], parent=self)
        if p:
            self.hist_var.set(p)

    def _pick_out(self):
        p = filedialog.asksaveasfilename(
            title="Guardar consolidado como...",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            parent=self,
        )
        if p:
            self.out_var.set(p)

    def _open_out(self):
        try:
            _open_in_excel(Path(self.out_var.get()))
        except Exception as exc:
            messagebox.showerror("Error", str(exc), parent=self)

    def _merge(self):
        import pandas as pd  # Lazy import
        
        current_path = Path(self.current_var.get())
        hist_path = Path(self.hist_var.get())
        out_path = Path(self.out_var.get())

        if not current_path.exists():
            messagebox.showerror("Error", f"No existe: {current_path}", parent=self)
            return
        if not hist_path.exists():
            messagebox.showerror("Error", f"No existe: {hist_path}", parent=self)
            return

        try:
            from etl.exceptions_helpers import leer_excel_con_reintentos
            df_current = leer_excel_con_reintentos(current_path, sheet_name="Programas")
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo leer el archivo actual: {exc}", parent=self)
            return
        try:
            # histórico de programas nuevos suele tener hoja ProgramasNuevos
            from etl.exceptions_helpers import leer_excel_con_reintentos
            try:
                df_hist = leer_excel_con_reintentos(hist_path, sheet_name="ProgramasNuevos")
            except Exception:
                df_hist = leer_excel_con_reintentos(hist_path)
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo leer el histórico: {exc}", parent=self)
            return

        key = "CÓDIGO_SNIES_DEL_PROGRAMA"
        if key not in df_current.columns or key not in df_hist.columns:
            messagebox.showerror("Error", f"Ambos archivos deben tener la columna '{key}'.", parent=self)
            return

        df_current = df_current.copy()
        df_hist = df_hist.copy()
        df_current["ORIGEN_REGISTRO"] = "ACTUAL"
        df_hist["ORIGEN_REGISTRO"] = "HISTORICO"

        # Unificar columnas (union)
        all_cols = list(dict.fromkeys(list(df_current.columns) + list(df_hist.columns)))
        for c in all_cols:
            if c not in df_current.columns:
                df_current[c] = None
            if c not in df_hist.columns:
                df_hist[c] = None

        # Merge con regla de negocio:
        # 1) Si hay AJUSTE_MANUAL=True, eso gana sobre automático.
        # 2) Si no hay ajuste manual, gana ACTUAL sobre HISTORICO.
        # 3) Si hay FECHA_AJUSTE, se prefiere la más reciente.
        combined = pd.concat([df_hist[all_cols], df_current[all_cols]], ignore_index=True)

        def norm_codigo(v: object) -> str:
            s = str(v).strip() if v is not None else ""
            return s[:-2] if s.endswith(".0") else s

        combined["_CODIGO_NORM"] = combined[key].apply(norm_codigo)

        if "AJUSTE_MANUAL" not in combined.columns:
            combined["AJUSTE_MANUAL"] = False
        # Normalizar AJUSTE_MANUAL de forma segura:
        # - Excel puede traer bool, 0/1, o strings ("Sí"/"No", "true"/"false").
        def _to_bool(v: object) -> bool:
            if v is None:
                return False
            if isinstance(v, bool):
                return v
            try:
                # np.bool_ / ints / floats
                if isinstance(v, (int, float)):
                    return bool(int(v))
            except Exception:
                pass
            s = str(v).strip().lower()
            if s in ("1", "true", "t", "yes", "y", "si", "sí"):
                return True
            if s in ("0", "false", "f", "no", "n", ""):
                return False
            # fallback conservador: si no se entiende, asumir False
            return False

        combined["AJUSTE_MANUAL"] = combined["AJUSTE_MANUAL"].apply(_to_bool)

        if "FECHA_AJUSTE" not in combined.columns:
            combined["FECHA_AJUSTE"] = ""
        # Parse simple: si no parsea, queda NaT y se ordena al final
        combined["_FECHA_AJUSTE_TS"] = pd.to_datetime(combined["FECHA_AJUSTE"], errors="coerce")

        # Prioridades: manual primero, luego ACTUAL, luego fecha ajuste desc
        combined["_PRIO_MANUAL"] = combined["AJUSTE_MANUAL"].astype(int)
        combined["_PRIO_ORIGEN"] = (combined["ORIGEN_REGISTRO"].astype(str) == "ACTUAL").astype(int)

        combined = combined.sort_values(
            by=["_CODIGO_NORM", "_PRIO_MANUAL", "_PRIO_ORIGEN", "_FECHA_AJUSTE_TS"],
            ascending=[True, False, False, False],
        )
        combined["FUENTE_CONSOLIDADO"] = combined.apply(
            lambda r: "MANUAL" if bool(r.get("AJUSTE_MANUAL")) else str(r.get("ORIGEN_REGISTRO") or ""),
            axis=1,
        )
        combined = combined.drop_duplicates(subset=["_CODIGO_NORM"], keep="first")
        combined = combined.drop(columns=["_CODIGO_NORM", "_FECHA_AJUSTE_TS", "_PRIO_MANUAL", "_PRIO_ORIGEN"])

        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with pd.ExcelWriter(out_path, mode="w", engine="openpyxl") as writer:
                combined.to_excel(writer, sheet_name="Consolidado", index=False)
        except PermissionError:
            safe_messagebox_error("Error", explain_file_in_use(), parent=self)
            return

        self._log(f"Consolidado generado: {out_path} ({len(combined)} filas)")
        messagebox.showinfo("OK", f"Consolidado generado:\n{out_path}", parent=self)


class MainMenuGUI:
    """Menú principal del sistema."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Clasificación de Programas SNIES - EAFIT")
        self.root.geometry("1200x720")
        self.root.minsize(900, 600)  # Tamaño mínimo más generoso para mejor visualización
        self.root.resizable(True, True)
        apply_modern_style(self.root)

        self.outer = ttk.Frame(root, padding=18, style="App.TFrame")
        self.outer.pack(fill=tk.BOTH, expand=True)
        
        # Contenedor para páginas (se muestra/oculta según la acción)
        self.content_container = ttk.Frame(self.outer, style="App.TFrame")
        # Inicialmente oculto, se mostrará cuando se abra una página
        # self.content_container.pack(fill=tk.BOTH, expand=True)  # No empaquetar inicialmente
        
        # Frame del menú principal (inicialmente visible)
        # Estructura: contenido scrollable + footer fijo siempre visible
        self.menu_frame = ttk.Frame(self.outer, style="App.TFrame")
        self.menu_frame.pack(fill=tk.BOTH, expand=True)
        
        # Contenedor para el contenido scrollable (sin el footer)
        self.menu_content_container = ttk.Frame(self.menu_frame, style="App.TFrame")
        self.menu_content_container.pack(fill=tk.BOTH, expand=True)
        
        # Canvas y scrollbar para contenido scrollable (dentro del contenedor, no en menu_frame directamente)
        self.menu_canvas = tk.Canvas(self.menu_content_container, highlightthickness=0, bg=EAFIT["bg"])
        self.menu_scrollbar = ttk.Scrollbar(self.menu_content_container, orient="vertical", command=self.menu_canvas.yview)
        
        # Frame contenedor para centrar el contenido y limitar ancho máximo
        self.menu_content_wrapper = ttk.Frame(self.menu_canvas, style="App.TFrame")
        
        # Frame para el contenido scrollable (todo excepto el footer)
        # Padding más generoso para mejor espaciado visual
        self.menu_content = ttk.Frame(self.menu_content_wrapper, padding=24, style="App.TFrame")
        self.menu_content.pack(fill=tk.BOTH, expand=True)
        
        # Configurar scroll y ajuste de ancho del canvas
        def _configure_canvas(event=None):
            # Ajustar ancho del contenido al canvas completo
            canvas_width = self.menu_canvas.winfo_width()
            if canvas_width > 1:
                # Usar TODO el ancho disponible del canvas (sin restricciones)
                # El scrollbar se manejará automáticamente
                self.menu_canvas.itemconfig(self.menu_content_window, width=canvas_width)
            # Actualizar scrollregion y visibilidad del scrollbar
            # El scrollregion solo debe considerar el contenido, no el footer
            self.menu_canvas.configure(scrollregion=self.menu_canvas.bbox("all"))
            _update_scrollbar_visibility()
            # Forzar actualización responsive después de cambiar el ancho
            self.root.after_idle(self._update_responsive)
        
        # Función para mostrar/ocultar scrollbar según necesidad
        def _update_scrollbar_visibility():
            try:
                bbox = self.menu_canvas.bbox("all")
                if bbox:
                    canvas_height = self.menu_canvas.winfo_height()
                    content_height = bbox[3] - bbox[1]
                    if content_height > canvas_height and canvas_height > 0:
                        # Mostrar scrollbar solo si el contenido es más alto que el canvas
                        if not self.menu_scrollbar.winfo_ismapped():
                            self.menu_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
                    else:
                        # Ocultar scrollbar si no es necesario
                        if self.menu_scrollbar.winfo_ismapped():
                            self.menu_scrollbar.pack_forget()
            except Exception:
                pass
        
        self.menu_content_wrapper.bind("<Configure>", _configure_canvas)
        
        self.menu_content_window = self.menu_canvas.create_window((0, 0), window=self.menu_content_wrapper, anchor="nw")
        self.menu_canvas.configure(yscrollcommand=self.menu_scrollbar.set)
        
        # Bind para ajustar ancho cuando cambia el tamaño del canvas
        self.menu_canvas.bind("<Configure>", _configure_canvas)
        # También actualizar cuando cambie el tamaño de la ventana principal
        self.root.bind("<Configure>", lambda e: self.root.after_idle(_configure_canvas) if e.widget == self.root else None)
        
        # Empacar canvas y scrollbar dentro del contenedor de contenido
        # El scrollbar se mostrará/ocultará automáticamente según necesidad en _configure_canvas
        self.menu_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # El scrollbar se empaqueta inicialmente pero se ocultará si no es necesario
        self.menu_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Ejecutar configuración inicial después de que todo esté empaquetado
        self.root.after_idle(_configure_canvas)
        
        # Bind mousewheel para scroll
        def _on_mousewheel(event):
            self.menu_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        
        self.menu_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Páginas actuales (se crean bajo demanda)
        self.current_page: ttk.Frame | None = None
        self.pages: dict[str, ttk.Frame] = {}

        # Header mejorado con mejor diseño
        header = ttk.Frame(self.menu_content, style="App.TFrame")
        header.pack(fill=tk.X, pady=(0, 24))
        
        # Título principal con icono
        title_frame = ttk.Frame(header, style="App.TFrame")
        title_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(
            title_frame, 
            text="📊 Clasificación de Programas SNIES", 
            style="Header.TLabel"
        ).pack(anchor="w")
        
        # Subtítulo mejorado (wraplength dinámico se ajustará en _update_responsive)
        self.subtitle_label = ttk.Label(
            header,
            text="Descarga, normaliza, clasifica programas académicos y gestiona referentes EAFIT.",
            style="SubHeader.TLabel",
            wraplength=800,
            justify="left",
        )
        self.subtitle_label.pack(anchor="w", pady=(4, 0))
        
        # Separador elegante
        separator = ttk.Frame(header, style="Separator.TFrame", height=1)
        separator.pack(fill=tk.X, pady=(16, 0))
        separator.pack_propagate(False)

        # Layout de dos columnas que se expanden para llenar todo el espacio disponible
        main_content = ttk.Frame(self.menu_content, style="App.TFrame")
        main_content.pack(fill=tk.BOTH, expand=True)
        
        # Columna izquierda: Acciones principales y configuración (50% del espacio)
        left_column = ttk.Frame(main_content, style="App.TFrame")
        left_column.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 16))
        
        # Columna derecha: Estado del sistema y utilidades (50% del espacio)
        right_column = ttk.Frame(main_content, style="App.TFrame")
        right_column.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(16, 0))

        # === COLUMNA IZQUIERDA ===
        
        # Card: Acción principal (más destacada y limpia)
        primary_action_card = ttk.Frame(left_column, padding=28, style="Card.TFrame")
        primary_action_card.pack(fill=tk.X, pady=(0, 24))
        
        # Botón principal más grande y destacado
        primary_btn = ttk.Button(
            primary_action_card,
            text="▶️ Ejecutar análisis SNIES (Pipeline)",
            command=self._open_pipeline,
            style="Primary.TButton",
        )
        primary_btn.pack(fill=tk.X, pady=(0, 12))
        
        # Descripción con wraplength dinámico (se actualizará en _update_responsive)
        self.primary_desc_label = ttk.Label(
            primary_action_card,
            text="Descarga desde SNIES, normaliza y clasifica programas académicos.",
            style="Muted.TLabel",
            wraplength=400,  # Valor inicial, se actualizará dinámicamente
            justify="left",
        )
        self.primary_desc_label.pack(anchor="w", fill=tk.X)
        
        # Card: Otras acciones (más compactas y limpias)
        other_actions_card = ttk.Frame(left_column, padding=28, style="Card.TFrame")
        other_actions_card.pack(fill=tk.X, pady=(0, 24))
        
        other_actions_title = ttk.Label(
            other_actions_card, 
            text="📋 Otras Acciones", 
            style="SectionTitle.TLabel"
        )
        other_actions_title.pack(anchor="w", pady=(0, 16))
        
        self._action_desc_labels = []  # Para actualización responsive
        
        def compact_action_row(title: str, desc: str, cmd, icon: str = ""):
            row = ttk.Frame(other_actions_card, style="Card.TFrame")
            row.pack(fill=tk.X, pady=(0, 14))
            btn_text = f"{icon} {title}" if icon else title
            ttk.Button(row, text=btn_text, command=cmd, style="Secondary.TButton").pack(fill=tk.X)
            # Descripción más pequeña y discreta con wraplength dinámico
            desc_label = ttk.Label(
                row, 
                text=desc, 
                style="Light.TLabel", 
                wraplength=400,  # Valor inicial, se actualizará dinámicamente
                justify="left",
                font=("Segoe UI", 8)
            )
            desc_label.pack(anchor="w", pady=(4, 0), fill=tk.X)
            self._action_desc_labels.append(desc_label)
        
        compact_action_row(
            "Ajuste manual de emparejamientos",
            "Revisa y corrige ES_REFERENTE y programa EAFIT.",
            self._open_manual,
            icon="✏️",
        )
        compact_action_row(
            "Reentrenamiento del modelo",
            "Edita referentes y reentrena el modelo.",
            self._open_retrain,
            icon="🎯",
        )
        compact_action_row(
            "Consolidar archivos (Merge)",
            "Combina Programas.xlsx con un histórico.",
            self._open_merge,
            icon="🔀",
        )
        
        # Card: Configuración (más compacta y limpia)
        config_card = ttk.Frame(left_column, padding=28, style="Card.TFrame")
        config_card.pack(fill=tk.X)
        
        ttk.Label(
            config_card, 
            text="⚙️ Configuración", 
            style="SectionTitle.TLabel"
        ).pack(anchor="w", pady=(0, 14))

        self.base_dir: Path | None = None
        self.base_label = ttk.Label(
            config_card,
            text="📁 Carpeta del proyecto: (no configurado)",
            style="Muted.TLabel",
            wraplength=400,  # Valor inicial, se actualizará dinámicamente
            justify="left",
            font=("Segoe UI", 9)
        )
        self.base_label.pack(anchor="w", pady=(0, 14), fill=tk.X)

        ttk.Button(
            config_card,
            text="📂 Cambiar carpeta del proyecto",
            command=self._configure,
            style="Small.TButton",
        ).pack(fill=tk.X)

        # === COLUMNA DERECHA ===
        
        # Card: Estado del Sistema (más compacto y limpio)
        health_card = ttk.Frame(right_column, padding=28, style="Card.TFrame")
        health_card.pack(fill=tk.X, pady=(0, 24))
        
        health_title = ttk.Label(
            health_card, 
            text="💚 Estado del Sistema", 
            style="SectionTitle.TLabel"
        )
        health_title.pack(anchor="w", pady=(0, 14))
        
        # Frame para estados (con mejor formato)
        self.health_frame = ttk.Frame(health_card, style="Card.TFrame")
        self.health_frame.pack(fill=tk.X, pady=(0, 14))
        
        self.health_status_labels = {}
        
        # Botones más compactos en una sola fila con mejor espaciado
        health_btn_frame = ttk.Frame(health_card, style="Card.TFrame")
        health_btn_frame.pack(fill=tk.X)
        
        ttk.Button(
            health_btn_frame,
            text="🔍 Verificar",
            command=self._run_health_check,
            style="Small.TButton",
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
        ttk.Button(
            health_btn_frame,
            text="🔧 Reparar",
            command=self._repair_system,
            style="Small.TButton",
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Ejecutar health check automáticamente al abrir
        self.root.after_idle(lambda: self.root.after(1500, self._run_health_check))
        
        # Card: Utilidades (más compacta y organizada)
        util_card = ttk.Frame(right_column, padding=28, style="Card.TFrame")
        util_card.pack(fill=tk.X, pady=(0, 24))
        
        util_title = ttk.Label(
            util_card, 
            text="🛠️ Utilidades", 
            style="SectionTitle.TLabel"
        )
        util_title.pack(anchor="w", pady=(0, 14))
        
        self.util_btns = ttk.Frame(util_card, style="Card.TFrame")
        self.util_btns.pack(fill=tk.X)
        
        # Botones con textos completos y mejor organización
        util_buttons_data = [
            ("📋 Ver logs", self._open_logs),
            ("🔓 Desbloquear", self._unlock_if_needed),
            ("📂 Outputs", self._open_outputs),
            ("📊 Programas.xlsx", self._open_programas),
            ("🧹 Limpiar", self._limpiar_historicos),
        ]
        
        self._util_buttons = []
        for text, cmd in util_buttons_data:
            btn = ttk.Button(self.util_btns, text=text, command=cmd, style="Small.TButton")
            self._util_buttons.append(btn)
        
        # Organizar botones en grid de 2 columnas con mejor espaciado
        for i, btn in enumerate(self._util_buttons):
            row = i // 2
            col = i % 2
            padx_right = 8 if col == 0 else 0
            pady_bottom = 8 if row < (len(self._util_buttons) - 1) // 2 else 0
            btn.grid(row=row, column=col, sticky="ew", padx=(0, padx_right), pady=(0, pady_bottom))
        
        # Configurar columnas para distribución uniforme
        self.util_btns.columnconfigure(0, weight=1, uniform="util_col")
        self.util_btns.columnconfigure(1, weight=1, uniform="util_col")
        
        # Card: Flujo recomendado (más compacto y visual)
        flow_card = ttk.Frame(right_column, padding=28, style="Card.TFrame")
        flow_card.pack(fill=tk.X)
        
        flow_title = ttk.Label(
            flow_card, 
            text="🔄 Flujo recomendado", 
            style="SectionTitle.TLabel"
        )
        flow_title.pack(anchor="w", pady=(0, 12))
        
        # Flujo más visual y compacto con mejor diseño
        flow_steps = ttk.Frame(flow_card, style="Card.TFrame")
        flow_steps.pack(fill=tk.X)
        
        steps_data = [
            ("1", "Pipeline"),
            ("2", "Ajuste"),
            ("3", "Reentrenar"),
            ("4", "Consolidar")
        ]
        
        for i, (num, text) in enumerate(steps_data):
            step_frame = ttk.Frame(flow_steps, style="Card.TFrame")
            step_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
            step_label = ttk.Label(
                step_frame,
                text=f"{num}. {text}",
                style="Muted.TLabel",
                font=("Segoe UI", 9),
                anchor="center"
            )
            step_label.pack(fill=tk.X)
            
            # Agregar flecha entre pasos (excepto después del último)
            if i < len(steps_data) - 1:
                arrow_label = ttk.Label(
                    flow_steps,
                    text="→",
                    style="Muted.TLabel",
                    font=("Segoe UI", 11),
                    foreground=EAFIT["text_muted"]
                )
                arrow_label.pack(side=tk.LEFT, padx=4)
        
        self.flow_label = None  # Ya no se usa el label individual

        # Footer/status mejorado - siempre visible en la parte inferior, FUERA del área scrollable
        # El footer se crea DESPUÉS de todo el contenido para asegurar que quede fijo en la parte inferior
        # y no afecte el área scrollable del contenido central
        # Separador visual antes del footer
        footer_separator = ttk.Frame(self.menu_frame, style="Separator.TFrame", height=1)
        footer_separator.pack_propagate(False)  # Mantener altura fija
        footer_separator.pack(fill=tk.X, side=tk.BOTTOM, pady=(0, 0))
        
        # Footer fijo (NO expand, solo fill horizontal) - siempre en la parte inferior
        footer = ttk.Frame(self.menu_frame, style="App.TFrame", padding=(18, 12))
        footer.pack(fill=tk.X, side=tk.BOTTOM)  # Siempre en la parte inferior, sin expand
        
        # Estado mejorado (izquierda)
        status_frame = ttk.Frame(footer, style="App.TFrame")
        status_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.status_label = ttk.Label(
            status_frame, 
            text="✅ Estado: listo", 
            style="Status.TLabel"
        )
        self.status_label.pack(side=tk.LEFT)
        
        # Botón salir mejorado (derecha, siempre visible)
        exit_btn = ttk.Button(
            footer, 
            text="🚪 Salir", 
            command=self.root.destroy, 
            style="Secondary.TButton"
        )
        exit_btn.pack(side=tk.RIGHT, padx=(12, 0))

        # Responsive: reajuste al redimensionar (wraplength y contenido)
        self._last_width = 0
        self._last_height = 0
        self.root.bind("<Configure>", self._on_configure_resize)
        self.outer.bind("<Configure>", self._on_configure_resize)
        
        # Asegurar que el footer siempre sea visible
        self.root.update_idletasks()
        self._ensure_footer_visible()

        # Cargar configuración y forzar un reajuste inicial cuando la ventana esté dibujada
        self.root.after(100, self._refresh_base_dir)
        self.root.after(350, self._update_responsive)
        self.root.after(500, self._ensure_footer_visible)
    
    def _ensure_footer_visible(self):
        """Asegura que el footer siempre sea visible en la parte inferior."""
        try:
            # Forzar actualización del layout
            self.root.update_idletasks()
            # El footer ya está correctamente posicionado con side=tk.BOTTOM
            # Solo necesitamos asegurar que el contenedor de contenido respete su espacio
        except Exception:
            pass

    def _on_configure_resize(self, event):
        """Cuando se redimensiona la ventana, reajusta wraplength y notifica a las páginas."""
        # Aceptar evento del root o del frame principal
        if event.widget not in (self.root, self.outer):
            return
        self.root.after_idle(self._update_responsive)

    def _update_responsive(self):
        """Actualiza wraplength de labels, botones de utilidades y tablas según el tamaño actual de la ventana."""
        try:
            w_root = self.root.winfo_width()
            h = self.root.winfo_height()
            w_outer = self.outer.winfo_width() if self.outer.winfo_exists() else 0
            # Usar el ancho del área de contenido (outer); si no está mapeado aún, usar root
            w = max(w_root, w_outer) if w_outer > 50 else w_root
            w = max(200, w)
            
            # Calcular ancho disponible para el contenido (usando el canvas real)
            try:
                canvas_width = self.menu_canvas.winfo_width()
                if canvas_width < 10:  # Si el canvas aún no está inicializado
                    canvas_width = w - 40  # Aproximación
            except (tk.TclError, AttributeError):
                canvas_width = w - 40
            
            # Calcular ancho del contenido (considerando padding del menu_content: 24px cada lado = 48px)
            # Y padding entre columnas: 16px cada lado = 32px
            # Y padding interno de cards: ~28px cada lado = 56px por card
            content_width = max(600, canvas_width - 48)  # Padding del menu_content
            wraplen = max(400, content_width - 100)  # Para el subtítulo completo
            
            # Actualizar wraplength de labels específicos
            if abs(w - getattr(self, "_last_width", 0)) >= 5:  # Cambiar más frecuentemente
                self._last_width = w
                
                # Calcular wraplength para columnas (considerando padding entre columnas: 16px cada lado = 32px)
                # Y padding interno de cards: ~28px cada lado = 56px
                column_width = (content_width - 32) // 2  # Dividir entre dos columnas menos padding entre ellas
                # Restar menos padding para que las columnas no sean demasiado estrechas
                wraplen_column = max(250, column_width - 80)  # Menos padding restado para mejor uso del espacio
                
                # Actualizar labels específicos con wraplength dinámico
                if hasattr(self, 'subtitle_label'):
                    try:
                        self.subtitle_label.configure(wraplength=wraplen)
                    except (tk.TclError, AttributeError):
                        pass
                
                # Actualizar descripción de acción principal
                if hasattr(self, 'primary_desc_label'):
                    try:
                        self.primary_desc_label.configure(wraplength=wraplen_column)
                    except (tk.TclError, AttributeError):
                        pass
                
                if hasattr(self, 'base_label'):
                    try:
                        self.base_label.configure(wraplength=wraplen_column)
                    except (tk.TclError, AttributeError):
                        pass
                
                # Actualizar descripciones de acciones (en columnas)
                if hasattr(self, '_action_desc_labels'):
                    for label in self._action_desc_labels:
                        try:
                            label.configure(wraplength=wraplen_column)
                        except (tk.TclError, AttributeError):
                            pass
                
                # Actualizar labels de estado del sistema si existen
                if hasattr(self, 'health_status_labels'):
                    for check_name, (label, ok) in self.health_status_labels.items():
                        try:
                            label.configure(wraplength=wraplen_column)
                        except (tk.TclError, AttributeError):
                            pass
                
                # Los botones de utilidades ya están en grid de 2 columnas, no necesitan reorganización
            
            # Asegurar que el footer sea visible
            self._ensure_footer_visible()
            
            # Notificar a la página actual para que actualice tablas (altura responsive)
            if self.current_page and hasattr(self.current_page, "_on_resize"):
                if abs(h - getattr(self, "_last_height", 0)) >= 20:
                    self._last_height = h
                    self.current_page._on_resize(w, h)
        except (tk.TclError, AttributeError):
            pass

    def _relayout_util_buttons(self, w: int):
        """Reorganiza los botones de Utilidades en una o múltiples filas según el ancho."""
        if not hasattr(self, "_util_buttons") or not self._util_buttons:
            return
        
        for i, btn in enumerate(self._util_buttons):
            try:
                btn.grid_forget()
            except tk.TclError:
                pass
        
        # Calcular cuántos botones caben en una fila según el ancho disponible
        # Cada botón necesita aproximadamente 140-180px de ancho (dependiendo del texto)
        # Usar un cálculo más conservador para evitar cortes
        estimated_button_width = 160  # Ancho estimado por botón
        buttons_per_row = max(2, min(5, int((w - 40) / estimated_button_width)))  # -40 para padding
        
        # Si hay más botones de los que caben, usar múltiples filas
        if len(self._util_buttons) > buttons_per_row:
            # Usar múltiples filas
            for i, btn in enumerate(self._util_buttons):
                row = i // buttons_per_row
                col = i % buttons_per_row
                padx_left = 0 if col == 0 else 8
                padx_right = 0
                pady_top = 0 if row == 0 else 6
                btn.grid(
                    row=row, 
                    column=col, 
                    sticky="w", 
                    padx=(padx_left, padx_right), 
                    pady=(pady_top, 0)
                )
        else:
            # Una sola fila - distribuir uniformemente
            for i, btn in enumerate(self._util_buttons):
                padx_left = 0 if i == 0 else 8
                btn.grid(
                    row=0, 
                    column=i, 
                    sticky="w", 
                    padx=(padx_left, 0)
                )
        
        # Actualizar el frame de botones para que se ajuste
        try:
            self.util_btns.update_idletasks()
        except:
            pass

    def _all_children(self, parent):
        """Generador recursivo de todos los hijos de un widget."""
        try:
            for c in parent.winfo_children():
                yield c
                yield from self._all_children(c)
        except (tk.TclError, AttributeError):
            pass

    def _refresh_base_dir(self):
        # No pedir carpeta al abrir el menú; solo mostrar estado.
        # Operaciones ligeras primero (sin I/O pesado)
        bd = get_configured_base_dir()
        if bd:
            self.base_dir = bd
            self.base_label.config(text=f"Carpeta del proyecto: {bd}")
            if hasattr(self, "status_label"):
                self.status_label.config(text="Estado: listo")
        else:
            self.base_dir = None
            self.base_label.config(text="Carpeta del proyecto: (no configurado)")
            if hasattr(self, "status_label"):
                self.status_label.config(text="Estado: requiere configuración")

        # Operaciones I/O más pesadas se hacen después (async)
        # Esto evita bloquear el inicio de la aplicación
        self.root.after(200, self._refresh_status_async)
    
    def _refresh_status_async(self):
        """Actualiza el estado de forma asíncrona (no bloquea el inicio)."""
        try:
            # Indicar lock (pipeline corriendo o lock huérfano)
            lock_file = get_pipeline_lock_file()
            age = get_lock_age_seconds(lock_file)
            if age is not None and hasattr(self, "status_label"):
                if age > LOCK_STALE_SECONDS:
                    self.status_label.config(text="Estado: lock detectado (posible cierre inesperado)")
                else:
                    self.status_label.config(text="Estado: pipeline en ejecución")
            else:
                # Si hay Programas.xlsx, mostrar fecha de actualización
                try:
                    from etl.normalizacion import ARCHIVO_PROGRAMAS  # Lazy import
                    p = ARCHIVO_PROGRAMAS
                    if p.exists() and hasattr(self, "status_label"):
                        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.stat().st_mtime))
                        self.status_label.config(text=f"Estado: listo (Programas.xlsx actualizado: {ts})")
                except Exception:
                    pass
        except Exception:
            # Si falla, no bloquear la aplicación
            pass

    def _configure(self):
        # Forzar selección de carpeta
        selected_dir = filedialog.askdirectory(title="Seleccionar carpeta raíz del proyecto", initialdir=str(Path.home()), parent=self.root)
        if not selected_dir:
            return
        p = Path(selected_dir)
        if not set_base_dir(p):
            messagebox.showerror("Error", "No se pudo guardar la configuración.", parent=self.root)
            return
        try:
            update_paths_for_base_dir(p)
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo aplicar la configuración:\n\n{exc}", parent=self.root)
            return
        self.base_dir = p
        self.base_label.config(text=f"Carpeta del proyecto: {p}")
        if hasattr(self, "status_label"):
            self.status_label.config(text="Estado: listo")
        # Re-ejecutar health check después de configurar
        self.root.after(500, self._run_health_check)
    
    def _run_health_check(self):
        """Ejecuta diagnóstico del sistema y muestra resultados."""
        # Limpiar frame de health
        for widget in self.health_frame.winfo_children():
            widget.destroy()
        
        checks = {}
        
        # 1. Conexión a Internet
        try:
            import urllib.request
            urllib.request.urlopen("https://www.google.com", timeout=5)
            checks["internet"] = (True, "Conexión a Internet: OK")
        except Exception:
            checks["internet"] = (False, "Conexión a Internet: ❌ No disponible (necesario para descarga SNIES)")
        
        # 2. Archivos base (ref/)
        try:
            from etl.config import REF_DIR, get_archivo_referentes, get_archivo_catalogo_eafit
            ref_dir_ok = REF_DIR.exists() and REF_DIR.is_dir()
            referentes_ok = get_archivo_referentes().exists()
            catalogo_ok = get_archivo_catalogo_eafit().exists()
            
            if ref_dir_ok and referentes_ok and catalogo_ok:
                checks["archivos_ref"] = (True, "Archivos de referencia (ref/): OK")
            else:
                msg = "Archivos de referencia (ref/): ❌ Faltan archivos"
                if not referentes_ok:
                    msg += " (referentesUnificados)"
                if not catalogo_ok:
                    msg += " (catalogoOfertasEAFIT)"
                checks["archivos_ref"] = (False, msg)
        except Exception as e:
            checks["archivos_ref"] = (False, f"Archivos de referencia: ❌ Error: {e}")
        
        # 3. Modelos ML
        try:
            from etl.clasificacionProgramas import MODELO_CLASIFICADOR, MODELO_EMBEDDINGS_OBJ, ENCODER_PROGRAMAS_EAFIT
            modelos_ok = all([
                MODELO_CLASIFICADOR.exists(),
                MODELO_EMBEDDINGS_OBJ.exists(),
                ENCODER_PROGRAMAS_EAFIT.exists()
            ])
            if modelos_ok:
                # Intentar cargar para verificar integridad
                try:
                    from etl.clasificacionProgramas import cargar_modelos
                    cargar_modelos()
                    checks["modelos"] = (True, "Modelos ML: OK (cargados correctamente)")
                except Exception as e:
                    checks["modelos"] = (False, f"Modelos ML: ⚠️ Existen pero están corruptos: {e}")
            else:
                checks["modelos"] = (False, "Modelos ML: ❌ No encontrados (ejecuta reentrenamiento)")
        except Exception as e:
            checks["modelos"] = (False, f"Modelos ML: ❌ Error: {e}")
        
        # 4. Permisos de escritura en outputs/
        try:
            from etl.config import OUTPUTS_DIR
            test_file = OUTPUTS_DIR / ".test_write"
            try:
                test_file.write_text("test")
                test_file.unlink()
                checks["permisos"] = (True, "Permisos de escritura (outputs/): OK")
            except PermissionError:
                checks["permisos"] = (False, "Permisos de escritura (outputs/): ❌ Sin permisos de escritura")
            except Exception as e:
                checks["permisos"] = (False, f"Permisos de escritura: ❌ Error: {e}")
        except Exception as e:
            checks["permisos"] = (False, f"Permisos de escritura: ❌ Error: {e}")
        
        # Mostrar resultados de forma más limpia y compacta
        # Obtener ancho disponible para wraplength dinámico
        try:
            canvas_width = self.menu_canvas.winfo_width() if hasattr(self, 'menu_canvas') else 800
            content_width = max(600, canvas_width - 48)
            column_width = (content_width - 32) // 2
            health_wraplength = max(200, column_width - 80)
        except (tk.TclError, AttributeError):
            health_wraplength = 300  # Fallback
        
        for check_name, (ok, msg) in checks.items():
            # Simplificar mensajes para que sean más cortos
            if ok:
                # Mensajes OK más cortos
                if "Internet" in msg:
                    display_msg = "🌐 Internet: OK"
                elif "referencia" in msg.lower():
                    display_msg = "📁 Archivos ref/: OK"
                elif "Modelos" in msg:
                    display_msg = "🤖 Modelos ML: OK"
                elif "Permisos" in msg:
                    display_msg = "✍️ Permisos: OK"
                else:
                    display_msg = msg.split(":")[0] + ": OK" if ":" in msg else msg
            else:
                # Mensajes de error más cortos
                if "Internet" in msg:
                    display_msg = "🌐 Internet: ❌"
                elif "referencia" in msg.lower():
                    display_msg = "📁 Archivos ref/: ❌"
                elif "Modelos" in msg:
                    display_msg = "🤖 Modelos ML: ❌"
                elif "Permisos" in msg:
                    display_msg = "✍️ Permisos: ❌"
                else:
                    display_msg = msg.split(":")[0] + ": ❌" if ":" in msg else msg
            
            color = EAFIT["success"] if ok else EAFIT["danger"]
            label = ttk.Label(
                self.health_frame,
                text=display_msg,
                foreground=color,
                style="Muted.TLabel",
                font=("Segoe UI", 9),
                wraplength=health_wraplength,  # Agregar wraplength dinámico
                justify="left"
            )
            label.pack(anchor="w", pady=3, fill=tk.X)
            self.health_status_labels[check_name] = (label, ok)
    
    def _repair_system(self):
        """Intenta reparar problemas detectados en el health check."""
        problemas = []
        soluciones = []
        
        # Verificar cada check
        for check_name, (label_widget, ok) in self.health_status_labels.items():
            if not ok:
                problemas.append(check_name)
        
        if not problemas:
            messagebox.showinfo("Sistema OK", "No se detectaron problemas que requieran reparación.", parent=self.root)
            return
        
        # Intentar reparar cada problema
        if "archivos_ref" in problemas:
            soluciones.append("Verifica que los archivos en ref/ existan y tengan el formato correcto.")
        
        if "modelos" in problemas:
            respuesta = messagebox.askyesno(
                "Reparar Modelos",
                "Los modelos ML no están disponibles o están corruptos.\n\n"
                "¿Deseas ir a la página de reentrenamiento para entrenar nuevos modelos?",
                parent=self.root
            )
            if respuesta:
                self._open_retrain()
                return
        
        if "permisos" in problemas:
            soluciones.append("Verifica que tengas permisos de escritura en la carpeta outputs/.")
            soluciones.append("Cierra Excel/Power BI si tienen archivos abiertos en outputs/.")
        
        if "internet" in problemas:
            soluciones.append("Verifica tu conexión a Internet.")
            soluciones.append("El sistema necesita Internet para descargar datos desde SNIES.")
        
        mensaje = "Problemas detectados:\n\n"
        mensaje += "\n".join(f"• {p}" for p in problemas)
        mensaje += "\n\nSoluciones sugeridas:\n\n"
        mensaje += "\n".join(f"• {s}" for s in soluciones)
        
        messagebox.showinfo("Reparación del Sistema", mensaje, parent=self.root)

    def _show_page(self, page_name: str, page_class, *args, **kwargs):
        """Muestra una página específica, ocultando el menú y otras páginas."""
        # Ocultar menú
        self.menu_frame.pack_forget()
        
        # Ocultar página actual si existe
        if self.current_page:
            self.current_page.pack_forget()
        
        # Mostrar content_container (si estaba oculto)
        self.content_container.pack(fill=tk.BOTH, expand=True)
        
        # Crear o reutilizar página
        if page_name not in self.pages:
            self.pages[page_name] = page_class(self.content_container, on_back=self._show_menu, *args, **kwargs)
        
        self.current_page = self.pages[page_name]
        self.current_page.pack(fill=tk.BOTH, expand=True)
        
        # Mínimos por página (la ventana conserva el tamaño actual; el usuario puede maximizar o reducir)
        if page_name == "pipeline":
            self.root.minsize(700, 500)
        elif page_name in ("manual", "retrain"):
            self.root.minsize(900, 600)
        elif page_name == "merge":
            self.root.minsize(700, 450)
        
        self.root.update_idletasks()
        
        # Llamar _on_resize después de mostrar la página para ajustar elementos responsive
        if hasattr(self.current_page, "_on_resize"):
            try:
                w = self.root.winfo_width()
                h = self.root.winfo_height()
                self.current_page._on_resize(w, h)
            except (tk.TclError, AttributeError):
                pass
        # Forzar un reajuste responsive al cambiar de página (tabla y wraplength)
        self.root.after(50, self._update_responsive)
    
    def _show_menu(self):
        """Vuelve al menú principal, ocultando la página actual."""
        # Ocultar página actual si existe
        if self.current_page:
            self.current_page.pack_forget()
        self.current_page = None
        
        # Ocultar content_container
        self.content_container.pack_forget()
        
        # Mostrar menú
        self.menu_frame.pack(fill=tk.BOTH, expand=True)
        
        self.root.minsize(800, 500)
        self._refresh_base_dir()
        self.root.update_idletasks()

    def _open_pipeline(self):
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        self._show_page("pipeline", PipelinePage)

    def _open_manual(self):
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        self._show_page("manual", ManualReviewPage)

    def _open_retrain(self):
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        self._show_page("retrain", RetrainPage)

    def _open_merge(self):
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        self._show_page("merge", MergePage)

    def _open_logs(self):
        base = get_configured_base_dir()
        if not base:
            safe_messagebox_error("Error", "Configura primero la carpeta del proyecto.", parent=self.root)
            return
        log_path = base / "logs" / "pipeline.log"
        if not log_path.exists():
            safe_messagebox_error("Atención", f"No existe el log aún:\n{log_path}", parent=self.root)
            return
        try:
            _open_text_file(log_path)
        except Exception:
            # fallback: abrir con app por defecto
            try:
                os.startfile(str(log_path))  # type: ignore[attr-defined]
            except Exception as exc:
                safe_messagebox_error("Error", str(exc), parent=self.root)

    def _unlock_if_needed(self):
        lock_file = get_pipeline_lock_file()
        if not lock_file.exists():
            messagebox.showinfo("OK", "No hay lock activo.", parent=self.root)
            return
        age = get_lock_age_seconds(lock_file)
        if age is None:
            age = 0
        if age < LOCK_STALE_SECONDS:
            messagebox.showwarning(
                "Atención",
                "Parece que el pipeline está en ejecución.\n\n"
                "Solo desbloquea si estás seguro de que NO hay un proceso corriendo.",
                parent=self.root,
            )
        if _ask_yes_no("Confirmar", "¿Eliminar el lock para desbloquear la edición?"):
            try:
                lock_file.unlink()
                messagebox.showinfo("OK", "Lock eliminado.", parent=self.root)
                self._refresh_base_dir()
            except Exception as exc:
                safe_messagebox_error("Error", f"No se pudo eliminar el lock: {exc}", parent=self.root)

    def _open_outputs(self):
        base = get_configured_base_dir()
        if not base:
            safe_messagebox_error("Error", "Configura primero la carpeta del proyecto.", parent=self.root)
            return
        out_dir = base / "outputs"
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            _open_default_app(out_dir)
        except Exception as exc:
            safe_messagebox_error("Error", str(exc), parent=self.root)

    def _open_programas(self):
        from etl.normalizacion import ARCHIVO_PROGRAMAS  # Lazy import
        
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        p = ARCHIVO_PROGRAMAS
        if not p.exists():
            safe_messagebox_error("Atención", "Aún no existe outputs/Programas.xlsx. Ejecuta el análisis SNIES.", parent=self.root)
            return
        try:
            _open_in_excel(p)
        except Exception as exc:
            safe_messagebox_error("Error", str(exc), parent=self.root)
    
    def _limpiar_historicos(self):
        """Consolida y limpia archivos históricos manualmente."""
        from etl.limpieza_historicos import consolidar_historicos  # Lazy import
        
        if not ensure_base_dir(self.root, prompt_if_missing=True):
            return
        
        if not _ask_yes_no(
            "Confirmar limpieza",
            "¿Consolidar archivos históricos en HistoricoProgramasNuevos.xlsx y eliminar archivos individuales?\n\n"
            "Esto consolidará todos los archivos en outputs/historico/ y los eliminará después de consolidarlos.",
            parent=self.root
        ):
            return
        
        try:
            archivos_eliminados, registros_agregados = consolidar_historicos(umbral=1)  # Umbral 1 = consolidar todos
            
            if archivos_eliminados > 0:
                messagebox.showinfo(
                    "Limpieza completada",
                    f"Se consolidaron {archivos_eliminados} archivos históricos.\n\n"
                    f"Se agregaron {registros_agregados} registros a HistoricoProgramasNuevos.xlsx.\n\n"
                    f"Los archivos individuales fueron eliminados.",
                    parent=self.root
                )
            else:
                messagebox.showinfo(
                    "No se requirió limpieza",
                    "No hay archivos históricos suficientes para consolidar.\n\n"
                    "La limpieza automática se ejecuta cuando hay más de 20 archivos.",
                    parent=self.root
                )
        except Exception as exc:
            safe_messagebox_error("Error", f"Error al limpiar archivos históricos:\n\n{exc}", parent=self.root)


class PipelinePage(ttk.Frame):
    """Interfaz gráfica para el pipeline de análisis SNIES."""
    
    def __init__(self, parent: tk.Misc, on_back=None):
        super().__init__(parent)
        self.on_back = on_back
        self.root = parent.winfo_toplevel()
        
        # Import lazy de ARCHIVO_PROGRAMAS (se usa en validaciones)
        from etl.normalizacion import ARCHIVO_PROGRAMAS
        self.ARCHIVO_PROGRAMAS = ARCHIVO_PROGRAMAS
        
        # Estado del pipeline
        self.is_running = False
        self.base_dir = None
        self.cancel_event = threading.Event()  # Evento para cancelar la ejecución
        
        # Configurar el estilo
        self._setup_ui()
        
        # Verificar configuración inicial
        self._check_initial_config()

    def _on_resize(self, w: int, h: int) -> None:
        """Responsive: ajusta wraplengths dinámicamente."""
        # Ajustar wraplength del label de descripción
        if hasattr(self, 'pipeline_desc_label'):
            wraplength = max(400, w - 100)
            self.pipeline_desc_label.config(wraplength=wraplength)

    def _setup_ui(self):
        """Configura la interfaz de usuario."""
        main_frame = ttk.Frame(self, padding=20, style="Page.TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título y navegación mejorado
        title_frame = ttk.Frame(main_frame, style="Page.TFrame")
        title_frame.pack(fill=tk.X, pady=(0, 20))
        ttk.Label(title_frame, text="▶️ Pipeline de Análisis SNIES", style="Header.TLabel").pack(side=tk.LEFT)
        if self.on_back:
            ttk.Button(title_frame, text="← Volver al menú", command=lambda: self.on_back() if self.on_back else None, style="Back.TButton").pack(side=tk.RIGHT)
        
        # Card: Carpeta del proyecto mejorada
        dir_card = ttk.Frame(main_frame, padding=16, style="Card.TFrame")
        dir_card.pack(fill=tk.X, pady=(0, 14))
        ttk.Label(dir_card, text="📁 Carpeta del proyecto", style="SectionTitle.TLabel").pack(anchor="w")
        self.dir_label = ttk.Label(dir_card, text="No configurado", style="Muted.TLabel")
        self.dir_label.pack(anchor="w", pady=(8, 10))
        btn_change_dir = ttk.Button(dir_card, text="📂 Cambiar carpeta", command=self._select_base_directory, style="Secondary.TButton")
        btn_change_dir.pack(anchor="w")
        
        # Card: Ejecución mejorada
        run_card = ttk.Frame(main_frame, padding=16, style="Card.TFrame")
        run_card.pack(fill=tk.X, pady=(0, 14))
        ttk.Label(run_card, text="🚀 Ejecutar", style="SectionTitle.TLabel").pack(anchor="w")
        self.pipeline_desc_label = ttk.Label(run_card, text="Descarga SNIES, normaliza, detecta nuevos y clasifica. Puede tardar varios minutos.", style="Muted.TLabel")
        self.pipeline_desc_label.pack(anchor="w", pady=(6, 12), fill=tk.X)
        self.last_success_label = ttk.Label(run_card, text="Última ejecución exitosa: —", style="Muted.TLabel")
        self.last_success_label.pack(anchor="w", pady=(0, 8))
        btn_frame = ttk.Frame(run_card, style="Card.TFrame")
        btn_frame.pack(fill=tk.X)
        self.btn_execute = ttk.Button(btn_frame, text="▶️ Ejecutar pipeline", command=self._on_execute_clicked, state=tk.DISABLED, style="Primary.TButton")
        self.btn_execute.pack(side=tk.LEFT)
        self.btn_cancel = ttk.Button(btn_frame, text="⏹️ Cancelar", command=self._on_cancel_clicked, state=tk.DISABLED, style="Danger.TButton")
        self.btn_cancel.pack(side=tk.LEFT, padx=(10, 0))
        self.btn_validar = ttk.Button(btn_frame, text="🔍 Validar entorno", command=self._on_validar_entorno, style="Secondary.TButton")
        self.btn_validar.pack(side=tk.LEFT, padx=(10, 0))
        self.progress_label = ttk.Label(btn_frame, text="Progreso: listo", style="Status.TLabel")
        self.progress_label.pack(side=tk.LEFT, padx=(16, 0))
        self.progress = ttk.Progressbar(run_card, mode="determinate", maximum=7, value=0)
        self.progress.pack(fill=tk.X, pady=(12, 0))
        
        # Card: Estado y mensajes mejorada
        status_card = ttk.Frame(main_frame, padding=16, style="Card.TFrame")
        status_card.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        ttk.Label(status_card, text="📋 Estado y mensajes", style="SectionTitle.TLabel").pack(anchor="w")
        self.status_label = ttk.Label(status_card, text="Listo. Configure la carpeta para habilitar la ejecución.", style="Muted.TLabel")
        self.status_label.pack(anchor="w", pady=(6, 8))
        scrollbar = ttk.Scrollbar(status_card)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.messages_text = tk.Text(
            status_card,
            height=10,
            wrap=tk.WORD,
            yscrollcommand=scrollbar.set,
            state=tk.DISABLED,
            font=("Consolas", 9),
            bg=EAFIT["card_bg"],
            fg=EAFIT["text"],
        )
        self.messages_text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.messages_text.yview)
    
    def _refresh_last_success_label(self):
        """Actualiza la etiqueta de última ejecución exitosa desde config."""
        try:
            from etl.config import get_last_success
            iso_ts, dur_min = get_last_success()
            if iso_ts:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
                    text = dt.strftime("Última ejecución exitosa: %d/%m/%Y %H:%M")
                    if dur_min is not None:
                        text += f" ({dur_min:.1f} min)"
                except Exception:
                    text = f"Última ejecución exitosa: {iso_ts[:16]}"
            else:
                text = "Última ejecución exitosa: —"
            self.last_success_label.config(text=text)
        except Exception:
            self.last_success_label.config(text="Última ejecución exitosa: —")

    def _on_validar_entorno(self):
        """Ejecuta validación del entorno y muestra el resultado en un messagebox."""
        ok, mensajes = validar_entorno_pipeline()
        if ok:
            messagebox.showinfo("Validar entorno", "\n".join(mensajes), parent=self.root)
        else:
            messagebox.showwarning(
                "Validar entorno",
                "El entorno no está listo:\n\n" + "\n".join(f"• {m}" for m in mensajes),
                parent=self.root
            )

    def _check_initial_config(self):
        """Verifica si hay una configuración inicial y solicita la carpeta si es necesario."""
        base_dir = get_configured_base_dir()
        self._refresh_last_success_label()

        if not base_dir or not base_dir.exists():
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
            self._refresh_last_success_label()
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
            self.dir_label.config(text=dir_str, foreground=EAFIT["text"])
        else:
            self.dir_label.config(text="No configurado", foreground=EAFIT["text_muted"])
    
    def _log_message(self, message: str):
        """Agrega un mensaje al área de texto."""
        self.messages_text.config(state=tk.NORMAL)
        timestamp = time.strftime("%H:%M:%S")
        self.messages_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.messages_text.see(tk.END)
        self.messages_text.config(state=tk.DISABLED)
        self.root.update_idletasks()
    
    def _update_status(self, status: str, color: str = "black"):
        """Actualiza el estado mostrado en la interfaz. Usa colorimetría EAFIT."""
        color_map = {
            "black": EAFIT["text"],
            "green": EAFIT["success"],
            "red": EAFIT["danger"],
            "orange": EAFIT["warning"],
            "gray": EAFIT["text_muted"],
        }
        fg = color_map.get(color, color)
        self.status_label.config(text=status, foreground=fg)
        self.root.update_idletasks()
    
    def _on_execute_clicked(self):
        """Maneja el evento de clic en el botón de ejecutar."""
        if self.is_running:
            messagebox.showwarning("Atención", "El pipeline ya se está ejecutando.", parent=self.root)
            return
        
        if not self.base_dir:
            messagebox.showerror("Error", "Debe configurar la carpeta del proyecto primero.", parent=self.root)
            return

        # Pre-chequeos de estabilidad:
        # - lock activo
        lock = get_pipeline_lock_file()
        age = get_lock_age_seconds(lock)
        if age is not None and age < LOCK_STALE_SECONDS:
            messagebox.showwarning(
                "Atención",
                "Se detectó que el pipeline ya está en ejecución (lock activo).\n\n"
                "Espera a que termine antes de iniciar otro proceso.",
                parent=self.root,
            )
            return
        if age is not None and age >= LOCK_STALE_SECONDS:
            # lock viejo: permitir desbloquear
            if _ask_yes_no(
                "Lock detectado",
                "Se detectó un lock antiguo (posible cierre inesperado).\n\n"
                "¿Deseas eliminar el lock para continuar?",
            ):
                try:
                    lock.unlink()
                except Exception:
                    pass

        # - archivo Programas.xlsx abierto/bloqueado
        if self.ARCHIVO_PROGRAMAS.exists() and not can_write_file(self.ARCHIVO_PROGRAMAS):
            safe_messagebox_error("Error", explain_file_in_use(), parent=self.root)
            return
        
        # Confirmar ejecución
        result = messagebox.askyesno(
            "Confirmar Ejecución",
            "¿Desea ejecutar el pipeline ahora?\n\n"
            "Este proceso puede tardar varios minutos.",
            parent=self.root
        )
        
        if result:
            self._execute_pipeline()
    
    def _execute_pipeline(self):
        """Ejecuta el pipeline en un hilo separado."""
        self.is_running = True
        self.cancel_event.clear()  # Resetear el evento de cancelación
        self.btn_execute.config(state=tk.DISABLED)
        self.btn_cancel.config(state=tk.NORMAL)
        self._update_status("Procesando...", "orange")
        self._log_message("=" * 50)
        self._log_message("Iniciando ejecución del pipeline...")
        self.progress.config(value=0)
        self.progress_label.config(text="Progreso: iniciando...", foreground=EAFIT["text_muted"])
        
        # Ejecutar en un hilo separado para no bloquear la GUI
        thread = threading.Thread(target=self._run_pipeline_thread, daemon=True)
        thread.start()
    
    def _on_cancel_clicked(self):
        """Maneja el clic en el botón Cancelar."""
        if not self.is_running:
            return
        
        result = messagebox.askyesno(
            "Cancelar ejecución",
            "¿Está seguro de que desea cancelar la ejecución del pipeline?\n\n"
            "Los cambios realizados hasta el momento se perderán.",
            parent=self.root
        )
        
        if result:
            self.cancel_event.set()
            self._log_message("[CANCELADO] Cancelación solicitada por el usuario...")
            self._update_status("Cancelando...", "orange")
    
    def _run_pipeline_thread(self):
        """Ejecuta el pipeline en un hilo separado."""
        try:
            # Actualizar rutas para usar el base_dir configurado
            update_paths_for_base_dir(self.base_dir)
            
            # Ejecutar el pipeline
            def progress_cb(stage_idx: int, stage_name: str, status: str):
                # status: "start" | "done"
                def apply():
                    if status == "start":
                        self.progress_label.config(text=f"Progreso: {stage_name}...", foreground=EAFIT["text_muted"])
                        self.progress.config(value=stage_idx)
                    elif status == "done":
                        self.progress_label.config(text=f"Progreso: {stage_name} ✓", foreground=EAFIT["success"])
                        self.progress.config(value=stage_idx + 1)
                self.root.after(0, apply)

            resultado = run_pipeline(
                self.base_dir,
                log_callback=self._log_message,
                progress_callback=progress_cb,
                cancel_event=self.cancel_event
            )
            
            # Verificar si fue cancelado (verificar ANTES de actualizar UI para evitar race condition)
            was_cancelled = self.cancel_event.is_set()
            
            if was_cancelled:
                self.root.after(0, self._on_pipeline_error, "Cancelado por el usuario")
            else:
                # Actualizar UI en el hilo principal
                self.root.after(0, self._on_pipeline_completed, resultado == 0)
            
        except Exception as e:
            error_msg = f"Error inesperado: {str(e)}"
            self.root.after(0, self._on_pipeline_error, error_msg)
    
    def _on_pipeline_completed(self, success: bool):
        """Maneja la finalización del pipeline."""
        self.is_running = False
        self.btn_execute.config(state=tk.NORMAL)
        self.btn_cancel.config(state=tk.DISABLED)
        
        if success:
            self._refresh_last_success_label()
            self._update_status("Completado", "green")
            self._log_message("=" * 50)
            self._log_message("✓ Pipeline completado exitosamente")
            messagebox.showinfo(
                "Éxito",
                "El pipeline se ejecutó correctamente.\n\n"
                f"Los archivos se guardaron en:\n{self.base_dir / 'outputs'}",
                parent=self.root
            )
        else:
            self._update_status("Error", "red")
            self._log_message("=" * 50)
            self._log_message("✗ El pipeline finalizó con errores")
            messagebox.showerror(
                "Error",
                "El pipeline finalizó con errores.\n\n"
                "Revise los mensajes para más detalles.",
                parent=self.root
            )
    
    def _on_pipeline_error(self, error_msg: str):
        """Maneja errores durante la ejecución del pipeline."""
        self.is_running = False
        self.btn_execute.config(state=tk.NORMAL)
        self.btn_cancel.config(state=tk.DISABLED)
        
        # Verificar si fue cancelación
        if "Cancelado" in error_msg or self.cancel_event.is_set():
            self._update_status("Cancelado", "orange")
            self._log_message("✗ Ejecución cancelada por el usuario")
            messagebox.showinfo(
                "Cancelado",
                "La ejecución del pipeline fue cancelada.\n\n"
                "Los cambios realizados hasta el momento se descartaron.",
                parent=self.root
            )
        else:
            self._update_status("Error", "red")
            self._log_message(f"✗ ERROR: {error_msg}")
            messagebox.showerror("Error", f"Error durante la ejecución:\n\n{error_msg}", parent=self.root)


def run_pipeline(
    base_dir: Path,
    log_callback=None,
    progress_callback: Callable[[int, str, str], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> int:
    """
    Ejecuta el pipeline completo de análisis SNIES.

    progress_callback(stage_idx, stage_name, status)
      - status: "start" | "done"
    """
    # Imports lazy de módulos ETL pesados (solo cuando se ejecuta el pipeline)
    import pandas as pd
    from etl.historicoProgramasNuevos import actualizar_historico_programas_nuevos
    from etl.normalizacion import ARCHIVO_PROGRAMAS, normalizar_programas
    from etl.normalizacion_final import aplicar_normalizacion_final
    from etl.pipeline_logger import (
        log_error,
        log_etapa_completada,
        log_etapa_iniciada,
        log_exception,
        log_fin,
        log_inicio,
        log_warning,
    )
    from etl.procesamientoSNIES import procesar_programas_nuevos
    from etl.clasificacionProgramas import clasificar_programas_nuevos
    
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            print(msg)

    def progress(stage_idx: int, stage_name: str, status: str):
        if progress_callback is None:
            return
        try:
            progress_callback(stage_idx, stage_name, status)
        except Exception:
            pass

    # Lock file: indica que el pipeline está ejecutándose.
    # Sirve para que otras ventanas no intenten escribir Programas.xlsx mientras se reescribe.
    lock_file = ARCHIVO_PROGRAMAS.parent / ".pipeline.lock"
    
    tiempo_inicio = time.time()
    log_inicio()
    # Crear lock
    lock_created = False
    try:
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        lock_file.write_text(f"running_since={time.strftime('%Y-%m-%d %H:%M:%S')}\n", encoding="utf-8")
        lock_created = True
    except Exception as e:
        # Si no se puede crear el lock, no bloqueamos la ejecución pero lo registramos
        log_warning(f"No se pudo crear lock file: {e}")

    pipeline_failed = [False]

    try:
        # Pre-checks centralizados (fallar temprano con mensajes claros)
        ok_env, mensajes_env = validar_entorno_pipeline()
        if not ok_env:
            msg = "El entorno no está listo para ejecutar el pipeline:\n\n" + "\n".join(f"• {m}" for m in mensajes_env if "Todo listo" not in m)
            log(f"[ERROR] {msg}")
            log_error(msg)
            pipeline_failed[0] = True
            return 1
        log(mensajes_env[0] if mensajes_env else "Pre-checks OK.")

        # Verificar modelos ML y entrenar automáticamente si es la primera vez
        modelo_clf = None
        necesita_entrenar = False
        try:
            modelo_clf = MODELS_DIR / "clasificador_referentes.pkl"
            modelo_emb = MODELS_DIR / "modelo_embeddings.pkl"
            encoder = MODELS_DIR / "encoder_programas_eafit.pkl"
            
            # Verificar si faltan modelos (primera ejecución)
            if not modelo_clf.exists() or not modelo_emb.exists() or not encoder.exists():
                necesita_entrenar = True
                log("[INFO] No se encontraron modelos ML entrenados. Esto parece ser la primera ejecución.")
                log("[INFO] Se intentará entrenar el modelo automáticamente...")
        except Exception:
            pass

        progress(0, "Inicializando", "start")
        log("=== Paso 1: Resguardo de históricos ===")
        log(
            f"Si se logra obtener una versión nueva de Programas.xlsx, "
            f"el archivo anterior se trasladará a: {HISTORIC_DIR}"
        )
        log(
            "Si falla la descarga SNIES, no se realizará ninguna modificación sobre archivos existentes."
        )
        progress(0, "Inicializando", "done")
    
        # Verificar cancelación antes de iniciar descarga
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado antes de iniciar descarga.")
            return 1
        
        progress(1, "Descarga SNIES", "start")
        t_etapa = time.time()
        log("=== Paso 2: Descarga de Programas SNIES ===")
        log_etapa_iniciada("Descarga SNIES")
        log("Descargando archivo desde SNIES...")
        # Import diferido: permite abrir el menú aunque no estén instaladas las dependencias de Selenium.
        try:
            from etl.descargaSNIES import main as descargar_programas
        except Exception as exc:
            error_msg = (
                "No se pudo iniciar el módulo de descarga (Selenium). "
                "Verifica que el entorno virtual esté activo y que las dependencias estén instaladas.\n\n"
                f"Detalle: {exc}"
            )
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            pipeline_failed[0] = True
            return 1

        ruta_descargada = descargar_programas(log_callback=log, cancel_event=cancel_event)
        
        # Verificar cancelación después de descarga
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado durante la descarga.")
            pipeline_failed[0] = True
            return 1
        
        if not ruta_descargada:
            error_msg = "No se obtuvo una ruta de descarga válida."
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            pipeline_failed[0] = True
            return 1

        ruta_descargada = Path(ruta_descargada)
        if not ruta_descargada.exists():
            error_msg = f"El archivo descargado no existe: {ruta_descargada}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            return 1

        nombre_archivo = ruta_descargada.name
        log(f"✓ Archivo descargado: {nombre_archivo}")
        log_etapa_completada("Descarga SNIES", f"{nombre_archivo} (duración: {time.time() - t_etapa:.1f}s)")
        progress(1, "Descarga SNIES", "done")

        # Validación de schema mínimo (para detectar cambios en SNIES)
        ok, msg = validate_programas_schema(ARCHIVO_PROGRAMAS)
        if not ok:
            log(f"[ERROR] {msg}")
            log_error(msg)
            pipeline_failed[0] = True
            return 1

        if ruta_descargada != ARCHIVO_PROGRAMAS:
            warning_msg = (
                f"El archivo descargado está en {ruta_descargada}, "
                f"pero la normalización usará {ARCHIVO_PROGRAMAS}."
            )
            log(f"[WARN] {warning_msg}")
            log_warning(warning_msg)

        # OPTIMIZACIÓN: Leer archivo una sola vez y trabajar en memoria
        log("=== OPTIMIZACIÓN: Pipeline en memoria ===")
        log("Leyendo archivo una vez y procesando en memoria para reducir I/O...")
        from etl.exceptions_helpers import leer_excel_con_reintentos
        
        try:
            df_programas = leer_excel_con_reintentos(ARCHIVO_PROGRAMAS, sheet_name="Programas")
            
            # Validar que el DataFrame no esté vacío
            if df_programas is None:
                error_msg = "El archivo Programas.xlsx está vacío o no se pudo leer correctamente."
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                pipeline_failed[0] = True
                return 1
            
            if len(df_programas) == 0:
                error_msg = "El archivo Programas.xlsx no contiene filas de datos."
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                pipeline_failed[0] = True
                return 1
            
            log(f"✓ Archivo cargado en memoria: {len(df_programas)} filas")
        except Exception as exc:
            error_msg = f"No se pudo leer Programas.xlsx: {exc}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            log_exception(exc)
            pipeline_failed[0] = True
            return 1

        # Verificar cancelación antes de normalización
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado antes de normalización.")
            pipeline_failed[0] = True
            return 1
        
        progress(2, "Normalización", "start")
        t_etapa = time.time()
        log("=== Paso 3: Normalización de columnas ===")
        log_etapa_iniciada("Normalización")
        try:
            log("Normalizando columnas del archivo...")
            df_programas = normalizar_programas(df=df_programas)  # Modo optimizado: en memoria
            
            # Validar que la normalización retornó un DataFrame válido
            if df_programas is None or len(df_programas) == 0:
                error_msg = "La normalización retornó un DataFrame vacío o None."
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                pipeline_failed[0] = True
                return 1
            
            log("✓ Normalización completada")
            log_etapa_completada("Normalización", f"duración: {time.time() - t_etapa:.1f}s")
            progress(2, "Normalización", "done")
        except Exception as exc:
            error_msg = f"Falló la normalización: {exc}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            log_exception(exc)
            pipeline_failed[0] = True
            return 1

        # Verificar cancelación antes de procesamiento
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado antes de procesamiento de programas nuevos.")
            pipeline_failed[0] = True
            return 1
        
        progress(3, "Programas nuevos", "start")
        t_etapa = time.time()
        log("=== Paso 4: Procesamiento de programas nuevos ===")
        log_etapa_iniciada("Procesamiento de programas nuevos")
        try:
            log("Procesando programas nuevos...")
            df_programas = procesar_programas_nuevos(df=df_programas)  # Modo optimizado: en memoria
            
            # Validar que el procesamiento retornó un DataFrame válido
            if df_programas is None or len(df_programas) == 0:
                error_msg = "El procesamiento de programas nuevos retornó un DataFrame vacío o None."
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                pipeline_failed[0] = True
                return 1
            
            log("✓ Procesamiento completado")
            log_etapa_completada("Procesamiento de programas nuevos", f"duración: {time.time() - t_etapa:.1f}s")
            progress(3, "Programas nuevos", "done")
        except Exception as exc:
            error_msg = f"Falló el procesamiento de programas nuevos: {exc}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            pipeline_failed[0] = True
            return 1

        progress(4, "Clasificación", "start")
        t_etapa_clasif = time.time()
        log("=== Paso 5: Clasificación de programas nuevos ===")
        log_etapa_iniciada("Clasificación de programas nuevos")
        
        # Si es la primera ejecución y faltan modelos, entrenar automáticamente
        if necesita_entrenar:
            log("Entrenando modelo ML automáticamente (primera ejecución)...")
            log("Esto puede tardar varios minutos. Por favor espera...")
            try:
                from etl.clasificacionProgramas import entrenar_y_guardar_modelo, get_archivo_referentes
                
                # Verificar que existe el archivo de referentes
                archivo_referentes = get_archivo_referentes()
                if not archivo_referentes or not archivo_referentes.exists():
                    error_msg = (
                        f"No se encontró el archivo de referentes para entrenar.\n\n"
                        "Coloca el archivo referentesUnificados.csv o .xlsx en la carpeta ref/ "
                        "y vuelve a ejecutar el pipeline."
                    )
                    log(f"[ERROR] {error_msg}")
                    log_error(error_msg)
                    log("Continuando sin clasificación...")
                    progress(4, "Clasificación", "done")
                else:
                    entrenar_y_guardar_modelo()
                    log("✓ Modelo entrenado exitosamente")
            except Exception as exc:
                error_msg = f"Falló el entrenamiento automático del modelo: {exc}"
                log(f"[WARN] {error_msg}")
                log("Continuando sin clasificación...")
                log_error(error_msg)
                log_warning("Continuando sin clasificación...")
                progress(4, "Clasificación", "done")
        
        # Intentar clasificar programas nuevos
        try:
            log("Clasificando programas nuevos...")
            log("La clasificación compara cada programa nuevo del SNIES con el catálogo EAFIT.")
            log("Para cada programa nuevo, determina:")
            log("  - Si es referente (ES_REFERENTE: Sí/No)")
            log("  - Qué programa EAFIT le corresponde (PROGRAMA_EAFIT_CODIGO y PROGRAMA_EAFIT_NOMBRE)")
            log("  - La probabilidad de que sea referente (PROBABILIDAD: 0.0-1.0)")
            log("  - Métricas de similitud (SIMILITUD_EMBEDDING, SIMILITUD_CAMPO, SIMILITUD_NIVEL)")
            def _progress_clasif(cur: int, tot: int, nom: str) -> None:
                log(f"  Clasificando {cur}/{tot}: {nom[:50]}{'...' if len(nom) > 50 else ''}")
            df_programas = clasificar_programas_nuevos(df_programas=df_programas, progress_callback=_progress_clasif)  # Modo optimizado: en memoria
            
            # Validar que la clasificación retornó un DataFrame válido
            if df_programas is None or len(df_programas) == 0:
                error_msg = "La clasificación retornó un DataFrame vacío o None."
                log(f"[WARN] {error_msg}")
                log_warning(error_msg)
                # Continuar sin clasificación en lugar de fallar
                progress(4, "Clasificación", "done")
            else:
                log("✓ Clasificación completada")
                log_etapa_completada("Clasificación de programas nuevos", f"duración: {time.time() - t_etapa_clasif:.1f}s")
                progress(4, "Clasificación", "done")
        except FileNotFoundError as exc:
            # Error específico cuando faltan modelos después del entrenamiento
            error_msg = f"No se encontraron modelos ML después del entrenamiento: {exc}"
            log(f"[WARN] {error_msg}")
            log("Continuando sin clasificación...")
            log_error(error_msg)
            log_warning("Continuando sin clasificación...")
            progress(4, "Clasificación", "done")
        except Exception as exc:
            error_msg = f"Falló la clasificación de programas nuevos: {exc}"
            log(f"[WARN] {error_msg}")
            log("Continuando sin clasificación...")
            log_error(error_msg)
            log_warning("Continuando sin clasificación...")
            progress(4, "Clasificación", "done")
            # No retornamos error aquí porque la clasificación es opcional

        # Verificar cancelación antes de normalización final
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado antes de normalización final.")
            pipeline_failed[0] = True
            return 1
        
        progress(5, "Normalización final", "start")
        t_etapa = time.time()
        log("=== Paso 6: Normalización final de ortografía y formato ===")
        log_etapa_iniciada("Normalización final")
        try:
            log("Aplicando normalización final...")
            df_programas = aplicar_normalizacion_final(df=df_programas)  # Modo optimizado: en memoria
            
            # Validar que la normalización final retornó un DataFrame válido
            if df_programas is None or len(df_programas) == 0:
                error_msg = "La normalización final retornó un DataFrame vacío o None."
                log(f"[ERROR] {error_msg}")
                log_error(error_msg)
                pipeline_failed[0] = True
                return 1
            
            log("✓ Normalización final completada")
            log_etapa_completada("Normalización final", f"duración: {time.time() - t_etapa:.1f}s")
            progress(5, "Normalización final", "done")
        except Exception as exc:
            error_msg = f"Falló la normalización final: {exc}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            log_exception(exc)
            pipeline_failed[0] = True
            return 1

        # Verificar cancelación antes de guardar
        if cancel_event and cancel_event.is_set():
            log("[CANCELADO] Pipeline cancelado antes de guardar archivo.")
            pipeline_failed[0] = True
            return 1
        
        # OPTIMIZACIÓN: Escribir archivo una sola vez al final
        progress(6, "Guardando archivo", "start")
        t_etapa = time.time()
        log("=== Paso 7: Guardando archivo final ===")
        log_etapa_iniciada("Guardado de archivo")
        try:
            from etl.exceptions_helpers import escribir_excel_con_reintentos
            log("Guardando Programas.xlsx con todos los cambios aplicados...")
            escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_programas, sheet_name="Programas")
            log("✓ Archivo guardado exitosamente")
            log_etapa_completada("Guardado de archivo", f"duración: {time.time() - t_etapa:.1f}s")
            progress(6, "Guardando archivo", "done")
        except Exception as exc:
            error_msg = f"Falló el guardado del archivo: {exc}"
            log(f"[ERROR] {error_msg}")
            log_error(error_msg)
            log_exception(exc)
            pipeline_failed[0] = True
            return 1

        progress(7, "Histórico programas nuevos", "start")
        t_etapa = time.time()
        log("=== Paso 8: Actualización de histórico de programas nuevos ===")
        log_etapa_iniciada("Actualización de histórico de programas nuevos")
        try:
            log("Actualizando histórico...")
            actualizar_historico_programas_nuevos()
            log("✓ Histórico actualizado")
            log_etapa_completada("Actualización de histórico de programas nuevos", f"duración: {time.time() - t_etapa:.1f}s")
            progress(7, "Histórico programas nuevos", "done")
        except Exception as exc:
            error_msg = f"Falló la actualización del histórico: {exc}"
            log(f"[WARN] {error_msg}")
            log_error(error_msg)
            progress(7, "Histórico programas nuevos", "done")
            # No retornamos error aquí porque el histórico es complementario

        # Limpieza automática de archivos históricos (si hay muchos)
        try:
            from etl.limpieza_historicos import limpiar_historicos_automatico
            log("Verificando si se requiere limpieza de archivos históricos...")
            if limpiar_historicos_automatico():
                log("✓ Archivos históricos consolidados y limpiados")
            else:
                log("No se requiere limpieza de archivos históricos (pocos archivos)")
        except Exception as exc:
            log(f"[WARN] Error en limpieza automática de históricos: {exc}")
            # No es crítico, continuar

        tiempo_fin = time.time()
        duracion_minutos = (tiempo_fin - tiempo_inicio) / 60.0
        log("Pipeline completado exitosamente.")
        log_fin(duracion_minutos)
        try:
            from etl.config import set_last_success
            from datetime import datetime
            set_last_success(datetime.now().isoformat(), duracion_minutos)
        except Exception:
            pass

        return 0
    finally:
        # Remover lock siempre (incluso si hubo KeyboardInterrupt o SystemExit)
        try:
            if lock_file.exists():
                lock_file.unlink()
                if lock_created:
                    log_info("Lock file eliminado correctamente.")
        except Exception as e:
            log_error(f"No se pudo eliminar lock file: {e}")
            # Intentar una vez más después de un breve delay
            try:
                import time as time_module
                time_module.sleep(0.5)
                if lock_file.exists():
                    lock_file.unlink()
            except Exception:
                pass


def main():
    """Función principal que inicia el Menú Principal."""
    root = tk.Tk()
    app = MainMenuGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
