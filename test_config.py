"""
Script de prueba para verificar que la configuración funciona correctamente.
Ejecuta este script para verificar que las rutas se configuran bien localmente.
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.config import print_config_info

if __name__ == "__main__":
    print("=" * 60)
    print("  PRUEBA DE CONFIGURACIÓN LOCAL")
    print("=" * 60)
    print()
    print_config_info()
    print()
    print("✅ Si ves las rutas correctas arriba, la configuración funciona bien.")
    print("✅ Puedes ejecutar el pipeline con: python app/main.py")
    print("✅ O usar el archivo .bat: descarga.bat")

