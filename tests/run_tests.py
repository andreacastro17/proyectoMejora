"""
Script para ejecutar todos los tests del proyecto.

Uso:
    python tests/run_tests.py          # Ejecuta todos los tests
    python tests/run_tests.py -v      # Modo verbose
    python tests/run_tests.py -k filtros  # Solo tests que contengan "filtros"
"""

import sys
from pathlib import Path

# Agregar el directorio raíz al path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

if __name__ == "__main__":
    import pytest
    
    # Ejecutar pytest con los argumentos pasados
    exit_code = pytest.main([__file__.replace("run_tests.py", "")] + sys.argv[1:])
    sys.exit(exit_code)
