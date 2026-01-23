@echo off
echo ========================================
echo    PIPELINE SNIES - AUTOMATIZADO
echo ========================================
echo.

REM Cambiar al directorio del proyecto
cd /d "%~dp0"

REM Verificar que Python esté disponible
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no está disponible en el sistema
    echo Por favor, instala Python desde https://www.python.org/
    pause
    exit /b 1
)

REM Verificar si existe el entorno virtual, si no, crearlo
if not exist "env\Scripts\activate.bat" (
    echo El entorno virtual no existe. Creándolo...
    python -m venv env
    if errorlevel 1 (
        echo ERROR: No se pudo crear el entorno virtual
        pause
        exit /b 1
    )
    echo Entorno virtual creado exitosamente.
)

REM Activar el entorno virtual
echo Activando entorno virtual...
call env\Scripts\activate.bat

REM Verificar que el entorno virtual se activó correctamente
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: No se pudo activar el entorno virtual
    pause
    exit /b 1
)

REM Instalar/actualizar dependencias
echo.
echo Verificando e instalando dependencias...
python -m pip install --upgrade pip >nul 2>&1
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: No se pudieron instalar las dependencias
    pause
    exit /b 1
)
echo Dependencias instaladas correctamente.

echo.
echo Ejecutando pipeline completo de SNIES...
echo.

REM Ejecutar el orquestador principal
python app\main.py

REM Verificar si la ejecución fue exitosa
if errorlevel 1 (
    echo.
    echo ERROR: El pipeline falló. Revisa los mensajes anteriores.
    echo.
) else (
    echo.
    echo ========================================
    echo    PIPELINE COMPLETADO EXITOSAMENTE
    echo ========================================
    echo.
)

REM Desactivar el entorno virtual
call env\Scripts\deactivate.bat

echo.
echo Presiona cualquier tecla para cerrar...
pause >nul
