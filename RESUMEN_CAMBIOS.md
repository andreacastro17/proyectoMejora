# Resumen de Cambios - Empaquetado en .EXE y Rutas Configurables

## ✅ Cambios Realizados

### 1. Módulo de Configuración Centralizado (`etl/config.py`)
- **Nuevo archivo** que centraliza todas las rutas del proyecto
- Detecta automáticamente si se ejecuta como script o como .EXE
- Permite configurar rutas personalizadas mediante `config.json`
- Crea automáticamente los directorios necesarios

### 2. Actualización de Archivos ETL
Todos los archivos ETL ahora usan el módulo de configuración en lugar de rutas hardcodeadas:
- ✅ `etl/descargaSNIES.py`
- ✅ `etl/normalizacion.py`
- ✅ `etl/historicoProgramasNuevos.py`
- ✅ `etl/procesamientoSNIES.py`
- ✅ `etl/normalizacion_final.py`
- ✅ `etl/clasificacionProgramas.py`
- ✅ `etl/pipeline_logger.py`

### 3. Script de Empaquetado (`build_exe.py`)
- **Nuevo archivo** para crear el ejecutable .EXE
- Instala PyInstaller automáticamente si no está disponible
- Incluye todas las carpetas necesarias (`ref/`, `models/`, `docs/`)
- Genera instrucciones de uso

### 4. Archivos de Configuración
- ✅ `config.json` - Archivo de configuración (puede editarse para personalizar rutas)
- ✅ `config.json.example` - Ejemplo de configuración
- ✅ `INSTRUCCIONES_EMPAQUETADO.md` - Guía completa de empaquetado

### 5. Actualización de Dependencias
- ✅ `requirements.txt` - Agregado PyInstaller

## 🚀 Cómo Usar

### Para Empaquetar en .EXE:

```bash
python build_exe.py
```

El ejecutable se creará en `dist/PipelineSNIES.exe`

### Para Configurar Rutas de Carpeta Compartida:

1. Edita `config.json` (o créalo si no existe)
2. Especifica la ruta de la carpeta compartida en `outputs_dir`:
   ```json
   {
     "outputs_dir": "\\\\servidor\\carpeta_compartida\\outputs"
   }
   ```
3. Deja las demás rutas vacías para usar rutas relativas

### Estructura para Distribución:

```
CarpetaDistribucion/
├── PipelineSNIES.exe
├── config.json (opcional - para rutas personalizadas)
├── ref/
│   ├── referentesUnificados.xlsx
│   └── catalogoOfertasEAFIT.xlsx
├── models/
│   └── *.pkl (modelos entrenados)
└── docs/
    └── normalizacionFinal.xlsx
```

## 📝 Ventajas

1. **Portabilidad**: El ejecutable funciona en cualquier máquina sin necesidad de Python
2. **Rutas Configurables**: Cada usuario puede configurar sus propias rutas mediante `config.json`
3. **Carpeta Compartida**: Fácil configuración para usar una carpeta compartida en red
4. **Sin Código Fuente**: El .EXE no requiere el código fuente para ejecutarse
5. **Rutas Relativas por Defecto**: Si no se configura nada, usa rutas relativas al ejecutable

## ⚠️ Notas Importantes

- El ejecutable necesita **Google Chrome** instalado en el sistema
- Las rutas en `config.json` deben ser accesibles para todos los usuarios
- Si usas una carpeta compartida, asegúrate de tener permisos de escritura
- El programa crea automáticamente las carpetas necesarias si no existen

