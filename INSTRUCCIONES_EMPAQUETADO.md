# Instrucciones para Empaquetar y Distribuir el Proyecto

## 📦 Empaquetar en .EXE

### Paso 1: Preparar el entorno

1. Asegúrate de tener todas las dependencias instaladas:
   ```bash
   pip install -r requirements.txt
   ```

2. Verifica que todos los archivos necesarios estén presentes:
   - `ref/referentesUnificados.xlsx`
   - `ref/catalogoOfertasEAFIT.xlsx`
   - `docs/normalizacionFinal.xlsx`
   - `models/*.pkl` (si existen modelos entrenados)

### Paso 2: Ejecutar el script de empaquetado

```bash
python build_exe.py
```

Este script:
- Instalará PyInstaller si no está disponible
- Limpiará builds anteriores
- Creará el ejecutable `PipelineSNIES.exe` en la carpeta `dist/`
- Generará un archivo de instrucciones

### Paso 3: Distribuir el ejecutable

El ejecutable se encuentra en `dist/PipelineSNIES.exe`. Para distribuirlo:

1. **Copia el ejecutable** a la ubicación deseada
2. **Copia las carpetas necesarias** junto al ejecutable:
   - `ref/` (con todos sus archivos)
   - `models/` (si existen modelos entrenados)
   - `docs/` (con `normalizacionFinal.xlsx`)
3. **Crea un archivo `config.json`** (opcional) si necesitas rutas personalizadas

## 🔧 Configuración de Rutas para Carpeta Compartida

Si quieres que los outputs se guarden en una carpeta compartida, crea un archivo `config.json` en la misma carpeta que el ejecutable:

```json
{
  "outputs_dir": "\\\\servidor\\carpeta_compartida\\outputs",
  "ref_dir": "",
  "models_dir": "",
  "docs_dir": "",
  "logs_dir": "",
  "headless": false,
  "max_wait_download_sec": 180
}
```

### Ejemplo de estructura para distribución:

```
CarpetaDistribucion/
├── PipelineSNIES.exe
├── config.json (opcional)
├── ref/
│   ├── referentesUnificados.xlsx
│   └── catalogoOfertasEAFIT.xlsx
├── models/
│   ├── clasificador_referentes.pkl
│   ├── modelo_embeddings.pkl
│   └── encoder_programas_eafit.pkl
└── docs/
    └── normalizacionFinal.xlsx
```

## 📝 Notas Importantes

1. **Rutas relativas vs absolutas**:
   - Si dejas una ruta vacía en `config.json`, se usará la ruta relativa al ejecutable
   - Si especificas una ruta absoluta, se usará esa ruta (útil para carpetas compartidas)

2. **Carpeta compartida**:
   - Asegúrate de que todos los usuarios tengan permisos de lectura/escritura
   - Usa rutas UNC para carpetas compartidas (ej: `\\servidor\carpeta`)
   - El programa creará automáticamente las subcarpetas necesarias

3. **Primera ejecución**:
   - El programa creará automáticamente las carpetas `outputs/` y `logs/` si no existen
   - Si usas una carpeta compartida, asegúrate de que la ruta sea accesible

## 🚀 Uso del Ejecutable

1. Coloca el ejecutable y las carpetas necesarias en la ubicación deseada
2. (Opcional) Crea y edita `config.json` para personalizar rutas
3. Ejecuta `PipelineSNIES.exe` haciendo doble clic o desde la línea de comandos
4. Los resultados se guardarán en la carpeta configurada (o `outputs/` por defecto)

## ⚠️ Solución de Problemas

- **Error "Chrome no encontrado"**: El ejecutable necesita Google Chrome instalado en el sistema
- **Error de permisos**: Verifica que tengas permisos de escritura en las carpetas de salida
- **Error de rutas**: Revisa que las rutas en `config.json` sean correctas y accesibles
- **Archivos no encontrados**: Asegúrate de que todas las carpetas (`ref/`, `models/`, `docs/`) estén junto al ejecutable

