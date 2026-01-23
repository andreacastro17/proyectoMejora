# Guía Completa de Empaquetado y Distribución

## 📋 Resumen

Esta guía explica cómo convertir el proyecto de Python en una aplicación ejecutable (.exe) con interfaz gráfica para usuarios no técnicos.

---

## 🎯 Objetivo

Crear un archivo `app.exe` que:
- ✅ Funcione sin Python instalado
- ✅ Funcione sin instalar dependencias
- ✅ Tenga una interfaz gráfica simple
- ✅ Configure automáticamente las rutas del proyecto
- ✅ Sea fácil de usar para usuarios no técnicos

---

## 📁 Estructura del Proyecto

Antes de empaquetar, asegúrese de que su proyecto tenga esta estructura:

```
proyectoMejora2/
├── app/
│   └── main.py              # Archivo principal con GUI
├── etl/                     # Módulos del pipeline
├── ref/                     # Archivos de referencia
│   ├── referentesUnificados.xlsx
│   └── catalogoOfertasEAFIT.xlsx
├── models/                  # Modelos de ML
├── docs/                    # Documentación
│   └── normalizacionFinal.xlsx
├── requirements.txt
└── build_exe.py            # Script de empaquetado
```

---

## 🔧 Paso 1: Preparar el Entorno

### 1.1 Instalar Python (solo para desarrollo)

Si aún no tiene Python instalado, descárguelo desde [python.org](https://www.python.org/).

### 1.2 Instalar Dependencias

Abra una terminal en la carpeta del proyecto y ejecute:

```bash
pip install -r requirements.txt
```

Esto instalará todas las dependencias necesarias, incluyendo PyInstaller.

---

## 📦 Paso 2: Empaquetar la Aplicación

### 2.1 Ejecutar el Script de Empaquetado

En la terminal, ejecute:

```bash
python build_exe.py
```

Este script:
1. Verifica que PyInstaller esté instalado
2. Limpia builds anteriores
3. Crea un archivo `.spec` personalizado
4. Ejecuta PyInstaller
5. Copia las carpetas necesarias (ref/, models/, docs/)
6. Genera instrucciones de uso

**Tiempo estimado:** 5-15 minutos (dependiendo de la velocidad de su PC)

### 2.2 Resultado

Después de completarse, encontrará en la carpeta `dist/`:

```
dist/
├── app.exe                 # El ejecutable principal
├── ref/                    # Copia de ref/
├── models/                 # Copia de models/
├── docs/                   # Copia de docs/
└── INSTRUCCIONES.txt       # Instrucciones de uso
```

---

## 🚀 Paso 3: Distribuir la Aplicación

### 3.1 Preparar para Distribución

Para distribuir la aplicación, debe copiar **todo el contenido** de la carpeta `dist/`:

```
CarpetaDeDistribucion/
├── app.exe
├── ref/
│   ├── referentesUnificados.xlsx
│   └── catalogoOfertasEAFIT.xlsx
├── models/
│   └── (archivos .pkl)
└── docs/
    └── normalizacionFinal.xlsx
```

**IMPORTANTE:** Todos los archivos deben estar en la misma carpeta.

### 3.2 Opciones de Distribución

- **USB/Disco externo:** Copie la carpeta completa
- **Carpeta compartida:** Comparta la carpeta en la red
- **OneDrive/Google Drive:** Suba la carpeta completa

---

## 👥 Paso 4: Uso por el Usuario Final

### 4.1 Primera Ejecución

1. El usuario hace doble clic en `app.exe`
2. Se abre una ventana con la interfaz gráfica
3. La aplicación solicita seleccionar la **carpeta raíz del proyecto**
   - Esta debe ser la carpeta que contiene: `ref/`, `models/`, `docs/`
   - Ejemplo: `C:\Users\usuario\OneDrive - Universidad EAFIT\trabajo\proyectoMejora`
4. El usuario selecciona la carpeta y presiona "Aceptar"
5. La configuración se guarda automáticamente

### 4.2 Ejecuciones Posteriores

1. El usuario hace doble clic en `app.exe`
2. Se abre la ventana con la interfaz
3. Presiona el botón **"Ejecutar Pipeline"**
4. Espera a que termine (puede tardar varios minutos)
5. Los archivos se guardan automáticamente en `outputs/`

### 4.3 Resultados

Los archivos generados se guardan en:
- `outputs/HistoricoProgramasNuevos.xlsx` (archivo principal)
- `outputs/historico/Programas_YYYYMMDD_HHMMSS.xlsx` (histórico con fecha)

---

## 🧪 Paso 5: Probar en un Equipo Limpio

### 5.1 Requisitos del Equipo de Prueba

- ✅ Windows 10 o superior
- ✅ Google Chrome instalado
- ❌ **NO necesita Python**
- ❌ **NO necesita instalar dependencias**

### 5.2 Pasos de Prueba

1. **Copiar archivos:**
   - Copie todo el contenido de `dist/` a una carpeta temporal en el equipo de prueba
   - Asegúrese de que todas las subcarpetas (ref/, models/, docs/) estén incluidas

2. **Primera ejecución:**
   - Haga doble clic en `app.exe`
   - Debe aparecer la ventana de la aplicación
   - Seleccione la carpeta raíz del proyecto

3. **Ejecutar pipeline:**
   - Presione "Ejecutar Pipeline"
   - Verifique que el proceso se ejecute correctamente
   - Verifique que se generen los archivos en `outputs/`

4. **Verificar resultados:**
   - Revise que exista `outputs/HistoricoProgramasNuevos.xlsx`
   - Revise que exista al menos un archivo en `outputs/historico/`

### 5.3 Problemas Comunes

| Problema | Solución |
|----------|----------|
| "Chrome no encontrado" | Instalar Google Chrome |
| Error al seleccionar carpeta | Verificar que la carpeta contenga ref/, models/, docs/ |
| Error de permisos | Ejecutar como administrador o cambiar permisos de la carpeta |
| La aplicación no inicia | Verificar que ref/, models/, docs/ estén en la misma carpeta que app.exe |

---

## 🔍 Solución de Problemas

### Problema: PyInstaller no encuentra módulos

**Solución:** Verifique que todas las dependencias estén en `requirements.txt` y ejecute:
```bash
pip install --upgrade -r requirements.txt
```

### Problema: El ejecutable es muy grande (>500MB)

**Es normal.** El ejecutable incluye:
- Python completo
- Todas las dependencias
- Modelos de ML (sentence-transformers puede ser grande)
- Librerías de Selenium

### Problema: Error "ModuleNotFoundError" al ejecutar

**Solución:** Agregue el módulo faltante a `hiddenimports` en `build_exe.py`.

### Problema: Chrome no se encuentra al ejecutar

**Solución:** 
1. Instale Google Chrome en el equipo destino
2. O modifique el código para usar una ruta específica a Chrome

---

## 📝 Notas Importantes

### Rutas del Proyecto

- **NO** se usan rutas absolutas hardcodeadas
- La aplicación pide al usuario seleccionar la carpeta raíz
- Todas las rutas se construyen relativas a esa carpeta raíz
- La configuración se guarda en `config.json` junto al ejecutable

### Configuración Automática

- La primera vez que se ejecuta, pide la carpeta raíz
- La configuración se guarda automáticamente
- En ejecuciones posteriores, no se vuelve a pedir
- El usuario puede cambiar la carpeta usando el botón "Cambiar Carpeta"

### Archivos Generados

- Los outputs siempre se guardan en `outputs/` dentro de la carpeta raíz
- No se pide al usuario dónde guardar los resultados
- Los archivos históricos tienen fecha y hora en el nombre

---

## 🎓 Arquitectura Técnica

### Flujo de Ejecución

1. **Inicio:** `app/main.py` inicia la GUI con tkinter
2. **Configuración:** Lee `config.json` o solicita la carpeta raíz
3. **Actualización de rutas:** Llama a `update_paths_for_base_dir()`
4. **Ejecución:** Ejecuta `run_pipeline()` en un hilo separado
5. **Pipeline:** Ejecuta todos los pasos del ETL
6. **Resultados:** Muestra mensajes de estado y resultados en la GUI

### Componentes Principales

- **`app/main.py`:** GUI con tkinter y lógica de ejecución
- **`etl/config.py`:** Gestión de rutas y configuración
- **`build_exe.py`:** Script de empaquetado con PyInstaller

---

## 📞 Soporte

Si tiene problemas:

1. Revise los logs en `logs/pipeline.log`
2. Revise los mensajes en la ventana de la aplicación
3. Verifique que todas las carpetas (ref/, models/, docs/) estén presentes
4. Verifique que Google Chrome esté instalado

---

## ✅ Checklist de Empaquetado

Antes de distribuir, verifique:

- [ ] PyInstaller está instalado
- [ ] Todas las dependencias están en requirements.txt
- [ ] El script build_exe.py se ejecutó sin errores
- [ ] La carpeta dist/ contiene app.exe y las carpetas ref/, models/, docs/
- [ ] Se probó en un equipo limpio (sin Python)
- [ ] La primera ejecución solicita la carpeta raíz correctamente
- [ ] El pipeline se ejecuta correctamente
- [ ] Los archivos se generan en outputs/

---

¡Listo! Ahora tiene una aplicación ejecutable lista para distribuir. 🎉

