# Guía Completa: Cómo Subir tu Proyecto a GitHub

## ⚠️ Problema Detectado

El repositorio Git está inicializado en tu directorio home (`C:\Users\andre`) en lugar del directorio del proyecto. Esto hace que Git intente rastrear todos tus archivos personales.

## 🔧 Solución: Configurar Git Correctamente

### Paso 1: Eliminar el repositorio Git incorrecto

Abre PowerShell y ejecuta:

```powershell
# Navega a tu directorio home
cd $env:USERPROFILE

# Elimina el repositorio Git si existe
if (Test-Path .git) {
    Remove-Item -Path .git -Recurse -Force
    Write-Host "Repositorio Git eliminado del directorio home" -ForegroundColor Green
}
```

### Paso 2: Navegar al directorio del proyecto

```powershell
# Navega al directorio de tu proyecto
# Reemplaza la ruta con la ruta completa de tu proyecto
cd "OneDrive - Universidad EAFIT\Dirección de Estrategia - Documentos\Snies\proyectoMejora1"

# Verifica que estás en el lugar correcto (debe mostrar archivos como .gitignore, app/, etl/, etc.)
Get-ChildItem | Select-Object Name
```

### Paso 3: Inicializar el repositorio Git en el proyecto

```powershell
# Inicializa el repositorio Git
git init

# Verifica el estado
git status
```

Deberías ver solo los archivos de tu proyecto, no archivos personales.

### Paso 4: Configurar tu identidad en Git (si no lo has hecho)

```powershell
git config --global user.name "Tu Nombre"
git config --global user.email "tu.email@ejemplo.com"
```

## 📤 Pasos para Subir a GitHub

### Paso 1: Crear un repositorio en GitHub

1. Ve a [GitHub.com](https://github.com) e inicia sesión
2. Haz clic en el botón **"+"** en la esquina superior derecha
3. Selecciona **"New repository"**
4. Completa:
   - **Repository name**: `proyectoMejora1` (o el nombre que prefieras)
   - **Description**: "Proyecto de clasificación de programas académicos SNIES"
   - **Visibility**: Elige **Public** o **Private**
   - **NO marques** "Initialize this repository with a README" (ya tienes uno)
5. Haz clic en **"Create repository"**

### Paso 2: Agregar archivos al repositorio local

```powershell
# Asegúrate de estar en el directorio del proyecto
# Agrega todos los archivos (respetando el .gitignore)
git add .

# Verifica qué archivos se agregaron
git status
```

**Nota:** El `.gitignore` ya está configurado para excluir:
- Archivos grandes (modelos `.pkl`, ejecutables `.exe`)
- Carpetas de build (`build/`, `dist/`)
- Archivos de salida (`outputs/`, `logs/`)
- Entornos virtuales (`venv/`, `.venv/`)

### Paso 3: Hacer el primer commit

```powershell
git commit -m "Initial commit: Proyecto de clasificación SNIES"
```

### Paso 4: Conectar con GitHub

```powershell
# Reemplaza USUARIO con tu nombre de usuario de GitHub
# Reemplaza REPOSITORIO con el nombre que le diste al repositorio
git remote add origin https://github.com/USUARIO/REPOSITORIO.git

# Verifica que se agregó correctamente
git remote -v
```

**Ejemplo:**
```powershell
git remote add origin https://github.com/tu-usuario/proyectoMejora1.git
```

### Paso 5: Cambiar la rama a 'main' (si es necesario)

```powershell
git branch -M main
```

### Paso 6: Subir los archivos a GitHub

```powershell
git push -u origin main
```

**Nota sobre autenticación:**
- Si te pide usuario y contraseña, GitHub ya no acepta contraseñas
- Necesitas usar un **Personal Access Token (PAT)**
- Ve a: GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
- Crea un nuevo token con permisos `repo`
- Usa el token como contraseña cuando Git lo solicite

## 🔄 Actualizaciones Futuras

Una vez configurado, para subir cambios futuros:

```powershell
# 1. Ver qué cambió
git status

# 2. Agregar los cambios
git add .

# 3. Hacer commit
git commit -m "Descripción de los cambios"

# 4. Subir a GitHub
git push
```

## ✅ Verificación

Después de subir, verifica en GitHub:
1. Ve a tu repositorio en GitHub
2. Deberías ver todos los archivos del proyecto
3. Verifica que NO aparezcan archivos personales o archivos grandes

## 🆘 Solución de Problemas

### Error: "fatal: not a git repository"
**Solución:** Asegúrate de estar en el directorio del proyecto y ejecuta `git init`

### Error: "remote origin already exists"
**Solución:** Elimina el remote y vuelve a agregarlo:
```powershell
git remote remove origin
git remote add origin https://github.com/USUARIO/REPOSITORIO.git
```

### Error: "authentication failed"
**Solución:** Usa un Personal Access Token en lugar de tu contraseña

### Error: "file too large"
**Solución:** Verifica que el `.gitignore` esté funcionando correctamente:
```powershell
git check-ignore -v nombre_archivo_grande
```

### Archivos grandes ya agregados
Si accidentalmente agregaste archivos grandes:
```powershell
# Elimínalos del índice de Git (pero manténlos en tu disco)
git rm --cached archivo_grande.pkl
git commit -m "Remove large file"
git push
```

## 📝 Script Automatizado

También puedes usar el script `setup_git.ps1` que ya tienes en el proyecto:

```powershell
# Desde el directorio del proyecto
.\setup_git.ps1
```

Luego sigue los pasos 4-6 de esta guía para conectar con GitHub.

## 🎯 Resumen Rápido

```powershell
# 1. Eliminar Git del directorio home (si existe)
cd $env:USERPROFILE
if (Test-Path .git) { Remove-Item -Path .git -Recurse -Force }

# 2. Ir al proyecto
cd "OneDrive - Universidad EAFIT\Dirección de Estrategia - Documentos\Snies\proyectoMejora1"

# 3. Inicializar Git
git init

# 4. Agregar archivos
git add .

# 5. Commit inicial
git commit -m "Initial commit"

# 6. Conectar con GitHub (reemplaza la URL)
git remote add origin https://github.com/USUARIO/REPOSITORIO.git

# 7. Cambiar a main
git branch -M main

# 8. Subir
git push -u origin main
```

¡Listo! Tu proyecto debería estar en GitHub. 🚀

