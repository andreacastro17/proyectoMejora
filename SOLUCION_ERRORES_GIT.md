# Solución de Errores al Subir a GitHub

## Problema Principal Identificado

El repositorio Git se inicializó en el directorio home del usuario (`C:\Users\andre`) en lugar del directorio del proyecto. Esto causa que Git intente rastrear **todos los archivos del usuario**, lo cual genera errores y problemas.

## Errores Comunes y Soluciones

### 1. Error: "fatal: not a git repository"
**Causa:** El repositorio no está inicializado en el directorio correcto.

**Solución:**
```powershell
# Navega al directorio del proyecto primero
cd "ruta\a\tu\proyecto"

# Inicializa el repositorio
git init
```

### 2. Error: Archivos demasiado grandes (>100MB)
**Causa:** GitHub tiene un límite de 100MB por archivo. Archivos como modelos (.pkl), ejecutables, o archivos de build pueden exceder este límite.

**Solución:** El `.gitignore` ya está configurado para ignorar:
- `*.pkl` (modelos)
- `build/` y `dist/` (archivos compilados)
- `*.exe` (ejecutables)
- `logs/` (archivos de log)
- `outputs/` (archivos generados)

### 3. Error: "Permission denied" o problemas de autenticación
**Causa:** No tienes configuradas las credenciales de GitHub o el token de acceso.

**Solución:**
```powershell
# Configurar usuario
git config --global user.name "Tu Nombre"
git config --global user.email "tu.email@ejemplo.com"

# Para autenticación, usa un Personal Access Token (PAT)
# Ve a GitHub > Settings > Developer settings > Personal access tokens
```

### 4. Error: Repositorio inicializado en lugar incorrecto
**Causa:** Se ejecutó `git init` en el directorio equivocado.

**Solución:**
```powershell
# Elimina el .git del directorio incorrecto
Remove-Item -Path .git -Recurse -Force

# Navega al directorio correcto del proyecto
cd "ruta\a\tu\proyecto"

# Inicializa el repositorio correctamente
git init
```

## Pasos Correctos para Subir a GitHub

### Paso 1: Asegúrate de estar en el directorio correcto
```powershell
# Verifica que estás en el directorio del proyecto
# Debe contener archivos como .gitignore, app/, etl/, etc.
Get-Location
```

### Paso 2: Inicializa el repositorio (si no está inicializado)
```powershell
git init
```

### Paso 3: Agrega los archivos
```powershell
git add .
```

### Paso 4: Haz el primer commit
```powershell
git commit -m "Initial commit"
```

### Paso 5: Conecta con GitHub
```powershell
# Reemplaza <URL> con la URL de tu repositorio en GitHub
git remote add origin https://github.com/usuario/repositorio.git
```

### Paso 6: Cambia la rama a 'main' (si es necesario)
```powershell
git branch -M main
```

### Paso 7: Sube los archivos
```powershell
git push -u origin main
```

## Verificación del .gitignore

El `.gitignore` actualizado incluye:
- ✅ Entornos virtuales (venv, .venv)
- ✅ Archivos de build (build/, dist/, *.exe)
- ✅ Modelos grandes (*.pkl, *.pt, *.bin)
- ✅ Archivos de log (logs/, *.log)
- ✅ Outputs generados (outputs/, *.xlsx, *.docx)
- ✅ Archivos temporales

## Si Persisten los Errores

1. **Verifica el tamaño de los archivos:**
   ```powershell
   Get-ChildItem -Recurse | Where-Object {$_.Length -gt 100MB} | Select-Object FullName, @{Name="Size(MB)";Expression={[math]::Round($_.Length/1MB,2)}}
   ```

2. **Verifica qué archivos están siendo rastreados:**
   ```powershell
   git status
   ```

3. **Si hay archivos grandes ya rastreados, elimínalos del historial:**
   ```powershell
   git rm --cached archivo_grande.pkl
   git commit -m "Remove large file"
   ```

## Script Automatizado

Usa el script `setup_git.ps1` para configurar el repositorio automáticamente:
```powershell
.\setup_git.ps1
```

