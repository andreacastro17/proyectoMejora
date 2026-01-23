# Script para inicializar el repositorio Git
# Ejecutar este script desde el directorio del proyecto

Write-Host "Inicializando repositorio Git..." -ForegroundColor Green

# Inicializar Git
git init

# Agregar todos los archivos
Write-Host "Agregando archivos al staging..." -ForegroundColor Green
git add .

# Hacer commit inicial
Write-Host "Creando commit inicial..." -ForegroundColor Green
git commit -m "Initial commit"

Write-Host "`nRepositorio Git inicializado correctamente!" -ForegroundColor Green
Write-Host "`nPara conectar con GitHub, ejecuta:" -ForegroundColor Yellow
Write-Host "  git remote add origin <URL_DEL_REPOSITORIO>" -ForegroundColor Cyan
Write-Host "  git branch -M main" -ForegroundColor Cyan
Write-Host "  git push -u origin main" -ForegroundColor Cyan

