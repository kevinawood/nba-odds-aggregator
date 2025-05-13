isort src/
black src/
ruff check src/ --fix
Write-Host "✅ Linting complete"
