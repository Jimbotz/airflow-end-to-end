# Guía de Publicación - Faker Persona MX

Esta guía describe cómo publicar el paquete en PyPI para que otros puedan instalarlo con `pip install faker-persona-mx`.

## Pre-requisitos

1. Cuenta en PyPI: https://pypi.org/account/register/
2. Cuenta en TestPyPI (para pruebas): https://test.pypi.org/account/register/
3. Herramientas instaladas:
    ```bash
    pip install build twine
    ```

## Paso 1: Preparar el Paquete

### 1.1 Verificar Versión

Actualizar versión en `pyproject.toml` y `src/faker_persona_mx/__init__.py`:

```python
# src/faker_persona_mx/__init__.py
__version__ = "2.0.0"  # Actualizar según Semantic Versioning
```

```toml
# pyproject.toml
[project]
version = "2.0.0"  # Debe coincidir
```

### 1.2 Actualizar CHANGELOG.md

Agregar entrada para la nueva versión con todos los cambios.

### 1.3 Limpiar Builds Anteriores

```bash
rm -rf dist/ build/ *.egg-info
```

### 1.4 Ejecutar Tests

```bash
# Todos los tests deben pasar
pytest

# Verificar cobertura
pytest --cov

# Linting
black src/ tests/ --check
ruff check src/ tests/

# Type checking
mypy src/
```

## Paso 2: Construir el Paquete

```bash
# Construir distribuciones
python -m build

# Esto crea:
# dist/faker_persona_mx-2.0.0-py3-none-any.whl  (wheel)
# dist/faker-persona-mx-2.0.0.tar.gz             (source)
```

## Paso 3: Verificar el Build

```bash
# Verificar con twine
twine check dist/*

# Debe mostrar: "Checking dist/... PASSED"
```

## Paso 4: Probar en TestPyPI (Recomendado)

### 4.1 Subir a TestPyPI

```bash
# Configurar token de TestPyPI
# Crear en: https://test.pypi.org/manage/account/token/

# Subir
twine upload --repository testpypi dist/*

# Te pedirá:
# Username: __token__
# Password: tu-token-de-testpypi
```

### 4.2 Probar Instalación desde TestPyPI

```bash
# Crear entorno limpio
python -m venv test_env
source test_env/bin/activate

# Instalar desde TestPyPI
pip install --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    faker-persona-mx

# Probar
python -c "from faker_persona_mx import PersonaGenerator; print('OK')"
faker-persona-mx version

# Limpiar
deactivate
rm -rf test_env
```

## Paso 5: Publicar en PyPI (Producción)

### 5.1 Crear Token de PyPI

1. Ir a: https://pypi.org/manage/account/token/
2. Crear nuevo token con scope: "Entire account" o específico del proyecto
3. Guardar el token de forma segura

### 5.2 Subir a PyPI

```bash
# Subir a PyPI real
twine upload dist/*

# Username: __token__
# Password: tu-token-de-pypi
```

### 5.3 Verificar Publicación

```bash
# Debería estar disponible en:
# https://pypi.org/project/faker-persona-mx/

# Probar instalación
pip install faker-persona-mx
```

## Paso 6: Crear Release en GitHub

1. Ir a: https://github.com/pedro-aaron/faker-persona-mx/releases/new
2. Tag: `v2.0.0`
3. Title: `Release 2.0.0`
4. Description: Copiar del CHANGELOG.md
5. Adjuntar archivos de `dist/`:
    - `faker_persona_mx-2.0.0-py3-none-any.whl`
    - `faker-persona-mx-2.0.0.tar.gz`
6. Publicar release

## Paso 7: Post-Publicación

### 7.1 Actualizar README

Agregar badge de PyPI:

```markdown
[![PyPI version](https://badge.fury.io/py/faker-persona-mx.svg)](https://pypi.org/project/faker-persona-mx/)
[![Python Version](https://img.shields.io/pypi/pyversions/faker-persona-mx.svg)](https://pypi.org/project/faker-persona-mx/)
[![Downloads](https://pepy.tech/badge/faker-persona-mx)](https://pepy.tech/project/faker-persona-mx)
```

### 7.2 Actualizar Documentación

````markdown
## Instalación

```bash
pip install faker-persona-mx
```
````

### 7.3 Anunciar Release

- Twitter/X
- LinkedIn
- Reddit (r/Python, r/learnpython)
- Dev.to
- Python Weekly
- Python Discord servers

## Automatización con GitHub Actions

Para automatizar la publicación, crear `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
    release:
        types: [published]

jobs:
    deploy:
        runs-on: ubuntu-latest
        steps:
            - uses: actions/checkout@v3

            - name: Set up Python
              uses: actions/setup-python@v4
              with:
                  python-version: "3.11"

            - name: Install dependencies
              run: |
                  python -m pip install --upgrade pip
                  pip install build twine

            - name: Build package
              run: python -m build

            - name: Publish to PyPI
              env:
                  TWINE_USERNAME: __token__
                  TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
              run: twine upload dist/*
```

Agregar `PYPI_API_TOKEN` en GitHub Secrets:

- Settings → Secrets and variables → Actions → New repository secret

## Troubleshooting

### Error: "File already exists"

```bash
# Incrementar versión en pyproject.toml y __init__.py
# PyPI no permite re-subir la misma versión
```

### Error: "Invalid distribution"

```bash
# Verificar MANIFEST.in incluye todos los archivos necesarios
# Verificar pyproject.toml tiene configuración correcta
twine check dist/*
```

### Error: "Long description render failed"

```bash
# Verificar README.md tiene markdown válido
# Probar renderizado local:
python -m readme_renderer README.md
```

## Checklist de Publicación

Antes de publicar, verificar:

- [ ] Todos los tests pasan
- [ ] Versión actualizada en 2 lugares
- [ ] CHANGELOG.md actualizado
- [ ] README.md actualizado
- [ ] LICENSE incluido
- [ ] MANIFEST.in correcto
- [ ] Build creado y verificado
- [ ] Probado en TestPyPI
- [ ] Token de PyPI configurado
- [ ] Git tag creado
- [ ] GitHub release preparado

## Referencias

- [Python Packaging Guide](https://packaging.python.org/)
- [Twine Documentation](https://twine.readthedocs.io/)
- [PyPI Help](https://pypi.org/help/)
- [Semantic Versioning](https://semver.org/)

---

**¡Buena suerte con tu primera publicación en PyPI!** 🚀
