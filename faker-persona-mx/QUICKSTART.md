# 🚀 Inicio Rápido - Faker Persona MX

## Instalación en 30 segundos

```bash
# 1. Clonar repositorio
cd /mnt/c/Users/pedro/Documents/code/faker_persona

# 2. Crear entorno virtual (recomendado)
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 3. Instalar paquete
pip install -e .
```

## Uso Inmediato

### CLI (Línea de Comandos)

```bash
# Ver ayuda
faker-persona-mx --help

# Generar 5 personas y mostrar en pantalla
faker-persona-mx generate 5

# Generar 100 personas y exportar a CSV
faker-persona-mx generate 100 --output personas.csv

# Generar con semilla específica (reproducible)
faker-persona-mx generate 50 --output datos.json --seed 42

# Ver información de datasets
faker-persona-mx info

# Ver versión
faker-persona-mx version
```

### Python (Como Librería)

```python
from faker_persona_mx import PersonaGenerator

# Crear generador
gen = PersonaGenerator(seed=42)

# Generar una persona
persona = gen.generate_one()
print(persona.nombre_completo())  # Nombre completo
print(persona.email)               # Email
print(persona.telefono)            # Teléfono (10 dígitos)
print(persona.curp)                # CURP (18 caracteres)
print(persona.rfc)                 # RFC (13 caracteres)

# Generar 100 personas
personas = gen.generate_batch(100)

# Exportar a CSV
gen.export_to_csv(personas, "personas.csv")

# Exportar a JSON
gen.export_to_json(personas, "personas.json")

# Convertir a pandas DataFrame
df = gen.to_dataframe(personas)
print(df.head())
```

## Ejemplos Listos para Usar

```bash
# Ejecutar ejemplo básico
python examples/basic_usage.py

# Ejecutar ejemplo con pandas
python examples/pandas_integration.py

# Ejecutar test de librería
python test_library.py
```

## Desarrollo

```bash
# Instalar con dependencias de desarrollo
pip install -e ".[dev]"

# Ejecutar tests
pytest

# Ejecutar tests con cobertura
pytest --cov

# Formatear código
black src/ tests/

# Linting
ruff check src/ tests/

# Type checking
mypy src/
```

## Docker

```bash
# Construir imagen
docker build -t faker-persona-mx .

# Ejecutar
docker run --rm faker-persona-mx version

# Generar y guardar
docker run --rm -v $(pwd)/output:/app/output \
  faker-persona-mx generate 100 -o /app/output/personas.csv

# Usar docker-compose
docker-compose up
```

## Características Principales

✅ **Datos realistas mexicanos**

- Nombres y apellidos auténticos
- CURP válidas (18 caracteres)
- RFC válidos (13 caracteres)
- Teléfonos con ladas reales de México
- Emails con dominios personalizables

✅ **Reproducible**

- Usa semillas para generar los mismos datos
- Ideal para tests automatizados
- Caché inteligente para rendimiento

✅ **Flexible**

- Uso como CLI o librería Python
- Exportación a CSV, JSON
- Integración con pandas
- Type hints completos
- Validaciones con Pydantic

✅ **Production Ready**

- Tests unitarios
- CI/CD con GitHub Actions
- Logging profesional
- Manejo de errores
- Documentación completa

## Estructura de una Persona Generada

```json
{
    "nombre": "Juan Carlos",
    "apellido_paterno": "García",
    "apellido_materno": "López",
    "email": "juan.carlos@example.com",
    "telefono": "5512345678",
    "curp": "GALJ850815HDFRPN09",
    "rfc": "GALJ850815ABC",
    "fecha_nacimiento": null,
    "sexo": null,
    "estado_nacimiento": null
}
```

## Soporte

- 📖 Documentación completa: [README.md](README.md)
- 🔧 Resumen técnico: [REFACTORING_SUMMARY.md](REFACTORING_SUMMARY.md)
- 🐛 Issues: GitHub Issues
- 📧 Contacto: @watermarkero

---

**¡Listo para usar en 30 segundos!** 🎉
