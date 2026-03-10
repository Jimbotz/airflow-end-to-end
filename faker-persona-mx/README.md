# Faker Persona MX

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Generador profesional de datos ficticios de personas mexicanas para testing, desarrollo y demostración.

## Dataset Gigante

- Más de 41,000 nombres mexicanos comunes
- Más de 17,000 apellidos paternos | maternos
- Más de 11,000 dominios de correo electrónico reales
- Todas las ladas telefónicas de México
- 32 estados con códigos CURP actualizados

Total de combinaciones posibles: 41,000 x 17,000 x 17,000 x 32 = **380 billones de personas únicas**

## Características

✨ **Datos Realistas**

- Nombres y apellidos auténticos mexicanos
- CURP válidas (Clave Única de Registro de Población)
- RFC válidos (Registro Federal de Contribuyentes)
- Números telefónicos con ladas reales de México
- Correos electrónicos con dominios personalizables

🚀 **Producción Ready**

- Type hints completos (PEP 484)
- Validaciones con Pydantic
- Logging profesional
- Sistema de caché inteligente
- CLI poderoso con Typer
- 100% testeado con pytest

🎯 **Reproducible**

- Semillas configurables para datos consistentes
- Ideal para testing automatizado
- Datasets pre-barajados en caché

## Instalación

### Desde PyPI

```bash
pip install faker-persona-mx
```

### Desde código fuente

```bash
git clone https://github.com/pedro-aaron/faker-persona-mx.git
cd faker-persona-mx

python3 -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate

pip install -e .
```

### Para desarrollo

```bash
pip install -e ".[dev]"
```

## Uso Rápido

### Como librería Python

```python
from faker_persona_mx import PersonaGenerator

# Inicializar generador
generator = PersonaGenerator(seed=42)

# Generar una persona
persona = generator.generate_one()
print(persona.nombre_completo())  # "Juan Carlos García López"
print(persona.curp)               # "GALJ850815HDFRPN09"
print(persona.rfc)                # "GALJ850815ABC"
print(persona.email)              # "juan.carlos@example.com"
print(persona.telefono)           # "5512345678"

# Generar múltiples personas
personas = generator.generate_batch(100)

# Exportar a CSV
generator.export_to_csv(personas, "personas.csv")

# Exportar a JSON
generator.export_to_json(personas, "personas.json")

# Convertir a DataFrame
df = generator.to_dataframe(personas)
```

### Desde la línea de comandos

```bash
# Generar 100 personas y mostrar en tabla
faker-persona-mx generate 100 --format table

# Generar 500 personas y exportar a CSV
faker-persona-mx generate 500 -o personas.csv -f csv

# Generar con semilla específica para reproducibilidad
faker-persona-mx generate 50 --seed 42 -o datos.json

# Generar solo teléfonos de ciertos estados
faker-persona-mx generate 100 --estados "Ciudad de México,Jalisco" -o cdmx_jalisco.csv

# Ver información de datasets
faker-persona-mx info

# Ver versión
faker-persona-mx version

# Eliminar caché (con confirmación)
faker-persona-mx clear-cache

# Eliminar caché sin confirmación
faker-persona-mx clear-cache --force
```

## Ejemplos Avanzados

### Generar con configuración personalizada

```python
from faker_persona_mx import PersonaGenerator

# Deshabilitar caché y especificar estados
generator = PersonaGenerator(
    seed=123,
    cache_enabled=False,
    estados_activos=["Ciudad de México", "Nuevo León", "Jalisco"]
)

# Generación lazy con stream (memory efficient)
for persona in generator.generate_stream(10000):
    print(f"{persona.nombre_completo()} - {persona.email}")
```

### Integración con pandas

```python
import pandas as pd
from faker_persona_mx import PersonaGenerator

generator = PersonaGenerator(seed=42)
personas = generator.generate_batch(1000)

# Convertir a DataFrame
df = generator.to_dataframe(personas)

# Análisis de datos
print(df.describe())
print(df['telefono'].value_counts())

# Filtrar por criterios
df_cdmx = df[df['telefono'].str.startswith('55')]
```

### Testing automatizado

```python
import pytest
from faker_persona_mx import PersonaGenerator

@pytest.fixture
def personas_test():
    """Fixture con datos de prueba reproducibles."""
    generator = PersonaGenerator(seed=0)
    return generator.generate_batch(10)

def test_mi_funcion(personas_test):
    # Usar datos ficticios en tests
    resultado = mi_funcion_que_procesa_personas(personas_test)
    assert len(resultado) == 10
```

## Sistema de Seed (Reproducibilidad)

El sistema de **seed** (semilla) permite generar datos de manera **determinística y reproducible**, ideal para testing automatizado, debugging y datasets consistentes.

### ¿Cómo funciona?

La semilla controla el generador de números aleatorios utilizado para:

- Seleccionar nombres y apellidos de los datasets
- Elegir el orden de barajado de los datos
- Generar fechas de nacimiento
- Asignar estados y ladas telefónicas
- Calcular dígitos verificadores de CURP/RFC

**La misma semilla siempre produce exactamente los mismos datos.**

### Ejemplos de uso

#### Reproducibilidad básica

```python
from faker_persona_mx import PersonaGenerator

# Dos generadores con la misma semilla producen datos idénticos
gen1 = PersonaGenerator(seed=42)
gen2 = PersonaGenerator(seed=42)

persona1 = gen1.generate_one()
persona2 = gen2.generate_one()

assert persona1.nombre == persona2.nombre
assert persona1.curp == persona2.curp
# Todos los campos serán idénticos
```

#### Testing con datos consistentes

```python
import pytest
from faker_persona_mx import PersonaGenerator

class TestProcesadorPersonas:
    @pytest.fixture
    def dataset_prueba(self):
        """Siempre retorna las mismas 100 personas."""
        gen = PersonaGenerator(seed=123)
        return gen.generate_batch(100)

    def test_filtrado_por_estado(self, dataset_prueba):
        # El test siempre usará los mismos datos
        cdmx = [p for p in dataset_prueba if p.telefono.startswith('55')]
        assert len(cdmx) > 0

    def test_validacion_curp(self, dataset_prueba):
        # Datos reproducibles facilitan debugging
        for persona in dataset_prueba:
            assert len(persona.curp) == 18
```

#### Diferentes semillas para variedad

```python
from faker_persona_mx import PersonaGenerator

# Generar diferentes datasets para diferentes propósitos
gen_dev = PersonaGenerator(seed=1)      # Para desarrollo
gen_test = PersonaGenerator(seed=2)     # Para testing
gen_demo = PersonaGenerator(seed=3)     # Para demos

# Cada uno produce datos diferentes pero reproducibles
personas_dev = gen_dev.generate_batch(50)
personas_test = gen_test.generate_batch(100)
```

#### Seed desde línea de comandos

```bash
# Siempre genera los mismos 1000 registros
faker-persona-mx generate 1000 --seed 42 -o dataset_fijo.csv

# Útil para compartir datasets exactos entre equipos
faker-persona-mx generate 500 --seed 2024 -o equipo_qa.json
```

### Configuración de seed

#### Por defecto (variable de entorno)

```bash
# En .env
DEFAULT_SEED=12345
```

```python
# Usa el seed por defecto del .env
gen = PersonaGenerator()
```

#### Programáticamente

```python
from faker_persona_mx.utils.config import config

# Cambiar seed global
config.DEFAULT_SEED = 999

# Todos los generadores sin seed explícito usarán 999
gen = PersonaGenerator()
```

#### Seed dinámico (para datos aleatorios reales)

```python
import time

# Usar timestamp como seed para datos únicos cada vez
gen = PersonaGenerator(seed=int(time.time()))
personas = gen.generate_batch(100)
```

### Mejores prácticas

1. **Testing**: Usa semillas fijas (0, 42, 123) para tests reproducibles
2. **Desarrollo**: Usa semillas conocidas para debugging consistente
3. **Producción/Demos**: Usa semillas basadas en fecha/hora para variedad
4. **CI/CD**: Documenta las semillas usadas en pipelines
5. **Datasets compartidos**: Comparte tanto los datos como la semilla usada

## Sistema de Caché

El sistema de **caché** optimiza el rendimiento al pre-barajar y almacenar datasets en disco, evitando el shuffle costoso en cada ejecución.

### ¿Cómo funciona?

1. **Primera ejecución** con una semilla:
    - Carga los CSVs originales de nombres, apellidos, dominios
    - Los baraja usando la semilla especificada
    - Guarda los datasets barajados en `data/cache/` con hash único
    - Usa los datos barajados para generar personas

2. **Ejecuciones posteriores** con la misma semilla:
    - Detecta si ya existe caché para esa semilla
    - Carga directamente los datasets pre-barajados (mucho más rápido)
    - Genera personas sin necesidad de shuffle

### Estructura de caché

```
src/faker_persona_mx/data/cache/
├── nombres_seed_42_41234.csv
├── apellidos_paterno_seed_42_17123.csv
├── apellidos_materno_seed_42_17089.csv
└── dominios_seed_42_11456.csv
```

Formato: `{dataset}_seed_{seed}_{num_registros}.csv`

### Configuración de caché

#### Habilitar/Deshabilitar

```python
from faker_persona_mx import PersonaGenerator

# Con caché (por defecto, recomendado)
gen = PersonaGenerator(seed=42, cache_enabled=True)

# Sin caché (más lento, útil para desarrollo)
gen = PersonaGenerator(seed=42, cache_enabled=False)
```

#### Variables de entorno

```bash
# En .env
ENABLE_CACHE=true                # Habilitar caché globalmente
CACHE_SIZE_TOLERANCE=0.01        # Tolerancia de tamaño (1%)
```

### Tolerancia de tamaño de caché

El sistema valida que el caché tenga aproximadamente el mismo número de registros que el dataset original:

```python
# Si el dataset original tiene 41,000 nombres
# y CACHE_SIZE_TOLERANCE=0.01 (1%)
# El caché debe tener entre 40,590 y 41,410 registros
```

Esto previene usar cachés corruptos o incompletos.

### Gestión de caché

#### Limpiar caché con el CLI

```bash
# Eliminar caché con confirmación interactiva
faker-persona-mx clear-cache

# Eliminar caché sin confirmación (útil para scripts)
faker-persona-mx clear-cache --force
```

#### Limpiar caché manualmente

```bash
# Eliminar todos los archivos de caché
rm -rf src/faker_persona_mx/data/cache/*.csv
```

```python
import shutil
from pathlib import Path

# Desde Python
cache_dir = Path("src/faker_persona_mx/data/cache")
shutil.rmtree(cache_dir)
cache_dir.mkdir(exist_ok=True)
```

#### Verificar caché actual

```bash
# Ver archivos de caché
ls -lh src/faker_persona_mx/data/cache/

# Ver información del generador (incluye estado de caché)
faker-persona-mx info
```

### Ventajas del sistema de caché

1. **Performance**: 10-50x más rápido en ejecuciones posteriores
2. **Consistencia**: Garantiza mismos datos con misma semilla
3. **Disco vs RAM**: Usa espacio en disco (barato) para ahorrar CPU
4. **Automático**: Se gestiona sin intervención del usuario

### Desventajas y cuándo deshabilitarlo

- **Espacio en disco**: Cada semilla genera ~200MB de caché
- **Desarrollo de datasets**: Si modificas los CSVs originales, elimina el caché
- **Testing del shuffle**: Para probar el algoritmo de barajado

```python
# Deshabilitado para testing
gen = PersonaGenerator(seed=42, cache_enabled=False)
```

### Mejores prácticas

1. **Desarrollo normal**: Mantén caché habilitado (más rápido)
2. **CI/CD**: Considera deshabilitar caché en pipelines efímeros
3. **Modificación de datasets**: Limpia caché después de actualizar CSVs
4. **Espacio limitado**: Monitorea tamaño del directorio `cache/`
5. **Semillas múltiples**: Cada semilla crea su propio caché (considerar espacio)

## Estructura del Proyecto

```
faker-persona-mx/
├── src/
│   └── faker_persona_mx/
│       ├── core/           # Modelos y generador principal
│       ├── generators/     # Generadores individuales
│       ├── data/           # Datasets y loader
│       │   ├── datasets/   # CSVs con datos seed
│       │   └── cache/      # Caché de datasets barajados
│       └── utils/          # Configuración y logging
├── tests/                  # Suite de tests
├── examples/               # Ejemplos de uso
├── docs/                   # Documentación adicional
├── pyproject.toml         # Configuración del proyecto
└── README.md
```

## Datasets Incluidos

- **Nombres**: ~41,000 nombres mexicanos comunes
- **Apellidos**: ~17,000 apellidos paternos y maternos
- **Dominios Email**: ~11,000 dominios reales
- **Ladas México**: Todas las ladas telefónicas por estado
- **Estados**: 32 estados de México con códigos CURP

## Desarrollo

### Configurar entorno de desarrollo

```bash
# Clonar repositorio
git clone https://github.com/pedro-aaron/faker-persona-mx.git
cd faker-persona-mx

# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate

# Instalar dependencias de desarrollo
pip install -e ".[dev]"

# Instalar pre-commit hooks
pre-commit install
```

### Ejecutar tests

```bash
# Todos los tests
pytest

# Con cobertura
pytest --cov

# Tests específicos
pytest tests/test_generators.py -v
```

### Code Quality

```bash
# Formatear código
black src/ tests/

# Linting
ruff src/ tests/

# Type checking
mypy src/
```

## Configuración

### Variables de entorno

Crear archivo `.env` en la raíz del proyecto:

```env
# Logging
LOG_LEVEL=INFO
LOG_FILE=/tmp/faker_persona_mx.log

# Cache
ENABLE_CACHE=true
CACHE_SIZE_TOLERANCE=0.01

# Seed
DEFAULT_SEED=0
```

### Configuración programática

```python
from faker_persona_mx.utils.config import config

# Modificar configuración
config.DEFAULT_SEED = 42
config.ENABLE_CACHE = False
```

## Roadmap

### Próximas versiones

- [x] **v0.2.0**: Release inicial en PyPI
- [ ] **v0.3.0**: Asociación de nombres con sexo biológico
    - Clasificar nombres del dataset como masculinos, femeninos o unisex
    - Garantizar coherencia entre nombre y sexo en CURP
    - Permitir filtrado por sexo en generación
    - Estadísticas de distribución por sexo
- [ ] **v0.4.0**: Direcciones completas mexicanas
    - Calles, colonias, códigos postales reales
    - Integración con datos del SEPOMEX
    - Validación de direcciones por estado
- [ ] **v0.5.0**: Documentos de identidad
    - Generación de INE (credencial de elector)
    - Números de pasaporte mexicano
    - Licencias de conducir
- [ ] **v0.6.0**: Personas morales
    - Generación de empresas mexicanas (Razón Social)
    - RFC de personas morales
    - Giros empresariales

### Trabajo futuro

- [ ] API REST opcional con FastAPI
- [ ] Integración como provider de Faker library
- [ ] Dashboard web interactivo para generar datasets
- [ ] Exportación a formatos adicionales (Parquet, Avro, XML)
- [ ] Generación de relaciones familiares (padres, hijos, hermanos)
- [ ] Históricos de datos (cambios de domicilio, empleos)
- [ ] Plugin para pytest-fixtures
- [ ] Soporte para datos sensibles con encriptación

### Limitaciones conocidas

> **Importante**: En la versión actual (v0.2.0), los nombres no están asociados con el sexo biológico. Esto significa que:
>
> - Un nombre típicamente masculino puede asignarse con sexo 'M' o 'H' en la CURP de forma aleatoria
> - Un nombre típicamente femenino puede tener sexo 'H' en la CURP
> - La asignación de sexo es puramente aleatoria e independiente del nombre
>
> **Solución planificada**: La versión v0.3.0 incluirá un dataset clasificado de nombres por sexo, garantizando coherencia entre el nombre y el indicador de sexo en CURP/RFC.

## Contribuir

¡Las contribuciones son bienvenidas! Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add: amazing feature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## Licencia

Este proyecto está bajo la Licencia MIT. Ver archivo `LICENSE` para más detalles.

## Mantenedores

**Lead Developer**: [@pedro-aaron](https://github.com/pedro-aaron)

## Soporte

- 🐛 Issues: [GitHub Issues](https://github.com/pedro-aaron/faker-persona-mx/issues)
- 💡 Feature Requests: [GitHub Discussions](https://github.com/pedro-aaron/faker-persona-mx/discussions)
- 📖 Documentación: [Wiki del proyecto](https://github.com/pedro-aaron/faker-persona-mx/wiki)
- 📦 PyPI: [faker-persona-mx](https://pypi.org/project/faker-persona-mx/)

## Autor

**pedro-aaron**
GitHub: [@pedro-aaron](https://github.com/pedro-aaron)

Si este proyecto te resulta útil, considera darle una ⭐ en GitHub.

---

**Hecho con ❤️ en México 🇲🇽**
