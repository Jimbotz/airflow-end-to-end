# Dockerfile para Faker Persona MX

FROM python:3.11-slim

# Metadata
LABEL maintainer="Faker Persona MX Team"
LABEL description="Generador de datos ficticios de personas mexicanas"
LABEL version="2.0.0"

# Variables de entorno
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY src/ ./src/
COPY pyproject.toml .
COPY README.md .

# Instalar paquete
RUN pip install -e .

# Crear directorios necesarios
RUN mkdir -p /app/output /app/src/faker_persona_mx/data/cache

# Volumen para datos generados
VOLUME ["/app/output"]

# Punto de entrada
ENTRYPOINT ["faker-persona-mx"]
CMD ["--help"]

# Ejemplos de uso:
# docker build -t faker-persona-mx .
# docker run --rm faker-persona-mx version
# docker run --rm -v $(pwd)/output:/app/output faker-persona-mx generate 100 -o /app/output/personas.csv
