"""
Configuración y fixtures compartidos para tests.
"""

import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def temp_data_dir():
    """Crea un directorio temporal para datos de prueba."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_nombres():
    """Lista de nombres de ejemplo."""
    return ["Juan", "María", "Pedro", "Ana", "Luis", "Carmen"]


@pytest.fixture
def sample_apellidos():
    """Lista de apellidos de ejemplo."""
    return ["García", "Hernández", "López", "Martínez", "González", "Rodríguez"]


@pytest.fixture
def sample_email_domains():
    """Lista de dominios de email de ejemplo."""
    return ["example.com", "test.com", "demo.com"]


@pytest.fixture
def sample_ladas():
    """Diccionario de ladas de ejemplo."""
    return {
        "Ciudad de México": ["55", "56"],
        "Jalisco": ["33"],
        "Nuevo León": ["81", "82"]
    }
