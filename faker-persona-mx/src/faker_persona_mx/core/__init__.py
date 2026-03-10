"""Módulo core con la lógica principal del generador."""

from .models import Persona, GeneratorConfig
from .persona import PersonaGenerator

__all__ = ["Persona", "GeneratorConfig", "PersonaGenerator"]
