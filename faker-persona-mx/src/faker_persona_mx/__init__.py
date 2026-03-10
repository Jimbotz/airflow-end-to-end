"""
Faker Persona MX - Generador de datos ficticios de personas mexicanas.

Este paquete proporciona generadores de datos ficticios específicos para México,
incluyendo nombres, CURP, RFC, teléfonos y correos electrónicos.
"""

__version__ = "2.0.0"
__author__ = "watermarkero"

from .core.persona import PersonaGenerator
from .core.models import Persona

__all__ = ["PersonaGenerator", "Persona"]
