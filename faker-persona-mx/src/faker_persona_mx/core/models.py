"""
Modelos de datos para Faker Persona MX usando Pydantic.
"""

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, field_validator, EmailStr


class Persona(BaseModel):
    """Modelo de una persona mexicana ficticia con datos coherentes."""

    nombre: str = Field(..., min_length=2, max_length=100, description="Nombre(s) de la persona")
    apellido_paterno: str = Field(..., min_length=2, max_length=50, description="Apellido paterno")
    apellido_materno: str = Field(..., min_length=2, max_length=50, description="Apellido materno")
    email: EmailStr = Field(..., description="Correo electrónico")
    telefono: str = Field(..., pattern=r"^\d{10}$", description="Teléfono móvil de 10 dígitos")
    ciudad: str = Field(..., min_length=2, description="Ciudad del teléfono")
    curp: str = Field(..., pattern=r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$", description="CURP de 18 caracteres")
    rfc: str = Field(..., pattern=r"^[A-Z]{4}\d{6}[0-9A-Z]{3}$", description="RFC de 13 caracteres")
    fecha_nacimiento: date = Field(..., description="Fecha de nacimiento (extraída de CURP)")
    sexo: str = Field(..., description="Sexo: Hombre o Mujer (extraído de CURP)")
    estado_nacimiento: str = Field(..., description="Estado de nacimiento (extraído de CURP)")

    @field_validator("nombre", "apellido_paterno", "apellido_materno")
    @classmethod
    def validate_name_format(cls, v: str) -> str:
        """Valida que los nombres no contengan caracteres especiales."""
        if not v.replace(" ", "").replace("-", "").isalpha():
            raise ValueError("Los nombres solo pueden contener letras, espacios y guiones")
        return v.strip().title()

    @field_validator("curp")
    @classmethod
    def validate_curp(cls, v: str) -> str:
        """Valida el formato de la CURP."""
        if len(v) != 18:
            raise ValueError("La CURP debe tener exactamente 18 caracteres")
        return v.upper()

    @field_validator("rfc")
    @classmethod
    def validate_rfc(cls, v: str) -> str:
        """Valida el formato del RFC."""
        if len(v) != 13:
            raise ValueError("El RFC debe tener exactamente 13 caracteres")
        return v.upper()

    def nombre_completo(self) -> str:
        """Retorna el nombre completo de la persona."""
        return f"{self.nombre} {self.apellido_paterno} {self.apellido_materno}"

    class Config:
        """Configuración del modelo."""
        json_schema_extra = {
            "example": {
                "nombre": "Juan Carlos",
                "apellido_paterno": "García",
                "apellido_materno": "López",
                "email": "juan.carlos@example.com",
                "telefono": "5512345678",
                "ciudad": "Ciudad de México",
                "curp": "GALJ850815HDFRRN09",
                "rfc": "GALJ850815ABC",
                "fecha_nacimiento": "1985-08-15",
                "sexo": "Hombre",
                "estado_nacimiento": "Ciudad de México"
            }
        }


class GeneratorConfig(BaseModel):
    """Configuración para los generadores de datos."""

    seed: int = Field(default=0, ge=0, description="Semilla para reproducibilidad")
    cache_enabled: bool = Field(default=True, description="Habilitar caché de datasets")
    data_path: Optional[str] = Field(default=None, description="Ruta personalizada a datasets")

    class Config:
        """Configuración del modelo."""
        frozen = True  # Inmutable
