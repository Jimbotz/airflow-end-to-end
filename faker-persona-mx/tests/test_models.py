"""
Tests para los modelos de datos.
"""

import pytest
from pydantic import ValidationError
from faker_persona_mx.core.models import Persona, GeneratorConfig


class TestPersona:
    """Tests para el modelo Persona."""

    def test_persona_valid(self):
        """Test creación de persona válida."""
        from datetime import date
        persona = Persona(
            nombre="Juan Carlos",
            apellido_paterno="García",
            apellido_materno="López",
            email="juan.carlos@example.com",
            telefono="5512345678",
            ciudad="México",
            curp="GALJ850815HDFRPN09",
            rfc="GALJ850815ABC",
            fecha_nacimiento=date(1985, 8, 15),
            sexo="Hombre",
            estado_nacimiento="Ciudad de México"
        )
        assert persona.nombre == "Juan Carlos"
        assert persona.apellido_paterno == "García"
        assert persona.apellido_materno == "López"

    def test_persona_nombre_completo(self):
        """Test método nombre_completo."""
        from datetime import date
        persona = Persona(
            nombre="María",
            apellido_paterno="Hernández",
            apellido_materno="Martínez",
            email="maria@test.com",
            telefono="5587654321",
            ciudad="México",
            curp="HEMM900101MDFRRR08",
            rfc="HEMM900101XYZ",
            fecha_nacimiento=date(1990, 1, 1),
            sexo="Mujer",
            estado_nacimiento="Ciudad de México"
        )
        assert persona.nombre_completo() == "María Hernández Martínez"

    def test_persona_invalid_email(self):
        """Test validación de email inválido."""
        from datetime import date
        with pytest.raises(ValidationError):
            Persona(
                nombre="Test",
                apellido_paterno="Test",
                apellido_materno="Test",
                email="invalid-email",
                telefono="5512345678",
                ciudad="México",
                curp="TETF850815HDFRPN09",
                rfc="TETF850815ABC",
                fecha_nacimiento=date(1985, 8, 15),
                sexo="Hombre",
                estado_nacimiento="Ciudad de México"
            )

    def test_persona_invalid_telefono(self):
        """Test validación de teléfono inválido."""
        from datetime import date
        with pytest.raises(ValidationError):
            Persona(
                nombre="Test",
                apellido_paterno="Test",
                apellido_materno="Test",
                email="test@test.com",
                telefono="123",  # Muy corto
                ciudad="México",
                curp="TETF850815HDFRPN09",
                rfc="TETF850815ABC",
                fecha_nacimiento=date(1985, 8, 15),
                sexo="Hombre",
                estado_nacimiento="Ciudad de México"
            )

    def test_persona_invalid_curp_length(self):
        """Test validación de CURP con longitud incorrecta."""
        from datetime import date
        with pytest.raises(ValidationError):
            Persona(
                nombre="Test",
                apellido_paterno="Test",
                apellido_materno="Test",
                email="test@test.com",
                telefono="5512345678",
                ciudad="México",
                curp="CURP_CORTA",  # Muy corta
                rfc="TETF850815ABC",
                fecha_nacimiento=date(1985, 8, 15),
                sexo="Hombre",
                estado_nacimiento="Ciudad de México"
            )

    def test_persona_invalid_rfc_length(self):
        """Test validación de RFC con longitud incorrecta."""
        from datetime import date
        with pytest.raises(ValidationError):
            Persona(
                nombre="Test",
                apellido_paterno="Test",
                apellido_materno="Test",
                email="test@test.com",
                telefono="5512345678",
                ciudad="México",
                curp="TETF850815HDFRPN09",
                rfc="RFC_CORTO",  # Muy corto
                fecha_nacimiento=date(1985, 8, 15),
                sexo="Hombre",
                estado_nacimiento="Ciudad de México"
            )


class TestGeneratorConfig:
    """Tests para GeneratorConfig."""

    def test_config_defaults(self):
        """Test valores por defecto de configuración."""
        config = GeneratorConfig()
        assert config.seed == 0
        assert config.cache_enabled is True

    def test_config_custom_values(self):
        """Test configuración con valores personalizados."""
        config = GeneratorConfig(seed=42, cache_enabled=False)
        assert config.seed == 42
        assert config.cache_enabled is False

    def test_config_immutable(self):
        """Test que la configuración es inmutable."""
        config = GeneratorConfig(seed=42)
        with pytest.raises(ValidationError):
            config.seed = 100  # No debería permitir cambios
