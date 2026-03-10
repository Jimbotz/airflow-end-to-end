"""
Tests para los generadores individuales.
"""

import pytest
from faker_persona_mx.generators import (
    BaseConverter,
    NameGenerator,
    EmailGenerator,
    CurpGenerator,
    RfcGenerator,
    PhoneGenerator
)


class TestBaseConverter:
    """Tests para BaseConverter."""

    def test_convert_zero(self):
        """Test conversión de cero."""
        converter = BaseConverter(10)
        assert converter.convert(0) == [0]

    def test_convert_decimal_to_binary(self):
        """Test conversión decimal a binario."""
        converter = BaseConverter(2)
        assert converter.convert(5) == [1, 0, 1]
        assert converter.convert(8) == [1, 0, 0, 0]

    def test_convert_decimal_to_base_10(self):
        """Test conversión en base 10."""
        converter = BaseConverter(10)
        assert converter.convert(123) == [1, 2, 3]

    def test_invalid_base(self):
        """Test base inválida."""
        with pytest.raises(ValueError):
            BaseConverter(1)


class TestNameGenerator:
    """Tests para NameGenerator."""

    @pytest.fixture
    def name_list(self):
        return ["Juan", "María", "Pedro", "Ana"]

    def test_generate_name(self, name_list):
        """Test generación de nombre."""
        generator = NameGenerator(name_list, separator=" ")
        name = generator.get(0)
        assert isinstance(name, str)
        assert len(name) > 0

    def test_generate_multiple_names(self, name_list):
        """Test generación de múltiples nombres."""
        generator = NameGenerator(name_list)
        names = list(generator.generate(5))
        assert len(names) == 5
        assert all(isinstance(name, str) for name in names)

    def test_empty_name_list(self):
        """Test con lista vacía debe fallar."""
        with pytest.raises(ValueError):
            NameGenerator([])


class TestEmailGenerator:
    """Tests para EmailGenerator."""

    @pytest.fixture
    def email_names(self):
        return ["juan", "maria", "pedro"]

    @pytest.fixture
    def domains(self):
        return ["example.com", "test.com"]

    def test_generate_email(self, email_names, domains):
        """Test generación de email."""
        generator = EmailGenerator(email_names, domains)
        email = generator.get(0)
        assert "@" in email
        assert any(domain in email for domain in domains)

    def test_generate_multiple_emails(self, email_names, domains):
        """Test generación de múltiples emails."""
        generator = EmailGenerator(email_names, domains)
        emails = list(generator.generate(5))
        assert len(emails) == 5
        assert all("@" in email for email in emails)

    def test_empty_email_list(self):
        """Test con lista vacía debe fallar."""
        with pytest.raises(ValueError):
            EmailGenerator([])


class TestCurpGenerator:
    """Tests para CurpGenerator."""

    def test_generate_curp_format(self):
        """Test formato de CURP generada."""
        generator = CurpGenerator(seed=42)
        curp = generator.generate_curp(
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )
        assert len(curp) == 18
        assert curp.isalnum()
        assert curp.isupper()
        # Verificar que los primeros caracteres correspondan a los nombres
        assert curp[0] == "G"  # Primera letra de García
        assert curp[1] == "A"  # Primera vocal interna de García
        assert curp[2] == "L"  # Primera letra de López
        assert curp[3] == "J"  # Primera letra de Juan

    def test_generate_curp_with_seed(self):
        """Test reproducibilidad con semilla."""
        generator1 = CurpGenerator(seed=42)
        generator2 = CurpGenerator(seed=42)

        curp1 = generator1.generate_curp(
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )
        curp2 = generator2.generate_curp(
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )

        assert curp1 == curp2

    def test_generate_curp_without_names(self):
        """Test generación de CURP sin nombres (fallback)."""
        generator = CurpGenerator(seed=42)
        curp = generator.generate_curp()
        assert len(curp) == 18
        assert curp.isalnum()

    def test_generate_multiple_curps(self):
        """Test generación de múltiples CURPs."""
        generator = CurpGenerator(seed=42)
        curps = list(generator.generate(
            10,
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        ))
        assert len(curps) == 10
        assert all(len(curp) == 18 for curp in curps)
        # Todos deben tener los mismos caracteres iniciales
        assert all(curp[:4] == "GALJ" for curp in curps)


class TestRfcGenerator:
    """Tests para RfcGenerator."""


class TestPhoneGenerator:
    """Tests para PhoneGenerator."""

    @pytest.fixture
    def simple_ladas(self):
        # incluye lada de 2 y 3 dígitos
        return {"Estado1": ["55", "123"], "Estado2": ["444"]}

    def test_phone_list_length_and_format(self, simple_ladas):
        generator = PhoneGenerator(simple_ladas, seed=1)
        # generar algunos números
        phones = list(generator.generate(5))
        assert all(len(p) == 10 for p in phones)
        # verificar que los que usan lada de 2 dígitos estén bien
        assert any(p.startswith("55") for p in phones)

    def test_generate_single_format(self, simple_ladas):
        generator = PhoneGenerator(simple_ladas, seed=2)
        number = generator.generate_single()
        assert len(number) == 10
        assert number.isdigit()

    def test_index_out_of_range(self, simple_ladas):
        generator = PhoneGenerator(simple_ladas, seed=3)
        length = len(generator.phone_list)
        with pytest.raises(IndexError):
            generator.get(length)  # índice igual a largo debería fallar

    def test_request_too_many(self, simple_ladas):
        generator = PhoneGenerator(simple_ladas, seed=4)
        total = len(generator.phone_list)
        with pytest.raises(ValueError):
            list(generator.generate(total + 1))


class TestRfcGenerator:
    """Tests para RfcGenerator."""

    def test_generate_rfc_format(self):
        """Test formato de RFC generado."""
        from datetime import date
        generator = RfcGenerator(seed=42)
        rfc = generator.generate_rfc(
            fecha_nacimiento=date(1985, 8, 15),
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )
        assert len(rfc) == 13
        assert rfc.isalnum()
        assert rfc.isupper()
        # Verificar formato: GALJ850815RLJ
        assert rfc[:4] == "GALJ"  # Primeros 4 caracteres
        assert rfc[4:10] == "850815"  # Fecha

    def test_generate_rfc_with_seed(self):
        """Test reproducibilidad con semilla."""
        from datetime import date
        generator1 = RfcGenerator(seed=42)
        generator2 = RfcGenerator(seed=42)

        fecha = date(1985, 8, 15)
        rfc1 = generator1.generate_rfc(
            fecha_nacimiento=fecha,
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )
        rfc2 = generator2.generate_rfc(
            fecha_nacimiento=fecha,
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        )

        assert rfc1 == rfc2

    def test_generate_multiple_rfcs(self):
        """Test generación de múltiples RFCs."""
        from datetime import date
        generator = RfcGenerator(seed=42)
        rfcs = list(generator.generate(
            10,
            apellido_paterno="García",
            apellido_materno="López",
            nombre="Juan"
        ))
        assert len(rfcs) == 10
        assert all(len(rfc) == 13 for rfc in rfcs)
        rfcs = list(generator.generate(10))
        assert len(rfcs) == 10
        assert all(len(rfc) == 13 for rfc in rfcs)
