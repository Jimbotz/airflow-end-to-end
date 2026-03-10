"""
Generadores de identificadores oficiales mexicanos (CURP, RFC).

Los generadores ahora están acoplados a los nombres reales de la persona
para seguir las reglas legales de formación de CURP y RFC.
"""

import random
from typing import Generator, Dict, Tuple, Optional
from datetime import datetime, timedelta, date
from dataclasses import dataclass


@dataclass
class CurpData:
    """Datos extraídos de una CURP."""
    curp: str
    fecha_nacimiento: date
    sexo: str
    estado_nacimiento: str
    codigo_estado: str
    nombre: str = ""
    apellido_paterno: str = ""
    apellido_materno: str = ""


class IdentifierGenerator:
    """Clase base para generar identificadores oficiales mexicanos."""

    # Constantes compartidas
    VOWELS = "AEIOU"
    CONSONANTS = "BCDFGHJKLMNPQRSTVWXYZ"
    CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    DIGITS = "1234567890"
    ALL_CHARS = CHARACTERS + DIGITS
    SEXO_OPTIONS = "HM"

    ESTADOS_MEXICO: Dict[str, str] = {
        "AS": "Aguascalientes",
        "BC": "Baja California",
        "BS": "Baja California Sur",
        "CC": "Campeche",
        "CS": "Chiapas",
        "CH": "Chihuahua",
        "DF": "Ciudad de México",
        "CL": "Coahuila",
        "CM": "Colima",
        "DG": "Durango",
        "GT": "Guanajuato",
        "GR": "Guerrero",
        "HG": "Hidalgo",
        "JC": "Jalisco",
        "MC": "Estado de México",
        "MN": "Michoacán",
        "MS": "Morelos",
        "NT": "Nayarit",
        "NL": "Nuevo León",
        "OC": "Oaxaca",
        "PL": "Puebla",
        "QO": "Querétaro",
        "QR": "Quintana Roo",
        "SP": "San Luis Potosí",
        "SL": "Sinaloa",
        "SR": "Sonora",
        "TC": "Tabasco",
        "TS": "Tamaulipas",
        "TL": "Tlaxcala",
        "VZ": "Veracruz",
        "YN": "Yucatán",
        "ZS": "Zacatecas",
    }

    def __init__(self, seed: int = 0):
        """
        Inicializa el generador.

        Args:
            seed: Semilla para reproducibilidad
        """
        self.seed = seed
        # Use un RNG independiente para evitar alterar el estado global
        self._rng = random.Random(self.seed)
        self.estados_list = list(self.ESTADOS_MEXICO.keys())

    def random_date_object(
        self,
        start_date: str = "1950-01-01",
        end_date: str = "2005-12-31"
    ) -> date:
        """
        Genera una fecha aleatoria como objeto date.

        Args:
            start_date: Fecha inicial (YYYY-MM-DD)
            end_date: Fecha final (YYYY-MM-DD)

        Returns:
            Objeto date
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        random_date = start + timedelta(
            seconds=self._rng.randint(0, int((end - start).total_seconds()))
        )
        return random_date.date()

    @staticmethod
    def _clean_name(name: str) -> str:
        """
        Limpia el nombre removiendo espacios y guiones.

        Args:
            name: Nombre a limpiar

        Returns:
            Nombre sin espacios ni guiones
        """
        return name.replace(" ", "").replace("-", "").upper()

    @staticmethod
    def _get_first_consonant(text: str) -> str:
        """
        Obtiene la primera consonante de un texto.

        Args:
            text: Texto del cual extraer consonante

        Returns:
            Primera consonante encontrada, o 'X' si no hay
        """
        consonants = "BCDFGHJKLMNPQRSTVWXYZ"
        clean = IdentifierGenerator._clean_name(text)
        for char in clean:
            if char in consonants:
                return char
        return "X"

    @staticmethod
    def _get_first_internal_vowel(text: str) -> str:
        """
        Obtiene la primera vocal interna (no la primera letra) de un texto.

        Args:
            text: Texto del cual extraer la vocal

        Returns:
            Primera vocal interna encontrada, o 'X' si no hay
        """
        vowels = "AEIOU"
        clean = IdentifierGenerator._clean_name(text)
        # Buscar la primera vocal a partir de la posición 1 (ignorar la primera letra)
        for char in clean[1:]:
            if char in vowels:
                return char
        return "X"

    @staticmethod
    def _get_first_letter(text: str) -> str:
        """
        Obtiene la primera letra de un texto.

        Args:
            text: Texto del cual extraer

        Returns:
            Primera letra, o 'X' si está vacío
        """
        clean = IdentifierGenerator._clean_name(text)
        return clean[0] if clean else "X"


class RfcGenerator(IdentifierGenerator):
    """Generador de RFC (Registro Federal de Contribuyentes)."""

    def generate_rfc(
        self,
        fecha_nacimiento: date,
        apellido_paterno: str = "",
        apellido_materno: str = "",
        nombre: str = ""
    ) -> str:
        """
        Genera un RFC basado en nombre, apellidos y fecha de nacimiento.

        Sigue las reglas legales del SAT:
        - Posiciones 1-4: Derivadas del nombre y apellidos
        - Posiciones 5-10: Fecha de nacimiento (YYMMDD)
        - Posiciones 11-13: Homoclave (derivada del nombre)

        Args:
            fecha_nacimiento: Fecha de nacimiento de la persona
            apellido_paterno: Apellido paterno
            apellido_materno: Apellido materno
            nombre: Nombre(s) de la persona

        Returns:
            RFC de 13 caracteres
        """
        rfc = ""

        # Posiciones 1-4: Basadas en nombre y apellidos
        if apellido_paterno and apellido_materno and nombre:
            # Pos 1: Primera letra del apellido paterno
            rfc += self._get_first_letter(apellido_paterno)
            # Pos 2: Primera vocal interna del apellido paterno
            rfc += self._get_first_internal_vowel(apellido_paterno)
            # Pos 3: Primera letra del apellido materno
            rfc += self._get_first_letter(apellido_materno)
            # Pos 4: Primera letra del nombre
            rfc += self._get_first_letter(nombre)
        else:
            # Si no hay nombres, generar aleatoriamente (para compatibilidad)
            rfc += self._rng.choice(self.CHARACTERS)
            rfc += self._rng.choice(self.VOWELS)
            rfc += self._rng.choice(self.CHARACTERS)
            rfc += self._rng.choice(self.CHARACTERS)

        # Posiciones 5-10: Fecha de nacimiento (YYMMDD)
        rfc += fecha_nacimiento.strftime("%y%m%d")

        # Posiciones 11-13: Homoclave (3 caracteres alfanuméricos)
        if apellido_paterno and apellido_materno and nombre:
            # Generar homoclave basada en consonantes internas
            clean_ap = self._clean_name(apellido_paterno)
            clean_am = self._clean_name(apellido_materno)
            clean_n = self._clean_name(nombre)

            # Primera consonante interna de apellido paterno
            ap_consonant = self._get_first_consonant(clean_ap[1:]) if len(clean_ap) > 1 else "X"
            # Primera consonante de apellido materno
            am_consonant = self._get_first_consonant(clean_am)
            # Primera consonante de nombre
            n_consonant = self._get_first_consonant(clean_n)

            rfc += ap_consonant
            rfc += am_consonant
            rfc += n_consonant
        else:
            # Generar aleatoriamente si no hay datos
            rfc += self._rng.choice(self.ALL_CHARS)
            rfc += self._rng.choice(self.ALL_CHARS)
            rfc += self._rng.choice(self.ALL_CHARS)

        return rfc

    def generate(self, count: int, apellido_paterno: str = "", apellido_materno: str = "", nombre: str = "") -> Generator[str, None, None]:
        """
        Genera una secuencia de RFCs.

        Args:
            count: Cantidad de RFCs a generar
            apellido_paterno: Apellido paterno (opcional)
            apellido_materno: Apellido materno (opcional)
            nombre: Nombre (opcional)

        Yields:
            RFCs generados
        """
        for _ in range(count):
            fecha = self.random_date_object()
            yield self.generate_rfc(fecha, apellido_paterno, apellido_materno, nombre)


class CurpGenerator(IdentifierGenerator):
    """Generador de CURP (Clave Única de Registro de Población)."""

    def generate_curp_with_data(
        self,
        apellido_paterno: str = "",
        apellido_materno: str = "",
        nombre: str = "",
        fecha_nacimiento: Optional[date] = None,
        sexo: Optional[str] = None,
        codigo_estado: Optional[str] = None
    ) -> CurpData:
        """
        Genera una CURP completa con todos los datos asociados.

        Sigue las reglas legales de la RENAPO:
        - Posiciones 1-4: Derivadas del nombre y apellidos
        - Posiciones 5-10: Fecha de nacimiento (YYMMDD)
        - Posición 11: Sexo (H/M)
        - Posiciones 12-13: Código de estado
        - Posiciones 14-16: Consonantes internas
        - Posición 17: Dígito diferenciador
        - Posición 18: Dígito verificador

        Args:
            apellido_paterno: Apellido paterno
            apellido_materno: Apellido materno
            nombre: Nombre(s)
            fecha_nacimiento: Fecha de nacimiento (opcional)
            sexo: Sexo 'H' o 'M' (opcional)
            codigo_estado: Código del estado (opcional)

        Returns:
            CurpData con CURP y datos extraídos
        """
        # Generar datos base si no se proporcionan
        if fecha_nacimiento is None:
            fecha_nacimiento = self.random_date_object()
        if sexo is None:
            sexo = self._rng.choice(self.SEXO_OPTIONS)
        if codigo_estado is None:
            codigo_estado = self._rng.choice(self.estados_list)

        estado_nacimiento = self.ESTADOS_MEXICO[codigo_estado]

        # Construir CURP
        curp = ""

        # Posiciones 1-4: Derivadas del nombre y apellidos
        if apellido_paterno and apellido_materno and nombre:
            # Pos 1: Primera letra del apellido paterno
            curp += self._get_first_letter(apellido_paterno)
            # Pos 2: Primera vocal interna del apellido paterno
            curp += self._get_first_internal_vowel(apellido_paterno)
            # Pos 3: Primera letra del apellido materno
            curp += self._get_first_letter(apellido_materno)
            # Pos 4: Primera letra del nombre
            curp += self._get_first_letter(nombre)
        else:
            # Si no hay nombres, generar aleatoriamente (para compatibilidad)
            curp += self._rng.choice(self.CHARACTERS)
            curp += self._rng.choice(self.VOWELS)
            curp += self._rng.choice(self.CHARACTERS)
            curp += self._rng.choice(self.CHARACTERS)

        # Posiciones 5-10: Fecha de nacimiento (YYMMDD)
        curp += fecha_nacimiento.strftime("%y%m%d")

        # Posición 11: Sexo (H/M)
        curp += sexo

        # Posiciones 12-13: Estado de nacimiento
        curp += codigo_estado

        # Posiciones 14-16: Consonantes internas
        if apellido_paterno and apellido_materno and nombre:
            # Primera consonante interna del apellido paterno
            ap_consonant = self._get_first_consonant(self._clean_name(apellido_paterno)[1:]) if len(self._clean_name(apellido_paterno)) > 1 else "X"
            # Primera consonante del apellido materno
            am_consonant = self._get_first_consonant(self._clean_name(apellido_materno))
            # Primera consonante del nombre
            n_consonant = self._get_first_consonant(self._clean_name(nombre))

            curp += ap_consonant
            curp += am_consonant
            curp += n_consonant
        else:
            # Generar aleatoriamente si no hay datos
            curp += self._rng.choice(self.CONSONANTS)
            curp += self._rng.choice(self.CONSONANTS)
            curp += self._rng.choice(self.CONSONANTS)

        # Posición 17: Dígito diferenciador (0-9 o A-Z para años >= 2000)
        if fecha_nacimiento.year >= 2000:
            curp += self._rng.choice(self.CHARACTERS)
        else:
            curp += self._rng.choice(self.DIGITS)

        # Posición 18: Dígito verificador
        curp += self._rng.choice(self.DIGITS)

        return CurpData(
            curp=curp,
            fecha_nacimiento=fecha_nacimiento,
            sexo="Hombre" if sexo == "H" else "Mujer",
            estado_nacimiento=estado_nacimiento,
            codigo_estado=codigo_estado,
            nombre=nombre,
            apellido_paterno=apellido_paterno,
            apellido_materno=apellido_materno
        )

    def generate_curp(
        self,
        apellido_paterno: str = "",
        apellido_materno: str = "",
        nombre: str = ""
    ) -> str:
        """
        Genera solo la CURP (retrocompatibilidad).

        Args:
            apellido_paterno: Apellido paterno
            apellido_materno: Apellido materno
            nombre: Nombre(s)

        Returns:
            CURP de 18 caracteres
        """
        return self.generate_curp_with_data(apellido_paterno, apellido_materno, nombre).curp

    def generate(self, count: int, apellido_paterno: str = "", apellido_materno: str = "", nombre: str = "") -> Generator[str, None, None]:
        """
        Genera una secuencia de CURPs.

        Args:
            count: Cantidad de CURPs a generar
            apellido_paterno: Apellido paterno (opcional)
            apellido_materno: Apellido materno (opcional)
            nombre: Nombre (opcional)

        Yields:
            CURPs generadas
        """
        for _ in range(count):
            yield self.generate_curp(apellido_paterno, apellido_materno, nombre)
