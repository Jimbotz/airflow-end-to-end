"""
Generador principal de personas ficticias mexicanas con datos coherentes.
"""

from typing import List, Optional, Generator as TypingGenerator
import random

from .models import Persona, GeneratorConfig
from ..data.loader import DataLoader, LadaInfo
from ..generators import (
    NameGenerator,
    EmailGenerator,
    CurpGenerator,
    RfcGenerator,
    CurpData
)
from ..utils.logger import get_logger

logger = get_logger(__name__)


class PersonaGenerator:
    """
    Generador principal de personas ficticias mexicanas.

    Genera datos coherentes donde:
    - La CURP contiene fecha_nacimiento, sexo y estado_nacimiento
    - El RFC usa la misma fecha de nacimiento que la CURP
    - El teléfono corresponde a una ciudad real del estado de nacimiento
    """

    def __init__(
        self,
        seed: int = 0,
        cache_enabled: bool = True
    ):
        """
        Inicializa el generador de personas.

        Args:
            seed: Semilla para reproducibilidad
            cache_enabled: Habilitar caché de datasets
        """
        self.config = GeneratorConfig(seed=seed, cache_enabled=cache_enabled)
        self.seed = seed
        random.seed(seed)

        logger.info(f"Inicializando PersonaGenerator con seed={seed}")

        # Cargar datasets
        self.data_loader = DataLoader(seed=seed, cache_enabled=cache_enabled)
        self.data_loader.load_all()

        # Inicializar generadores
        self._init_generators()

        logger.info("PersonaGenerator inicializado exitosamente")

    def _init_generators(self) -> None:
        """Inicializa todos los generadores individuales."""

        # Generadores de nombres
        self.nombre_generator = NameGenerator(
            self.data_loader.nombres,
            separator=" "
        )
        self.apellido_paterno_generator = NameGenerator(
            self.data_loader.apellidos_paternos,
            separator=" "
        )
        self.apellido_materno_generator = NameGenerator(
            self.data_loader.apellidos_maternos,
            separator=" "
        )

        # Generador de emails
        self.email_generator = EmailGenerator(
            self.data_loader.email_usernames,
            self.data_loader.email_domains,
            separator="."
        )

        # Generadores de identificadores
        self.curp_generator = CurpGenerator(seed=self.seed)
        self.rfc_generator = RfcGenerator(seed=self.seed)

        logger.debug("Todos los generadores inicializados")

    def _get_telefono_y_ciudad(self, estado_nacimiento: str) -> tuple:
        """
        Obtiene un teléfono y ciudad coherente con el estado de nacimiento.

        Args:
            estado_nacimiento: Estado de nacimiento de la persona

        Returns:
            Tupla (telefono, ciudad)
        """
        # Filtrar ladas del estado de nacimiento
        ladas_estado = [
            lada for lada in self.data_loader.ladas_completas
            if lada.estado == estado_nacimiento
        ]

        if not ladas_estado:
            # Si no hay ladas para ese estado, usar cualquiera
            logger.warning(f"No se encontraron ladas para {estado_nacimiento}, usando aleatorio")
            ladas_estado = self.data_loader.ladas_completas

        # Seleccionar una lada aleatoria del estado
        lada_info = random.choice(ladas_estado)

        # Calcular dígitos del número local según la lada
        # En México: lada de 2 dígitos + 8 dígitos locales = 10
        #           lada de 3 dígitos + 7 dígitos locales = 10
        lada_len = len(lada_info.lada)
        local_digits = 10 - lada_len

        # Generar número local con la cantidad correcta de dígitos
        min_value = 10 ** (local_digits - 1)
        max_value = (10 ** local_digits) - 1
        numero = str(random.randint(min_value, max_value))

        telefono = f"{lada_info.lada}{numero}"
        ciudad = lada_info.ciudad

        return telefono, ciudad

    def generate_one(self, index: int = 0) -> Persona:
        """
        Genera una única persona ficticia con datos coherentes.

        Ahora el nombre, apellido paterno, apellido materno, CURP y RFC están
        completamente acoplados y siguen las reglas legales de formación.

        Args:
            index: Índice para la generación determinística

        Returns:
            Objeto Persona generado con datos coherentes
        """
        try:
            # Generar nombres y email
            nombre = self.nombre_generator.get(index)
            apellido_paterno = self.apellido_paterno_generator.get(index)
            apellido_materno = self.apellido_materno_generator.get(index)
            email = self.email_generator.get(index)

            # Generar CURP con datos extraídos y ACOPLADO a los nombres
            curp_data = self.curp_generator.generate_curp_with_data(
                apellido_paterno=apellido_paterno,
                apellido_materno=apellido_materno,
                nombre=nombre
            )

            # Generar RFC usando la misma fecha de nacimiento y ACOPLADO a los nombres
            rfc = self.rfc_generator.generate_rfc(
                fecha_nacimiento=curp_data.fecha_nacimiento,
                apellido_paterno=apellido_paterno,
                apellido_materno=apellido_materno,
                nombre=nombre
            )

            # Generar teléfono y ciudad del mismo estado de nacimiento
            telefono, ciudad = self._get_telefono_y_ciudad(curp_data.estado_nacimiento)

            # Crear persona con datos coherentes
            persona = Persona(
                nombre=nombre,
                apellido_paterno=apellido_paterno,
                apellido_materno=apellido_materno,
                email=email,
                telefono=telefono,
                ciudad=ciudad,
                curp=curp_data.curp,
                rfc=rfc,
                fecha_nacimiento=curp_data.fecha_nacimiento,
                sexo=curp_data.sexo,
                estado_nacimiento=curp_data.estado_nacimiento
            )

            return persona

        except Exception as e:
            logger.error(f"Error generando persona en índice {index}: {e}")
            raise

    def generate_batch(self, count: int) -> List[Persona]:
        """
        Genera un lote de personas ficticias.

        Args:
            count: Cantidad de personas a generar

        Returns:
            Lista de objetos Persona
        """
        logger.info(f"Generando {count} personas...")

        personas = []
        for i in range(count):
            try:
                persona = self.generate_one(i)
                personas.append(persona)
            except Exception as e:
                logger.warning(f"Error generando persona #{i}: {e}")
                continue

        logger.info(f"Generadas {len(personas)} personas exitosamente")
        return personas

    def generate_stream(self, count: int) -> TypingGenerator[Persona, None, None]:
        """
        Genera personas de forma lazy (generador).

        Args:
            count: Cantidad de personas a generar

        Yields:
            Objetos Persona uno por uno
        """
        for i in range(count):
            try:
                yield self.generate_one(i)
            except Exception as e:
                logger.warning(f"Error generando persona #{i}: {e}")
                continue

    def to_dict_list(self, personas: List[Persona]) -> List[dict]:
        """
        Convierte una lista de Persona a lista de diccionarios.

        Args:
            personas: Lista de objetos Persona

        Returns:
            Lista de diccionarios
        """
        return [persona.model_dump(mode='json') for persona in personas]

    def to_dataframe(self, personas: List[Persona]):
        """
        Convierte una lista de Persona a DataFrame de pandas.

        Args:
            personas: Lista de objetos Persona

        Returns:
            DataFrame con los datos
        """
        try:
            import pandas as pd
            data = self.to_dict_list(personas)
            return pd.DataFrame(data)
        except ImportError:
            logger.error("pandas no está instalado. Instálalo con: pip install pandas")
            raise

    def export_to_csv(
        self,
        personas: List[Persona],
        filepath: str,
        include_header: bool = True
    ) -> None:
        """
        Exporta personas a un archivo CSV.

        Args:
            personas: Lista de objetos Persona
            filepath: Ruta del archivo de salida
            include_header: Incluir encabezados
        """
        try:
            df = self.to_dataframe(personas)
            df.to_csv(filepath, index=False, header=include_header)
            logger.info(f"Exportadas {len(personas)} personas a {filepath}")
        except Exception as e:
            logger.error(f"Error exportando a CSV: {e}")
            raise

    def export_to_json(
        self,
        personas: List[Persona],
        filepath: str,
        indent: int = 2
    ) -> None:
        """
        Exporta personas a un archivo JSON.

        Args:
            personas: Lista de objetos Persona
            filepath: Ruta del archivo de salida
            indent: Espacios de indentación
        """
        try:
            import json
            from pathlib import Path

            data = self.to_dict_list(personas)
            Path(filepath).write_text(
                json.dumps(data, ensure_ascii=False, indent=indent),
                encoding="utf-8"
            )
            logger.info(f"Exportadas {len(personas)} personas a {filepath}")
        except Exception as e:
            logger.error(f"Error exportando a JSON: {e}")
            raise
