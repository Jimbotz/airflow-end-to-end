"""
Generador principal de personas ficticias mexicanas.
"""

from typing import List, Optional, Generator as TypingGenerator
import random

from .models import Persona, GeneratorConfig
from ..data.loader import DataLoader
from ..generators import (
    NameGenerator,
    EmailGenerator,
    CurpGenerator,
    RfcGenerator,
    PhoneGenerator
)
from ..utils.logger import get_logger

logger = get_logger(__name__)


class PersonaGenerator:
    """
    Generador principal de personas ficticias mexicanas.

    Orquesta todos los generadores individuales para crear objetos Persona completos.
    """

    def __init__(
        self,
        seed: int = 0,
        cache_enabled: bool = True,
        estados_activos: Optional[List[str]] = None
    ):
        """
        Inicializa el generador de personas.

        Args:
            seed: Semilla para reproducibilidad
            cache_enabled: Habilitar caché de datasets
            estados_activos: Lista de estados para generar teléfonos (None = todos)
        """
        self.config = GeneratorConfig(seed=seed, cache_enabled=cache_enabled)
        self.seed = seed
        random.seed(seed)

        logger.info(f"Inicializando PersonaGenerator con seed={seed}")

        # Cargar datasets
        self.data_loader = DataLoader(seed=seed, cache_enabled=cache_enabled)
        self.data_loader.load_all()

        # Inicializar generadores
        self._init_generators(estados_activos)

        logger.info("PersonaGenerator inicializado exitosamente")

    def _init_generators(self, estados_activos: Optional[List[str]]) -> None:
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

        # Generador de teléfonos
        self.phone_generator = PhoneGenerator(
            ladas_por_estado=self.data_loader.ladas_por_estado,
            estados_activos=estados_activos,
            seed=self.seed
        )

        logger.debug("Todos los generadores inicializados")

    def generate_one(self, index: int = 0) -> Persona:
        """
        Genera una única persona ficticia.

        Args:
            index: Índice para la generación determinística

        Returns:
            Objeto Persona generado
        """
        try:
            nombre = self.nombre_generator.get(index)
            apellido_paterno = self.apellido_paterno_generator.get(index)
            apellido_materno = self.apellido_materno_generator.get(index)
            email = self.email_generator.get(index)
            telefono = self.phone_generator.get(index)
            curp = self.curp_generator.generate_curp()
            rfc = self.rfc_generator.generate_rfc()

            persona = Persona(
                nombre=nombre,
                apellido_paterno=apellido_paterno,
                apellido_materno=apellido_materno,
                email=email,
                telefono=telefono,
                curp=curp,
                rfc=rfc
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

        personas: List[Persona] = []
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
        return [persona.model_dump() for persona in personas]

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
            return pd.DataFrame(self.to_dict_list(personas))
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
