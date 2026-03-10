"""
Generador de correos electrónicos.
"""

from typing import List, Generator
from .names import NameGenerator


class EmailGenerator:
    """Genera direcciones de correo electrónico."""

    def __init__(
        self,
        email_name_list: List[str],
        email_domain_list: List[str] = None,
        separator: str = "."
    ):
        """
        Inicializa el generador de emails.

        Args:
            email_name_list: Lista de nombres para usar en la parte local del email
            email_domain_list: Lista de dominios (default: ["example.com"])
            separator: Separador entre componentes del nombre (default: punto)
        """
        if not email_name_list:
            raise ValueError("La lista de nombres de email no puede estar vacía")

        self.name_list = email_name_list
        self.domain_names = email_domain_list or ["example.com"]
        self.separator = separator
        self.count_domains = len(self.domain_names)
        self.name_generator = NameGenerator(email_name_list, separator)

    def get(self, index: int) -> str:
        """
        Genera un email basado en un índice.

        Args:
            index: Índice para generar el email

        Returns:
            Email generado
        """
        domain_index = index % self.count_domains
        local_part = self.name_generator.get(index)
        domain = self.domain_names[domain_index]
        return f"{local_part}@{domain}"

    def generate(self, count: int) -> Generator[str, None, None]:
        """
        Genera una secuencia de emails.

        Args:
            count: Cantidad de emails a generar

        Yields:
            Emails generados
        """
        for i in range(count):
            yield self.get(i)
