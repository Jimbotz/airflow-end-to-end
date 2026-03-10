"""
Generador de nombres.
"""

from typing import List, Generator
from .base import BaseConverter


class NameGenerator:
    """Genera nombres combinando elementos de una lista."""

    def __init__(self, name_list: List[str], separator: str = " "):
        """
        Inicializa el generador de nombres.

        Args:
            name_list: Lista de nombres o componentes
            separator: Separador entre componentes (default: espacio)
        """
        if not name_list:
            raise ValueError("La lista de nombres no puede estar vacía")

        self.name_list = name_list
        self.separator = separator
        self.count = len(self.name_list)
        self.converter = BaseConverter(self.count)

    def get(self, index: int) -> str:
        """
        Obtiene un nombre basado en un índice.

        Args:
            index: Índice para generar el nombre

        Returns:
            Nombre generado

        Raises:
            TypeError: Si la lista contiene elementos que no son strings
        """
        index_list = self.converter.convert(index)
        name_parts: List[str] = []

        for i in index_list:
            try:
                name_parts.append(str(self.name_list[i]))
            except (IndexError, TypeError) as e:
                raise TypeError(
                    f"Error al acceder al índice {i} en la lista de nombres: {e}"
                ) from e

        return self.separator.join(name_parts)

    def generate(self, count: int) -> Generator[str, None, None]:
        """
        Genera una secuencia de nombres.

        Args:
            count: Cantidad de nombres a generar

        Yields:
            Nombres generados
        """
        for i in range(count):
            yield self.get(i)
