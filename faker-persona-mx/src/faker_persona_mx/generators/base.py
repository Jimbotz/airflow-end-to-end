"""
Clases base y utilidades para generadores.
"""

from typing import List


class BaseConverter:
    """Convierte números decimales a una base específica."""

    def __init__(self, base: int):
        """
        Inicializa el convertidor de base.

        Args:
            base: La base numérica a utilizar para conversión
        """
        if base < 2:
            raise ValueError("La base debe ser mayor o igual a 2")
        self.base = base

    def convert(self, num: int) -> List[int]:
        """
        Convierte un número decimal a la base especificada.

        Args:
            num: Número decimal a convertir

        Returns:
            Lista de dígitos en la base especificada
        """
        if num == 0:
            return [0]

        result: List[int] = []
        while num:
            result.insert(0, int(num % self.base))
            num //= self.base
        return result
