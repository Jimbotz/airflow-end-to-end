"""
Generador de números telefónicos mexicanos.
"""

import random
from typing import List, Dict, Generator, Optional
from sklearn.utils import shuffle as sk_shuffle


# note: reproducibility and lada-length handling fixes


class PhoneGenerator:
    """Generador de números telefónicos mexicanos válidos."""

    def __init__(
        self,
        ladas_por_estado: Dict[str, List[str]],
        estados_activos: Optional[List[str]] = None,
        seed: int = 0
    ):
        """
        Inicializa el generador de teléfonos.

        Args:
            ladas_por_estado: Diccionario de ladas organizadas por estado
            estados_activos: Lista de estados a usar (None = todos)
            seed: Semilla para reproducibilidad
        """
        if not ladas_por_estado:
            raise ValueError("El diccionario de ladas no puede estar vacío")

        self.ladas_por_estado = ladas_por_estado
        self.estados_activos = estados_activos or list(ladas_por_estado.keys())
        self.seed = seed
        # usar RNG local para no alterar el estado global
        self._rng = random.Random(self.seed)

        # Generar lista de teléfonos
        self._generate_phone_list()

    def _generate_phone_list(self, start: int = 1, count: int = 9_999_999) -> None:
        """
        Genera una lista pre-barajada de números telefónicos.

        Args:
            start: Número inicial para la secuencia
            count: Cantidad de números a generar
        """
        phone_list: List[str] = []

        for i in range(start, start + count):
            estado = self._rng.choice(self.estados_activos)
            if estado in self.ladas_por_estado and self.ladas_por_estado[estado]:
                lada = self._rng.choice(self.ladas_por_estado[estado])
                # calcular cuántos dígitos de número necesitamos para llegar a 10
                digits_needed = 10 - len(lada)
                if digits_needed < 1:
                    # en caso de lada demasiado larga, caer en 7 como antes
                    digits_needed = 7
                numero = str(i).zfill(digits_needed)
                phone_list.append(f"{lada}{numero}")

        # Barajar la lista para distribución aleatoria
        self.phone_list = sk_shuffle(phone_list, random_state=self.seed)

    def get(self, index: int) -> str:
        """
        Obtiene un teléfono por índice.

        Args:
            index: Índice del teléfono

        Returns:
            Número telefónico de 10 dígitos

        Raises:
            IndexError: Si el índice está fuera de rango
        """
        if index >= len(self.phone_list):
            raise IndexError(
                f"Índice {index} fuera de rango. "
                f"Máximo disponible: {len(self.phone_list) - 1}"
            )
        return self.phone_list[index]

    def generate(self, count: int) -> Generator[str, None, None]:
        """
        Genera una secuencia de números telefónicos.

        Args:
            count: Cantidad de teléfonos a generar

        Yields:
            Números telefónicos

        Raises:
            ValueError: Si se solicitan más números de los disponibles
        """
        if count > len(self.phone_list):
            raise ValueError(
                f"No se pueden generar {count} teléfonos. "
                f"Máximo disponible: {len(self.phone_list)}"
            )

        for i in range(count):
            yield self.phone_list[i]

    def generate_single(self) -> str:
        """
        Genera un único número telefónico aleatorio.

        Ahora considera la longitud de la lada y ajusta la cantidad de dígitos
        del número para que el resultado siempre tenga 10 dígitos.

        Returns:
            Número telefónico de 10 dígitos
        """
        estado = self._rng.choice(self.estados_activos)
        lada = self._rng.choice(self.ladas_por_estado[estado])
        digits_needed = 10 - len(lada)
        if digits_needed < 1:
            digits_needed = 7
        numero = str(self._rng.randint(1, 10**digits_needed - 1)).zfill(digits_needed)
        return f"{lada}{numero}"
