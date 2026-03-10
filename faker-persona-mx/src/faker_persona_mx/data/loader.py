"""
Cargador y gestor de datasets.
"""

import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import pandas as pd
from sklearn.utils import shuffle as sk_shuffle

from ..utils.logger import get_logger
from ..utils.config import config

logger = get_logger(__name__)


@dataclass
class LadaInfo:
    """Información de una lada con ciudad y estado."""
    lada: str
    ciudad: str
    estado: str


class DataLoader:
    """Carga y gestiona los datasets necesarios para la generación de datos."""

    def __init__(
        self,
        seed: int = 0,
        cache_enabled: bool = True,
        data_dir: Optional[Path] = None
    ):
        """
        Inicializa el cargador de datos.

        Args:
            seed: Semilla para reproducibilidad
            cache_enabled: Habilitar sistema de caché
            data_dir: Directorio personalizado de datos (usa config por defecto)
        """
        self.seed = seed
        self.cache_enabled = cache_enabled
        self.data_dir = data_dir or config.DATASETS_DIR
        self.cache_dir = config.CACHE_DIR

        # Datasets
        self.nombres: List[str] = []
        self.apellidos_paternos: List[str] = []
        self.apellidos_maternos: List[str] = []
        self.email_domains: List[str] = []
        self.email_usernames: List[str] = []
        self.ladas_por_estado: Dict[str, List[str]] = {}
        self.ladas_completas: List[LadaInfo] = []  # Lista completa con ciudades

        logger.info(f"DataLoader inicializado - Seed: {seed}, Cache: {cache_enabled}")

    def load_all(self) -> None:
        """Carga todos los datasets necesarios."""
        logger.info("Iniciando carga de datasets...")

        if self.cache_enabled and self._load_from_cache():
            logger.info("Datasets cargados desde caché")
            return

        # Cargar datasets originales
        self._load_datasets()

        # Barajar datasets
        self._shuffle_datasets()

        # Guardar en caché
        if self.cache_enabled:
            self._save_to_cache()
            logger.info("Datasets guardados en caché")

        logger.info("Carga de datasets completada")

    def _load_csv(self, filepath: Path, column: str = "tokens") -> List[str]:
        """
        Carga un archivo CSV y retorna una columna como lista.

        Args:
            filepath: Ruta al archivo CSV
            column: Nombre de la columna a extraer

        Returns:
            Lista de valores limpiados

        Raises:
            FileNotFoundError: Si el archivo no existe
            ValueError: Si la columna no existe
        """
        if not filepath.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")

        try:
            df = pd.read_csv(filepath)
            if column not in df.columns:
                raise ValueError(f"Columna '{column}' no encontrada en {filepath}")

            # Limpiar datos: eliminar NaN y strings vacíos
            df = df.dropna(subset=[column])
            df = df[df[column].astype(str).str.strip() != ""]

            return df[column].astype(str).str.strip().tolist()
        except Exception as e:
            logger.error(f"Error al cargar {filepath}: {e}")
            raise

    def _load_ladas_csv(self, filepath: Path) -> Tuple[Dict[str, List[str]], List[LadaInfo]]:
        """
        Carga el archivo CSV de ladas y organiza por estado.

        Args:
            filepath: Ruta al archivo CSV de ladas

        Returns:
            Tupla con (diccionario ladas por estado, lista completa de LadaInfo)
        """
        if not filepath.exists():
            raise FileNotFoundError(f"Archivo de ladas no encontrado: {filepath}")

        try:
            df = pd.read_csv(filepath)
            required_columns = ["lada", "estado", "ciudad"]

            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Columna '{col}' no encontrada en {filepath}")

            # Limpiar y organizar
            df = df.dropna(subset=required_columns)
            df["lada"] = df["lada"].astype(str).str.strip()
            df["estado"] = df["estado"].astype(str).str.strip()
            df["ciudad"] = df["ciudad"].astype(str).str.strip()

            # Agrupar por estado
            ladas_dict: Dict[str, List[str]] = {}
            for estado, group in df.groupby("estado"):
                ladas_dict[estado] = group["lada"].unique().tolist()

            # Crear lista completa de LadaInfo
            ladas_completas: List[LadaInfo] = []
            for _, row in df.iterrows():
                ladas_completas.append(LadaInfo(
                    lada=row["lada"],
                    ciudad=row["ciudad"],
                    estado=row["estado"]
                ))

            logger.info(
                f"Cargadas {len(ladas_dict)} estados con ladas, "
                f"{len(ladas_completas)} combinaciones lada-ciudad"
            )
            return ladas_dict, ladas_completas

        except Exception as e:
            logger.error(f"Error al cargar ladas desde {filepath}: {e}")
            raise

    def _load_datasets(self) -> None:
        """Carga todos los datasets desde archivos CSV."""
        self.nombres = self._load_csv(self.data_dir / config.NOMBRES_FILE)
        self.apellidos_paternos = self._load_csv(
            self.data_dir / config.APELLIDOS_PATERNOS_FILE
        )
        self.apellidos_maternos = self._load_csv(
            self.data_dir / config.APELLIDOS_MATERNOS_FILE
        )
        self.email_domains = self._load_csv(self.data_dir / config.EMAIL_DOMAINS_FILE)
        self.email_usernames = self._load_csv(
            self.data_dir / config.EMAIL_USERNAMES_FILE
        )
        self.ladas_por_estado, self.ladas_completas = self._load_ladas_csv(
            self.data_dir / config.LADAS_MEXICO_FILE
        )

        logger.info(
            f"Datasets cargados: {len(self.nombres)} nombres, "
            f"{len(self.apellidos_paternos)} apellidos paternos, "
            f"{len(self.apellidos_maternos)} apellidos maternos"
        )

    def _shuffle_datasets(self) -> None:
        """Baraja los datasets con la semilla configurada."""
        self.nombres = sk_shuffle(self.nombres, random_state=self.seed)
        self.apellidos_paternos = sk_shuffle(
            self.apellidos_paternos, random_state=self.seed
        )
        self.apellidos_maternos = sk_shuffle(
            self.apellidos_maternos, random_state=self.seed
        )
        self.email_domains = sk_shuffle(self.email_domains, random_state=self.seed)
        self.email_usernames = sk_shuffle(
            self.email_usernames, random_state=self.seed
        )

    def _get_cache_path(self, dataset_name: str) -> Path:
        """Genera la ruta del archivo de caché para un dataset."""
        return self.cache_dir / f"{dataset_name}_seed{self.seed}.csv"

    def _save_to_cache(self) -> None:
        """Guarda los datasets barajados en caché."""
        datasets = {
            "nombres": self.nombres,
            "apellidos_paternos": self.apellidos_paternos,
            "apellidos_maternos": self.apellidos_maternos,
            "email_domains": self.email_domains,
            "email_usernames": self.email_usernames,
        }

        for name, data in datasets.items():
            cache_file = self._get_cache_path(name)
            df = pd.DataFrame({"tokens": data})
            df.to_csv(cache_file, index=False)

        logger.debug(f"Caché guardado para seed {self.seed}")

    def _load_from_cache(self) -> bool:
        """
        Intenta cargar datasets desde caché.

        Returns:
            True si se cargó exitosamente, False si no
        """
        try:
            # Verificar que todos los archivos de caché existan
            cache_files = {
                "nombres": self._get_cache_path("nombres"),
                "apellidos_paternos": self._get_cache_path("apellidos_paternos"),
                "apellidos_maternos": self._get_cache_path("apellidos_maternos"),
                "email_domains": self._get_cache_path("email_domains"),
                "email_usernames": self._get_cache_path("email_usernames"),
            }

            for cache_file in cache_files.values():
                if not cache_file.exists():
                    logger.debug(f"Archivo de caché no encontrado: {cache_file}")
                    return False

            # Cargar desde caché
            self.nombres = self._load_csv(cache_files["nombres"])
            self.apellidos_paternos = self._load_csv(cache_files["apellidos_paternos"])
            self.apellidos_maternos = self._load_csv(cache_files["apellidos_maternos"])
            self.email_domains = self._load_csv(cache_files["email_domains"])
            self.email_usernames = self._load_csv(cache_files["email_usernames"])

            # Siempre cargar ladas desde el archivo original
            self.ladas_por_estado, self.ladas_completas = self._load_ladas_csv(
                self.data_dir / config.LADAS_MEXICO_FILE
            )

            return True

        except Exception as e:
            logger.warning(f"No se pudo cargar desde caché: {e}")
            return False
