"""
Configuración centralizada del proyecto.
"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Configuración de la aplicación usando variables de entorno."""

    # Rutas
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "src" / "faker_persona_mx" / "data"
    DATASETS_DIR: Path = DATA_DIR / "datasets"
    CACHE_DIR: Path = DATA_DIR / "cache"

    # Configuración de generación
    DEFAULT_SEED: int = 0
    ENABLE_CACHE: bool = True
    CACHE_SIZE_TOLERANCE: float = 0.01  # 1% de tolerancia

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Optional[Path] = None

    # Dataset files
    NOMBRES_FILE: str = "nombres.csv"
    APELLIDOS_PATERNOS_FILE: str = "apellidos_paternos.csv"
    APELLIDOS_MATERNOS_FILE: str = "apellidos_maternos.csv"
    EMAIL_DOMAINS_FILE: str = "email_domains.csv"
    EMAIL_USERNAMES_FILE: str = "email_usernames.csv"
    LADAS_MEXICO_FILE: str = "ladas_mexico.csv"

    class Config:
        """Configuración de Pydantic."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Crear directorios si no existen
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.DATASETS_DIR.mkdir(parents=True, exist_ok=True)


# Singleton de configuración
config = Config()
