"""
Sistema de logging profesional para Faker Persona MX.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def get_logger(
    name: str = "faker_persona_mx",
    level: int = logging.INFO,
    log_file: Optional[Path] = None
) -> logging.Logger:
    """
    Configura y retorna un logger.

    Args:
        name: Nombre del logger
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Ruta opcional para guardar logs en archivo

    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)

    # Evitar duplicar handlers si ya existe
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Formato del log
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler para archivo si se especifica
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
