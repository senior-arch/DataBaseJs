# utils/logger.py
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional, Union


def setup_logging(
    log_file: Optional[Union[str, Path]] = None,
    level: Union[str, int] = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    console: bool = True
) -> None:
    """
    Configura o logging global da aplicação.

    Args:
        log_file: Caminho para o arquivo de log (opcional). Se None, não loga em arquivo.
        level: Nível de log (ex: logging.INFO, "DEBUG").
        format: Formato da mensagem.
        datefmt: Formato da data.
        max_bytes: Tamanho máximo do arquivo antes de rotacionar.
        backup_count: Número de backups a manter.
        console: Se True, também envia logs para o console.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove handlers existentes para evitar duplicação
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(fmt=format, datefmt=datefmt)

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger com o nome especificado.
    Útil para módulos que querem seu próprio logger.
    """
    return logging.getLogger(name)