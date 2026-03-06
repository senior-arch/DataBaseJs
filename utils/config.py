# utils/config.py
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field


@dataclass
class Config:
    """Estrutura de configuração do servidor."""
    host: str = "0.0.0.0"
    port: int = 5432
    data_dir: Path = Path("./data")
    users_file: Path = Path("./users.json")
    log_file: Optional[Path] = Path("./server.log")
    log_level: str = "INFO"
    max_workers: int = 10
    ssl_cert: Optional[Path] = None
    ssl_key: Optional[Path] = None
    ssl_ca: Optional[Path] = None
    require_client_cert: bool = False

    def __post_init__(self):
        # Converte strings para Path quando aplicável
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if isinstance(self.users_file, str):
            self.users_file = Path(self.users_file)
        if isinstance(self.log_file, str):
            self.log_file = Path(self.log_file) if self.log_file else None
        if isinstance(self.ssl_cert, str):
            self.ssl_cert = Path(self.ssl_cert) if self.ssl_cert else None
        if isinstance(self.ssl_key, str):
            self.ssl_key = Path(self.ssl_key) if self.ssl_key else None
        if isinstance(self.ssl_ca, str):
            self.ssl_ca = Path(self.ssl_ca) if self.ssl_ca else None


def load_config(
    config_file: Optional[Union[str, Path]] = None,
    env_prefix: str = "DB_"
) -> Config:
    """
    Carrega configurações de um arquivo JSON e/ou variáveis de ambiente.
    Variáveis de ambiente têm precedência sobre o arquivo.

    Args:
        config_file: Caminho para arquivo de configuração JSON (opcional).
        env_prefix: Prefixo para variáveis de ambiente (ex: DB_HOST, DB_PORT).

    Returns:
        Objeto Config preenchido.
    """
    config = Config()

    # Carrega do arquivo se existir
    if config_file:
        config_path = Path(config_file)
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                file_config = json.load(f)
            for key, value in file_config.items():
                if hasattr(config, key):
                    setattr(config, key, value)

    # Sobrescreve com variáveis de ambiente
    env_vars = {
        "host": os.getenv(f"{env_prefix}HOST"),
        "port": os.getenv(f"{env_prefix}PORT"),
        "data_dir": os.getenv(f"{env_prefix}DATA_DIR"),
        "users_file": os.getenv(f"{env_prefix}USERS_FILE"),
        "log_file": os.getenv(f"{env_prefix}LOG_FILE"),
        "log_level": os.getenv(f"{env_prefix}LOG_LEVEL"),
        "max_workers": os.getenv(f"{env_prefix}MAX_WORKERS"),
        "ssl_cert": os.getenv(f"{env_prefix}SSL_CERT"),
        "ssl_key": os.getenv(f"{env_prefix}SSL_KEY"),
        "ssl_ca": os.getenv(f"{env_prefix}SSL_CA"),
        "require_client_cert": os.getenv(f"{env_prefix}REQUIRE_CLIENT_CERT"),
    }

    for key, value in env_vars.items():
        if value is not None:
            # Converte tipos
            if key in ("port", "max_workers"):
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError(f"Variável de ambiente {env_prefix}{key.upper()} deve ser um número")
            elif key in ("require_client_cert"):
                value = value.lower() in ("true", "1", "yes")
            if hasattr(config, key):
                setattr(config, key, value)

    # Pós-processamento
    config.__post_init__()

    return config


def save_config(config: Config, config_file: Union[str, Path]) -> None:
    """
    Salva a configuração em um arquivo JSON.
    """
    config_dict = {
        "host": config.host,
        "port": config.port,
        "data_dir": str(config.data_dir),
        "users_file": str(config.users_file),
        "log_file": str(config.log_file) if config.log_file else None,
        "log_level": config.log_level,
        "max_workers": config.max_workers,
        "ssl_cert": str(config.ssl_cert) if config.ssl_cert else None,
        "ssl_key": str(config.ssl_key) if config.ssl_key else None,
        "ssl_ca": str(config.ssl_ca) if config.ssl_ca else None,
        "require_client_cert": config.require_client_cert,
    }
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config_dict, f, indent=2, ensure_ascii=False)