# servidor/main.py
import argparse
import logging
import sys
from pathlib import Path

from .server import Server

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('server.log')
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Servidor de Banco de Dados JSON")
    parser.add_argument('--host', default='0.0.0.0', help='Endereço de bind (padrão: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5432, help='Porta (padrão: 5432)')
    parser.add_argument('--data-dir', default='./data', help='Diretório dos bancos de dados')
    parser.add_argument('--users-file', default='./users.json', help='Arquivo de usuários')
    parser.add_argument('--max-workers', type=int, default=10, help='Máximo de threads simultâneas')
    # Futuro: --ssl-cert, --ssl-key
    return parser.parse_args()


def main():
    args = parse_args()

    # Cria diretório de dados se não existir
    Path(args.data_dir).mkdir(parents=True, exist_ok=True)

    # Cria arquivo users.json padrão se não existir (será criado pelo AuthManager)
    # O AuthManager já cria se não existir

    server = Server(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        users_file=args.users_file,
        max_workers=args.max_workers
    )

    try:
        logger.info("Iniciando servidor...")
        server.start()
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário.")
        server.stop()
    except Exception as e:
        logger.exception("Erro fatal")
        sys.exit(1)


if __name__ == '__main__':
    main()