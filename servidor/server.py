# servidor/server.py
import socket
import threading
import logging
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from .session import Session
from .auth import AuthManager

logger = logging.getLogger(__name__)


class Server:
    """
    Servidor TCP que aceita conexões e cria uma thread para cada cliente.
    """

    def __init__(self, host: str, port: int, data_dir: str, users_file: str,
                 max_workers: int = 10, ssl_context=None):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.users_file = users_file
        self.max_workers = max_workers
        self.ssl_context = ssl_context  # Para futura implementação TLS
        self.socket: Optional[socket.socket] = None
        self.running = False
        self.auth_mgr = AuthManager(users_file)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def start(self):
        """Inicia o servidor e começa a aceitar conexões."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.bind((self.host, self.port))
            self.socket.listen(5)
            self.running = True
            logger.info(f"Servidor ouvindo em {self.host}:{self.port}")

            while self.running:
                try:
                    client_sock, addr = self.socket.accept()
                    logger.info(f"Conexão recebida de {addr}")
                    # Se SSL configurado, envolve o socket
                    if self.ssl_context:
                        client_sock = self.ssl_context.wrap_socket(client_sock, server_side=True)
                    # Dispara thread para tratar o cliente
                    self.executor.submit(self._handle_client, client_sock, addr)
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.exception(f"Erro ao aceitar conexão: {e}")

        except Exception as e:
            logger.exception(f"Erro no servidor: {e}")
        finally:
            self.stop()

    def stop(self):
        """Para o servidor e libera recursos."""
        self.running = False
        if self.socket:
            self.socket.close()
        self.executor.shutdown(wait=False)
        logger.info("Servidor parado.")

    def _handle_client(self, client_sock: socket.socket, addr: tuple):
        """Processa um cliente em uma thread separada."""
        try:
            session = Session(client_sock, addr, self.auth_mgr, self.data_dir)
            session.run()
        except Exception as e:
            logger.exception(f"Erro no handler do cliente {addr}: {e}")
        finally:
            client_sock.close()