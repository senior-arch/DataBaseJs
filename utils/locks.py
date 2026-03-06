# utils/locks.py
import threading
import fcntl
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional, Union


class FileLock:
    """
    Lock baseado em arquivo usando fcntl (Unix-like).
    Funciona entre processos e threads.
    """

    def __init__(self, lock_file: Union[str, Path], timeout: float = None):
        """
        Args:
            lock_file: Caminho para o arquivo de lock.
            timeout: Tempo máximo (segundos) para esperar pelo lock.
                     Se None, espera indefinidamente.
        """
        self.lock_file = Path(lock_file)
        self.timeout = timeout
        self.fd = None

    @contextmanager
    def acquire(self, shared: bool = False):
        """
        Context manager para adquirir o lock.
        shared: True para lock compartilhado (leitura), False para exclusivo (escrita).
        """
        self.fd = open(self.lock_file, 'w')
        try:
            # Tenta adquirir o lock
            if shared:
                lock_type = fcntl.LOCK_SH
            else:
                lock_type = fcntl.LOCK_EX

            if self.timeout is not None:
                # Tenta com timeout (LOCK_NB + polling)
                start = time.time()
                while True:
                    try:
                        fcntl.flock(self.fd, lock_type | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.time() - start > self.timeout:
                            raise TimeoutError(f"Não foi possível adquirir lock em {self.timeout}s")
                        time.sleep(0.05)
            else:
                # Espera indefinidamente
                fcntl.flock(self.fd, lock_type)

            yield
        finally:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()
            self.fd = None


class LockManager:
    """
    Gerencia locks em memória (por nome) para sincronização entre threads.
    Útil para recursos que não precisam de locks entre processos.
    """

    def __init__(self):
        self._locks: Dict[str, threading.RLock] = {}
        self._global_lock = threading.Lock()

    def get_lock(self, name: str) -> threading.RLock:
        """
        Obtém ou cria um lock reentrante para o nome especificado.
        """
        with self._global_lock:
            if name not in self._locks:
                self._locks[name] = threading.RLock()
            return self._locks[name]

    @contextmanager
    def acquire(self, name: str, blocking: bool = True, timeout: float = -1):
        """
        Adquire o lock para o nome especificado.
        """
        lock = self.get_lock(name)
        acquired = lock.acquire(blocking=blocking, timeout=timeout)
        if not acquired:
            raise TimeoutError(f"Não foi possível adquirir lock '{name}'")
        try:
            yield
        finally:
            lock.release()


# Exemplo de uso:
# lock_mgr = LockManager()
# with lock_mgr.acquire("tabela_clientes"):
#     # operação crítica