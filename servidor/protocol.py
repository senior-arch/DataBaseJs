# servidor/protocol.py
import json
import socket
from typing import Any, Dict, Optional


def recv_command(sock: socket.socket) -> Optional[str]:
    """
    Recebe uma linha terminada por \n do socket.
    Retorna a linha decodificada (strip) ou None se a conexão foi fechada.
    """
    data = b''
    while True:
        try:
            chunk = sock.recv(1)
            if not chunk:
                # Conexão fechada
                return None
            if chunk == b'\n':
                break
            data += chunk
        except (socket.error, ConnectionResetError, BrokenPipeError):
            return None
    try:
        return data.decode('utf-8').strip()
    except UnicodeDecodeError:
        return None


def send_response(sock: socket.socket, response: Dict[str, Any]) -> bool:
    """
    Envia uma resposta JSON terminada por \n.
    Retorna True se enviado com sucesso, False em caso de erro.
    """
    try:
        data = json.dumps(response, ensure_ascii=False) + '\n'
        sock.sendall(data.encode('utf-8'))
        return True
    except (socket.error, ConnectionResetError, BrokenPipeError):
        return False