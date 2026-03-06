# security/encryption.py
import ssl
import logging
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


def create_ssl_context(
    certfile: Union[str, Path],
    keyfile: Union[str, Path],
    cafile: Optional[Union[str, Path]] = None,
    require_client_cert: bool = False,
    purpose: ssl.Purpose = ssl.Purpose.CLIENT_AUTH
) -> ssl.SSLContext:
    """
    Cria um contexto SSL configurado para servidor ou cliente.

    Args:
        certfile: Caminho para o certificado (PEM).
        keyfile: Caminho para a chave privada (PEM).
        cafile: Caminho para o arquivo de CA (para verificação de cliente).
        require_client_cert: Se True, exige certificado do cliente (mútua autenticação).
        purpose: Propósito do contexto (CLIENT_AUTH para servidor, SERVER_AUTH para cliente).

    Returns:
        Contexto SSL configurado.
    """
    certfile = Path(certfile)
    keyfile = Path(keyfile)

    if not certfile.exists():
        raise FileNotFoundError(f"Arquivo de certificado não encontrado: {certfile}")
    if not keyfile.exists():
        raise FileNotFoundError(f"Arquivo de chave não encontrado: {keyfile}")
    if cafile and not Path(cafile).exists():
        raise FileNotFoundError(f"Arquivo CA não encontrado: {cafile}")

    # Cria contexto com versões TLS modernas (exclui SSLv2, SSLv3, TLSv1, TLSv1.1)
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER if purpose == ssl.Purpose.CLIENT_AUTH else ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.set_ciphers('ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS')

    try:
        context.load_cert_chain(certfile, keyfile)
    except Exception as e:
        raise RuntimeError(f"Erro ao carregar certificado/chave: {e}") from e

    if cafile:
        try:
            context.load_verify_locations(cafile=cafile)
        except Exception as e:
            raise RuntimeError(f"Erro ao carregar CA: {e}") from e
        if require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_OPTIONAL
    else:
        context.verify_mode = ssl.CERT_NONE

    return context


def wrap_server_socket(
    sock,
    certfile: Union[str, Path],
    keyfile: Union[str, Path],
    cafile: Optional[Union[str, Path]] = None,
    require_client_cert: bool = False
):
    """
    Envolve um socket do servidor com TLS.

    Args:
        sock: Socket TCP já criado e em listening.
        certfile, keyfile, cafile, require_client_cert: veja create_ssl_context.

    Returns:
        Socket envolvido com SSL.
    """
    context = create_ssl_context(
        certfile=certfile,
        keyfile=keyfile,
        cafile=cafile,
        require_client_cert=require_client_cert,
        purpose=ssl.Purpose.CLIENT_AUTH
    )
    return context.wrap_socket(sock, server_side=True)


def wrap_client_socket(
    sock,
    server_hostname: str,
    certfile: Optional[Union[str, Path]] = None,
    keyfile: Optional[Union[str, Path]] = None,
    cafile: Optional[Union[str, Path]] = None,
    verify_cert: bool = True
):
    """
    Envolve um socket do cliente com TLS.

    Args:
        sock: Socket TCP conectado.
        server_hostname: Nome do servidor para verificação SNI.
        certfile, keyfile: Certificado e chave do cliente (para autenticação mútua).
        cafile: CA para verificar o servidor.
        verify_cert: Se False, não verifica o certificado do servidor (apenas para testes).

    Returns:
        Socket envolvido com SSL.
    """
    purpose = ssl.Purpose.SERVER_AUTH
    context = ssl.create_default_context(purpose=purpose, cafile=cafile)

    if not verify_cert:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    if certfile and keyfile:
        context.load_cert_chain(certfile, keyfile)

    return context.wrap_socket(sock, server_hostname=server_hostname)
