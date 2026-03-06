# security/hashing.py
import bcrypt
from typing import Optional


def hash_password(password: str, rounds: int = 12) -> str:
    """
    Gera um hash bcrypt para a senha fornecida.

    Args:
        password: Senha em texto puro.
        rounds: Número de rounds (custo computacional). Padrão 12.

    Returns:
        String com o hash (inclui salt e parâmetros).
    """
    if not isinstance(password, str):
        raise TypeError("password deve ser string")
    salt = bcrypt.gensalt(rounds=rounds)
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """
    Verifica se a senha corresponde ao hash.

    Args:
        password: Senha em texto puro.
        password_hash: Hash previamente gerado.

    Returns:
        True se a senha for válida, False caso contrário.
    """
    if not isinstance(password, str) or not isinstance(password_hash, str):
        return False
    try:
        return bcrypt.checkpw(
            password.encode('utf-8'),
            password_hash.encode('utf-8')
        )
    except (ValueError, TypeError):
        # Hash inválido ou mal formatado
        return False


def needs_rehash(password_hash: str, rounds: int = 12) -> bool:
    """
    Verifica se o hash foi gerado com rounds inferiores ao recomendado.
    Útil para fazer upgrade gradual de segurança.

    Args:
        password_hash: Hash existente.
        rounds: Número de rounds desejado.

    Returns:
        True se o hash deve ser atualizado.
    """
    try:
        return bcrypt.check_password_hash(password_hash, rounds=rounds) is False
    except ValueError:
        return True