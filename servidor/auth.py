# servidor/auth.py
import json
import bcrypt
from pathlib import Path
from typing import Dict, Optional, List, Set


class AuthManager:
    """
    Gerencia usuários, hashing de senhas e permissões.
    """

    def __init__(self, users_file: str):
        self.users_file = Path(users_file)
        self.users: Dict[str, Dict] = {}
        self.load()

    def load(self):
        """Carrega o arquivo de usuários."""
        if not self.users_file.exists():
            # Cria um admin padrão se não existir
            self.users = {
                "admin": {
                    "password_hash": self._hash_password("admin"),
                    "permissions": ["*"]  # * significa acesso total
                }
            }
            self.save()
        else:
            with open(self.users_file, 'r', encoding='utf-8') as f:
                self.users = json.load(f)

    def save(self):
        """Salva o arquivo de usuários."""
        with open(self.users_file, 'w', encoding='utf-8') as f:
            json.dump(self.users, f, indent=2, ensure_ascii=False)

    def _hash_password(self, password: str) -> str:
        """Gera hash bcrypt de uma senha."""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verifica se a senha corresponde ao hash."""
        return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """
        Autentica um usuário.
        Retorna os dados do usuário (incluindo permissões) se OK, None caso contrário.
        """
        user = self.users.get(username)
        if not user:
            return None
        if self.verify_password(password, user["password_hash"]):
            return user
        return None

    def add_user(self, username: str, password: str, permissions: List[str] = None):
        """Adiciona um novo usuário."""
        if username in self.users:
            raise ValueError("Usuário já existe")
        self.users[username] = {
            "password_hash": self._hash_password(password),
            "permissions": permissions or []
        }
        self.save()

    def change_password(self, username: str, new_password: str):
        """Altera a senha de um usuário."""
        if username not in self.users:
            raise ValueError("Usuário não existe")
        self.users[username]["password_hash"] = self._hash_password(new_password)
        self.save()

    def check_permission(self, username: str, permission: str) -> bool:
        """
        Verifica se o usuário tem uma determinada permissão.
        A permissão pode ser algo como "banco1:rw" ou "*" para tudo.
        """
        user = self.users.get(username)
        if not user:
            return False
        perms = user.get("permissions", [])
        if "*" in perms:
            return True
        return permission in perms