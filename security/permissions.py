# security/permissions.py
from typing import List, Set, Optional, Union
from sql.ast import Command, CreateDatabase, DropDatabase, UseDatabase, CreateTable, DropTable, AlterTableAdd, AlterTableDrop, Insert, Select, Update, Delete, ShowTables, DescribeTable


class PermissionChecker:
    """
    Verifica se um usuário tem permissão para executar um comando SQL.
    As permissões são expressas como strings no formato:

        "*"                         -> acesso total
        "admin"                     -> operações administrativas (criar/remover bancos)
        "banco:r"                    -> leitura no banco 'banco'
        "banco:w"                    -> escrita no banco 'banco' (inclui alteração de estrutura e dados)
        "banco:tabela:r"              -> leitura apenas em uma tabela específica (opcional)
        "banco:tabela:w"              -> escrita apenas em uma tabela específica

    A ordem de precedência:
        - "*" sobrescreve tudo.
        - "admin" permite CREATE/DROP DATABASE.
        - Permissões específicas de banco/tabela são verificadas exatamente.
    """

    def __init__(self, user_permissions: List[str]):
        """
        Inicializa com a lista de permissões do usuário.
        """
        self.permissions = set(user_permissions) if user_permissions else set()
        self._is_super = "*" in self.permissions
        self._is_admin = "admin" in self.permissions

    def check(self, command: Command, current_db: Optional[str] = None) -> bool:
        """
        Verifica se o comando é permitido para o usuário.

        Args:
            command: Instância de Command (AST).
            current_db: Banco de dados atualmente selecionado (se aplicável).

        Returns:
            True se permitido, False caso contrário.
        """
        if self._is_super:
            return True

        # Comandos que criam/removem bancos (apenas admin)
        if isinstance(command, (CreateDatabase, DropDatabase)):
            return self._is_admin

        # Comandos que selecionam banco
        if isinstance(command, UseDatabase):
            # Precisa de permissão de leitura no banco (ou escrita)
            db = command.database_name
            return self._has_db_permission(db, 'r') or self._has_db_permission(db, 'w')

        # Comandos que atuam em tabelas dentro do banco atual
        if current_db is None:
            # Se o comando exige banco atual e não há, a verificação será feita depois (ou já deve ter falhado)
            # Aqui podemos retornar False, mas a validação semântica já vai acusar.
            return False

        # Comandos de criação/alteração de tabela (escrita)
        if isinstance(command, (CreateTable, DropTable, AlterTableAdd, AlterTableDrop)):
            # Permissão de escrita no banco inteiro ou na tabela específica?
            # Por simplicidade, exigimos escrita no banco.
            return self._has_db_permission(current_db, 'w')

        # Comandos de manipulação de dados
        if isinstance(command, (Insert, Update, Delete)):
            # Escrita no banco ou na tabela específica
            # Verifica permissão na tabela (se existir) ou no banco
            table = getattr(command, 'table_name', None)
            return self._has_table_permission(current_db, table, 'w') if table else self._has_db_permission(current_db, 'w')

        if isinstance(command, Select):
            table = getattr(command, 'table_name', None)
            return self._has_table_permission(current_db, table, 'r') if table else self._has_db_permission(current_db, 'r')

        # Comandos de metadados
        if isinstance(command, (ShowTables, DescribeTable)):
            # SHOW TABLES precisa de leitura no banco
            if isinstance(command, DescribeTable):
                table = command.table_name
                return self._has_table_permission(current_db, table, 'r')
            return self._has_db_permission(current_db, 'r')

        # Outros comandos (fallback)
        return False

    def _has_db_permission(self, db: str, mode: str) -> bool:
        """Verifica se o usuário tem permissão mode (r/w) no banco db."""
        if self._is_super:
            return True
        # Permissão exata no banco: f"{db}:{mode}"
        perm = f"{db}:{mode}"
        if perm in self.permissions:
            return True
        # Também pode ter permissão genérica "*:r" ou "*:w"? Por simplicidade, não.
        return False

    def _has_table_permission(self, db: str, table: Optional[str], mode: str) -> bool:
        """Verifica permissão em uma tabela específica (ou fallback para banco)."""
        if not table:
            return self._has_db_permission(db, mode)
        # Tenta permissão específica da tabela: f"{db}:{table}:{mode}"
        specific = f"{db}:{table}:{mode}"
        if specific in self.permissions:
            return True
        # Fallback para permissão no banco
        return self._has_db_permission(db, mode)

    @staticmethod
    def parse_permission_string(perm: str) -> tuple:
        """
        Utilitário para interpretar uma string de permissão.
        Retorna (tipo, banco, tabela, modo) onde tipo pode ser 'global', 'db', 'table'.
        Exemplo:
            "*" -> ('global', None, None, None)
            "admin" -> ('admin', None, None, None)
            "meubanco:r" -> ('db', 'meubanco', None, 'r')
            "meubanco:clientes:w" -> ('table', 'meubanco', 'clientes', 'w')
        """
        parts = perm.split(':')
        if len(parts) == 1:
            if parts[0] == '*':
                return ('global', None, None, None)
            else:
                return ('admin', None, None, None) if parts[0] == 'admin' else ('unknown',)
        elif len(parts) == 2:
            # banco:modo
            return ('db', parts[0], None, parts[1])
        elif len(parts) == 3:
            # banco:tabela:modo
            return ('table', parts[0], parts[1], parts[2])
        else:
            return ('invalid',)

    def get_effective_permissions(self, db: Optional[str] = None, table: Optional[str] = None) -> Set[str]:
        """
        Retorna os modos efetivos (r/w) para um dado banco/tabela.
        Útil para depuração ou interfaces.
        """
        modes = set()
        if self._is_super:
            modes.add('r')
            modes.add('w')
            return modes

        # Verifica permissões globais
        if self._is_admin:
            # admin tem tudo, mas para simplificar, retornamos r/w
            modes.add('r')
            modes.add('w')

        # Permissões de banco
        if db:
            if f"{db}:r" in self.permissions:
                modes.add('r')
            if f"{db}:w" in self.permissions:
                modes.add('w')

        # Permissões de tabela (sobrescrevem banco)
        if db and table:
            if f"{db}:{table}:r" in self.permissions:
                modes.add('r')
            if f"{db}:{table}:w" in self.permissions:
                modes.add('w')

        return modes