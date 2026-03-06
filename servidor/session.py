# servidor/session.py
import socket
import logging
from typing import Optional, Dict, Any

from core.database import Database
# servidor/session.py (trecho corrigido)
from core.errors import (
    DatabaseNotFoundError as DatabaseNotFound,
    TableNotFoundError as TableNotFound,
    ColumnNotFoundError as ColumnNotFound,
    ValidationError as CoreValidationError
)
from sql.parser import Parser, ParseError
from sql.lexer import Lexer
from sql.validator import Validator, ValidationError as SemanticValidationError
from sql.ast import (
    Command, CreateDatabase, DropDatabase, UseDatabase,
    CreateTable, DropTable, AlterTableAdd, AlterTableDrop,
    Insert, Select, Update, Delete, ShowTables, DescribeTable
)
from .protocol import recv_command, send_response
from .auth import AuthManager

logger = logging.getLogger(__name__)


class Session:
    """
    Representa uma conexão de cliente.
    Mantém estado (banco atual, usuário, permissões) e processa comandos.
    """

    def __init__(self, client_sock: socket.socket, addr: tuple, auth_mgr: AuthManager, data_dir: str):
        self.sock = client_sock
        self.addr = addr
        self.auth_mgr = auth_mgr
        self.data_dir = data_dir
        self.user: Optional[str] = None
        self.permissions: list = []
        self.db: Optional[Database] = None
        self.current_db_name: Optional[str] = None
        self.authenticated = False
        self.running = True

    def run(self):
        """Loop principal da sessão."""
        logger.info(f"Nova conexão de {self.addr}")
        try:
            while self.running:
                cmd_line = recv_command(self.sock)
                if cmd_line is None:
                    break  # cliente fechou conexão

                if not cmd_line:
                    continue  # linha vazia

                # Se não autenticado, o primeiro comando deve ser AUTH
                if not self.authenticated:
                    if cmd_line.upper().startswith("AUTH"):
                        self._handle_auth(cmd_line)
                    else:
                        self._send_error("Autenticação necessária. Use AUTH usuario senha")
                    continue

                # Processa comando SQL
                self._process_command(cmd_line)

        except Exception as e:
            logger.exception(f"Erro na sessão {self.addr}: {e}")
        finally:
            self.sock.close()
            logger.info(f"Conexão encerrada: {self.addr}")

    def _handle_auth(self, cmd_line: str):
        """Processa comando AUTH."""
        parts = cmd_line.split()
        if len(parts) != 3:
            self._send_error("Formato: AUTH usuario senha")
            return
        _, username, password = parts
        user_data = self.auth_mgr.authenticate(username, password)
        if user_data:
            self.user = username
            self.permissions = user_data.get("permissions", [])
            self.authenticated = True
            self._send_ok(f"Autenticado como {username}")
            logger.info(f"Usuário {username} autenticado de {self.addr}")
        else:
            self._send_error("Usuário ou senha inválidos")

    def _process_command(self, cmd_line: str):
        """Analisa, valida e executa um comando SQL."""
        try:
            # Tokenização e parsing
            lexer = Lexer(cmd_line)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            command = parser.parse()

            # Validação de permissões
            if not self._check_permission(command):
                self._send_error("Permissão negada")
                return

            # Validação semântica (se houver banco selecionado)
            if self.db:
                validator = Validator(self.db)
                validator.current_database = self.current_db_name
                validator.validate(command)

            # Execução
            result = self._execute(command)
            self._send_response(result)

        except ParseError as e:
            self._send_error(f"Erro de sintaxe: {e}")
        except SemanticValidationError as e:
            self._send_error(f"Erro semântico: {e}")
        except CoreValidationError as e:
            self._send_error(f"Erro no banco de dados: {e}")
        except Exception as e:
            logger.exception("Erro inesperado no processamento")
            self._send_error(f"Erro interno: {e}")

    def _check_permission(self, cmd: Command) -> bool:
        """
        Verifica se o usuário atual tem permissão para executar o comando.
        Regras básicas:
        - * permite tudo
        - Comandos de banco (CREATE/DROP DATABASE) requerem "admin"
        - Comandos de tabela/dados requerem permissão no banco específico (ex: "banco1:rw")
        """
        if "*" in self.permissions:
            return True

        # Comandos que não atuam em banco específico (ex: SHOW TABLES depende do banco atual)
        if isinstance(cmd, (CreateDatabase, DropDatabase)):
            # Apenas admin pode criar/remover bancos
            return "admin" in self.permissions

        if isinstance(cmd, UseDatabase):
            # Pode usar qualquer banco? Permissão baseada no nome do banco
            perm = f"{cmd.database_name}:r"  # use requer leitura
            return perm in self.permissions

        # Comandos que exigem um banco atual
        if not self.current_db_name:
            self._send_error("Nenhum banco de dados selecionado")
            return False

        # Permissão para o banco atual
        if isinstance(cmd, (CreateTable, DropTable, AlterTableAdd, AlterTableDrop)):
            perm = f"{self.current_db_name}:w"
        elif isinstance(cmd, (Select, ShowTables, DescribeTable)):
            perm = f"{self.current_db_name}:r"
        elif isinstance(cmd, (Insert, Update, Delete)):
            perm = f"{self.current_db_name}:w"
        else:
            perm = f"{self.current_db_name}:r"  # default

        return perm in self.permissions

    def _execute(self, cmd: Command) -> Dict[str, Any]:
        """
        Executa o comando no banco de dados atual.
        Retorna um dicionário com os resultados (para ser serializado em JSON).
        """
        # Comandos que não dependem de banco atual (mas podem alterá-lo)
        if isinstance(cmd, CreateDatabase):
            db = Database(self.data_dir)
            db.create_database(cmd.database_name)
            return {"status": "ok", "message": f"Banco {cmd.database_name} criado"}

        if isinstance(cmd, DropDatabase):
            db = Database(self.data_dir)
            db.drop_database(cmd.database_name)
            if self.current_db_name == cmd.database_name:
                self.current_db_name = None
                self.db = None
            return {"status": "ok", "message": f"Banco {cmd.database_name} removido"}

        if isinstance(cmd, UseDatabase):
            # Abre o banco
            self.db = Database(self.data_dir, cmd.database_name)
            self.current_db_name = cmd.database_name
            return {"status": "ok", "message": f"Banco {cmd.database_name} selecionado"}

        # A partir daqui, precisa de banco atual
        if not self.db:
            raise CoreValidationError("Nenhum banco de dados selecionado")

        if isinstance(cmd, CreateTable):
            self.db.create_table(cmd.table_name, cmd.columns)
            return {"status": "ok", "message": f"Tabela {cmd.table_name} criada"}

        if isinstance(cmd, DropTable):
            self.db.drop_table(cmd.table_name)
            return {"status": "ok", "message": f"Tabela {cmd.table_name} removida"}

        if isinstance(cmd, AlterTableAdd):
            table = self.db.get_table(cmd.table_name)
            table.add_column(cmd.column)
            return {"status": "ok", "message": f"Coluna {cmd.column.name} adicionada"}

        if isinstance(cmd, AlterTableDrop):
            table = self.db.get_table(cmd.table_name)
            table.drop_column(cmd.column_name)
            return {"status": "ok", "message": f"Coluna {cmd.column_name} removida"}

        if isinstance(cmd, Insert):
            table = self.db.get_table(cmd.table_name)
            # Insere cada linha
            ids = []
            for values in cmd.values:
                # Se columns não foi especificado, usa todas as colunas da tabela
                cols = cmd.columns if cmd.columns else [col.name for col in table.schema.columns]
                data = dict(zip(cols, values))
                record_id = table.insert(data)
                ids.append(record_id)
            return {"status": "ok", "message": f"Inseridos {len(ids)} registros", "ids": ids}

        if isinstance(cmd, Select):
            table = self.db.get_table(cmd.table_name)
            # Converte where expression para predicado (simplificado: só suporta operadores básicos)
            records = table.select(self._where_to_predicate(cmd.where)) if cmd.where else table.select()
            # Projeta colunas
            if cmd.columns != ['*']:
                result = []
                for rec in records:
                    projected = {col: rec.data.get(col) for col in cmd.columns}
                    result.append({"id": rec.id, "data": projected, "criado_em": rec.created_at.isoformat()})
            else:
                result = [{"id": rec.id, "data": rec.data, "criado_em": rec.created_at.isoformat()} for rec in records]
            return {"status": "ok", "rows": result, "count": len(result)}

        if isinstance(cmd, Update):
            table = self.db.get_table(cmd.table_name)
            predicate = self._where_to_predicate(cmd.where) if cmd.where else None
            updates = dict(cmd.assignments)
            updated = table.update(updates, predicate)
            return {"status": "ok", "message": f"Atualizados {updated} registros"}

        if isinstance(cmd, Delete):
            table = self.db.get_table(cmd.table_name)
            predicate = self._where_to_predicate(cmd.where) if cmd.where else None
            deleted = table.delete(predicate)
            return {"status": "ok", "message": f"Removidos {deleted} registros"}

        if isinstance(cmd, ShowTables):
            tables = self.db.list_tables()
            return {"status": "ok", "tables": tables}

        if isinstance(cmd, DescribeTable):
            table = self.db.get_table(cmd.table_name)
            schema = table.schema
            columns_info = []
            for col in schema.columns:
                columns_info.append({
                    "name": col.name,
                    "type": col.type.value,
                    "not_null": col.not_null,
                    "primary_key": col.primary_key,
                    "auto_increment": col.auto_increment,
                    "unique": col.unique,
                    "default": col.default
                })
            return {"status": "ok", "columns": columns_info, "record_count": schema.record_count}

        raise CoreValidationError(f"Comando não implementado: {type(cmd).__name__}")

    def _where_to_predicate(self, expr):
        """
        Converte uma expressão WHERE da AST em uma função predicado para o core.
        Implementação simplificada: apenas suporta comparações binárias simples.
        """
        if expr is None:
            return None

        from sql.ast import BinaryOp, ColumnRef, Literal

        def evaluate(record_data):
            return self._evaluate_expression(expr, record_data)

        return evaluate

    def _evaluate_expression(self, expr, record_data):
        """Avalia uma expressão booleana para um registro específico."""
        from sql.ast import BinaryOp, ColumnRef, Literal

        if isinstance(expr, BinaryOp):
            left = self._evaluate_expression(expr.left, record_data)
            right = self._evaluate_expression(expr.right, record_data)
            op = expr.operator.upper()
            if op == '=':
                return left == right
            elif op == '<>':
                return left != right
            elif op == '<':
                return left < right
            elif op == '>':
                return left > right
            elif op == '<=':
                return left <= right
            elif op == '>=':
                return left >= right
            elif op == 'LIKE':
                # Like simples (case-sensitive, % como coringa)
                import fnmatch
                return fnmatch.fnmatch(str(left), str(right).replace('%', '*'))
            elif op == 'AND':
                return left and right
            elif op == 'OR':
                return left or right
            else:
                raise CoreValidationError(f"Operador não suportado: {op}")
        elif isinstance(expr, ColumnRef):
            return record_data.get(expr.name)
        elif isinstance(expr, Literal):
            return expr.value
        else:
            raise CoreValidationError(f"Expressão inválida: {expr}")

    def _send_ok(self, message: str = "OK", data: Any = None):
        """Envia resposta de sucesso."""
        response = {"status": "ok", "message": message}
        if data is not None:
            response["data"] = data
        send_response(self.sock, response)

    def _send_error(self, message: str):
        """Envia resposta de erro."""
        send_response(self.sock, {"status": "error", "message": message})

    def _send_response(self, result: Dict[str, Any]):
        """Envia resposta genérica."""
        send_response(self.sock, result)