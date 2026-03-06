# sql/ast.py
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


class DataType(Enum):
    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    TEXT = "TEXT"


@dataclass
class ColumnDefinition:
    """Definição de coluna dentro de CREATE TABLE ou ALTER ADD"""
    name: str
    type: DataType
    not_null: bool = False
    primary_key: bool = False
    auto_increment: bool = False
    unique: bool = False
    default: Optional[Any] = None


@dataclass
class Expression:
    """Base para expressões (usado em WHERE)"""
    pass


@dataclass
class BinaryOp(Expression):
    """Expressão binária: left operador right"""
    left: Union[str, Expression, Any]
    operator: str  # '=', '<', '>', '<=', '>=', '<>', 'LIKE', 'AND', 'OR'
    right: Union[str, Expression, Any]


@dataclass
class Literal(Expression):
    """Valor literal (número, string, booleano)"""
    value: Any


@dataclass
class ColumnRef(Expression):
    """Referência a uma coluna"""
    name: str


# ==================== Comandos SQL ====================

class Command:
    """Classe base para todos os comandos"""
    pass


@dataclass
class CreateDatabase(Command):
    database_name: str


@dataclass
class DropDatabase(Command):
    database_name: str


@dataclass
class UseDatabase(Command):
    database_name: str


@dataclass
class CreateTable(Command):
    table_name: str
    columns: List[ColumnDefinition]


@dataclass
class DropTable(Command):
    table_name: str


@dataclass
class AlterTableAdd(Command):
    table_name: str
    column: ColumnDefinition


@dataclass
class AlterTableDrop(Command):
    table_name: str
    column_name: str


@dataclass
class Insert(Command):
    table_name: str
    columns: Optional[List[str]]  # None significa todas as colunas
    values: List[List[Any]]       # suporta múltiplas linhas


@dataclass
class Select(Command):
    columns: List[str]             # ['*'] significa todas
    table_name: str
    where: Optional[Expression] = None


@dataclass
class Update(Command):
    table_name: str
    assignments: List[tuple[str, Any]]  # [(coluna, valor), ...]
    where: Optional[Expression] = None


@dataclass
class Delete(Command):
    table_name: str
    where: Optional[Expression] = None


@dataclass
class ShowTables(Command):
    """SHOW TABLES"""
    pass


@dataclass
class DescribeTable(Command):
    """DESCRIBE nome"""
    table_name: str