# sql/validator.py
from typing import List, Optional, Any
from .ast import (
    Command, CreateDatabase, DropDatabase, UseDatabase,
    CreateTable, DropTable, AlterTableAdd, AlterTableDrop,
    Insert, Select, Update, Delete, ShowTables, DescribeTable,
    ColumnDefinition, DataType, Expression, BinaryOp, Literal, ColumnRef
)
from core.database import Database
from core.errors import TableNotFound, ColumnNotFound, DatabaseNotFound


class ValidationError(Exception):
    pass


class Validator:
    """Validador semântico que percorre a AST e verifica contra o schema atual."""

    def __init__(self, db: Database):
        self.db = db
        self.current_database = None  # será setado externamente

    def validate(self, command: Command) -> None:
        """Valida um comando, levantando ValidationError se inválido."""
        method_name = f"validate_{command.__class__.__name__.lower()}"
        method = getattr(self, method_name, self._default_validate)
        method(command)

    def _default_validate(self, command: Command) -> None:
        """Validação padrão: não faz nada."""
        pass

    # ========== Comandos de banco ==========

    def validate_createdatabase(self, cmd: CreateDatabase) -> None:
        # Verifica se já existe (opcional, pode ser feito no core)
        if self.db.exists(cmd.database_name):
            raise ValidationError(f"Banco de dados '{cmd.database_name}' já existe")

    def validate_dropdatabase(self, cmd: DropDatabase) -> None:
        if not self.db.exists(cmd.database_name):
            raise ValidationError(f"Banco de dados '{cmd.database_name}' não existe")

    def validate_usedatabase(self, cmd: UseDatabase) -> None:
        if not self.db.exists(cmd.database_name):
            raise ValidationError(f"Banco de dados '{cmd.database_name}' não existe")

    # ========== Comandos de tabela ==========

    def _ensure_database_selected(self):
        if not self.current_database:
            raise ValidationError("Nenhum banco de dados selecionado")

    def _table_exists(self, table_name: str) -> bool:
        return self.db.has_table(table_name)

    def _get_table_schema(self, table_name: str):
        try:
            return self.db.get_table(table_name).schema
        except TableNotFound:
            return None

    def validate_createtable(self, cmd: CreateTable) -> None:
        self._ensure_database_selected()
        if self._table_exists(cmd.table_name):
            raise ValidationError(f"Tabela '{cmd.table_name}' já existe")
        # Valida tipos e propriedades das colunas
        for col in cmd.columns:
            self._validate_column_definition(col)

    def validate_droptable(self, cmd: DropTable) -> None:
        self._ensure_database_selected()
        if not self._table_exists(cmd.table_name):
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")

    def validate_altertableadd(self, cmd: AlterTableAdd) -> None:
        self._ensure_database_selected()
        if not self._table_exists(cmd.table_name):
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")
        schema = self._get_table_schema(cmd.table_name)
        if any(col.name == cmd.column.name for col in schema.columns):
            raise ValidationError(f"Coluna '{cmd.column.name}' já existe na tabela")
        self._validate_column_definition(cmd.column)

    def validate_altertabledrop(self, cmd: AlterTableDrop) -> None:
        self._ensure_database_selected()
        if not self._table_exists(cmd.table_name):
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")
        schema = self._get_table_schema(cmd.table_name)
        if not any(col.name == cmd.column_name for col in schema.columns):
            raise ValidationError(f"Coluna '{cmd.column_name}' não existe na tabela")

    def _validate_column_definition(self, col: ColumnDefinition):
        # Validações adicionais: auto_increment só pode ser INTEGER e primary key
        if col.auto_increment:
            if col.type != DataType.INTEGER:
                raise ValidationError(f"AUTO_INCREMENT só pode ser usado em colunas INTEGER")
            if not col.primary_key:
                raise ValidationError(f"AUTO_INCREMENT requer PRIMARY KEY")
        if col.primary_key and col.not_null is False:
            # PRIMARY KEY implica NOT NULL
            pass  # podemos aceitar, mas o schema deve forçar not_null

    # ========== Comandos de manipulação de dados ==========

    def validate_insert(self, cmd: Insert) -> None:
        self._ensure_database_selected()
        schema = self._get_table_schema(cmd.table_name)
        if not schema:
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")

        col_names = [col.name for col in schema.columns]

        # Se columns foi especificado, verifica se todas existem
        if cmd.columns:
            for col in cmd.columns:
                if col not in col_names:
                    raise ValidationError(f"Coluna '{col}' não existe na tabela '{cmd.table_name}'")
        else:
            cmd.columns = col_names  # assume todas as colunas

        # Verifica número de valores
        expected_cols = len(cmd.columns)
        for row in cmd.values:
            if len(row) != expected_cols:
                raise ValidationError(f"Número de valores ({len(row)}) não corresponde ao número de colunas ({expected_cols})")

        # Valida tipos (opcional, pode ser feito no core)
        for row in cmd.values:
            for col_name, value in zip(cmd.columns, row):
                col_def = next(c for c in schema.columns if c.name == col_name)
                self._validate_type(value, col_def)

    def validate_select(self, cmd: Select) -> None:
        self._ensure_database_selected()
        schema = self._get_table_schema(cmd.table_name)
        if not schema:
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")

        col_names = [col.name for col in schema.columns]
        if cmd.columns != ['*']:
            for col in cmd.columns:
                if col not in col_names:
                    raise ValidationError(f"Coluna '{col}' não existe na tabela '{cmd.table_name}'")

        if cmd.where:
            self._validate_expression(cmd.where, schema)

    def validate_update(self, cmd: Update) -> None:
        self._ensure_database_selected()
        schema = self._get_table_schema(cmd.table_name)
        if not schema:
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")

        col_names = [col.name for col in schema.columns]
        for col, value in cmd.assignments:
            if col not in col_names:
                raise ValidationError(f"Coluna '{col}' não existe na tabela '{cmd.table_name}'")
            col_def = next(c for c in schema.columns if c.name == col)
            self._validate_type(value, col_def)

        if cmd.where:
            self._validate_expression(cmd.where, schema)

    def validate_delete(self, cmd: Delete) -> None:
        self._ensure_database_selected()
        schema = self._get_table_schema(cmd.table_name)
        if not schema:
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")
        if cmd.where:
            self._validate_expression(cmd.where, schema)

    def validate_showtables(self, cmd: ShowTables) -> None:
        self._ensure_database_selected()

    def validate_describetable(self, cmd: DescribeTable) -> None:
        self._ensure_database_selected()
        if not self._table_exists(cmd.table_name):
            raise ValidationError(f"Tabela '{cmd.table_name}' não existe")

    # ========== Validação de expressões ==========

    def _validate_expression(self, expr: Expression, schema):
        """Valida uma expressão WHERE, verificando se as colunas existem."""
        if isinstance(expr, BinaryOp):
            self._validate_expression(expr.left, schema)
            self._validate_expression(expr.right, schema)
        elif isinstance(expr, ColumnRef):
            col_names = [col.name for col in schema.columns]
            if expr.name not in col_names:
                raise ValidationError(f"Coluna '{expr.name}' não existe")
        elif isinstance(expr, Literal):
            pass  # literal sempre válido
        else:
            raise ValidationError(f"Expressão inválida: {expr}")

    def _validate_type(self, value: Any, col_def: ColumnDefinition):
        """Valida se o valor é compatível com o tipo da coluna (básico)."""
        if value is None:
            if col_def.not_null:
                raise ValidationError(f"Coluna '{col_def.name}' não pode ser NULL")
            return

        type_map = {
            DataType.INTEGER: int,
            DataType.DECIMAL: (float, int),
            DataType.BOOLEAN: bool,
            DataType.VARCHAR: str,
            DataType.TEXT: str,
            DataType.DATE: str,  # simplificado
        }
        expected = type_map.get(col_def.type)
        if expected and not isinstance(value, expected):
            raise ValidationError(
                f"Valor '{value}' para coluna '{col_def.name}' deve ser do tipo {col_def.type.value}"
            )