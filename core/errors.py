# core/errors.py
"""Exceções personalizadas para o core do banco de dados."""


class DatabaseError(Exception):
    """Erro base para todas as exceções do banco de dados."""
    pass


# ========== Erros de Banco de Dados ==========

class DatabaseNotFoundError(DatabaseError):
    """Banco de dados não encontrado."""
    pass


class DatabaseAlreadyExistsError(DatabaseError):
    """Banco de dados já existe."""
    pass


# ========== Erros de Tabela ==========

class TableNotFoundError(DatabaseError):
    """Tabela não encontrada."""
    pass


class TableAlreadyExistsError(DatabaseError):
    """Tabela já existe."""
    pass


# ========== Erros de Coluna ==========

class ColumnNotFoundError(DatabaseError):
    """Coluna não encontrada."""
    pass


class ColumnAlreadyExistsError(DatabaseError):
    """Coluna já existe na tabela."""
    pass


# ========== Erros de Validação ==========

class ValidationError(DatabaseError):
    """Erro de validação de dados."""
    pass


class InvalidSchemaError(ValidationError):
    """Erro no schema da tabela (estrutura inválida)."""
    pass


class InvalidDataError(ValidationError):
    """Erro nos dados (tipo, formato, etc.)."""
    pass


class DuplicateEntryError(DatabaseError):
    """Registro duplicado (violação de unique)."""
    pass


class IntegrityError(DatabaseError):
    """Erro de integridade (ex: not null, foreign key)."""
    pass


# ========== Erros de Operação ==========

class OperationError(DatabaseError):
    """Erro em operação de banco de dados."""
    pass


class LockTimeoutError(OperationError):
    """Timeout ao tentar adquirir lock."""
    pass


# ========== Aliases para compatibilidade ==========
DatabaseNotFound = DatabaseNotFoundError
TableNotFound = TableNotFoundError
ColumnNotFound = ColumnNotFoundError
TableAlreadyExists = TableAlreadyExistsError
DatabaseAlreadyExists = DatabaseAlreadyExistsError