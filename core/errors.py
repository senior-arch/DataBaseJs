# core/errors.py
"""Exceções personalizadas para o core do banco de dados."""


class DatabaseError(Exception):
    """Erro base para todas as exceções do banco de dados."""
    pass


class DatabaseNotFoundError(DatabaseError):
    """Banco de dados não encontrado."""
    pass


class TableNotFoundError(DatabaseError):
    """Tabela não encontrada."""
    pass


class ColumnNotFoundError(DatabaseError):
    """Coluna não encontrada."""
    pass


class ValidationError(DatabaseError):
    """Erro de validação de dados."""
    pass


class DuplicateEntryError(DatabaseError):
    """Registro duplicado (violação de unique)."""
    pass


class IntegrityError(DatabaseError):
    """Erro de integridade (ex: foreign key, not null)."""
    pass


# Aliases para compatibilidade (opcional)
DatabaseNotFound = DatabaseNotFoundError
TableNotFound = TableNotFoundError
ColumnNotFound = ColumnNotFoundError