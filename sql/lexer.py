# sql/lexer.py
import re
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional


class TokenType(Enum):
    # Palavras-chave
    CREATE = "CREATE"
    DATABASE = "DATABASE"
    DROP = "DROP"
    USE = "USE"
    TABLE = "TABLE"
    ALTER = "ALTER"
    ADD = "ADD"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    UPDATE = "UPDATE"
    SET = "SET"
    DELETE = "DELETE"
    SHOW = "SHOW"
    TABLES = "TABLES"
    DESCRIBE = "DESCRIBE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    LIKE = "LIKE"
    NULL = "NULL"

    # Tipos de dados
    VARCHAR = "VARCHAR"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    TEXT = "TEXT"

    # Propriedades de coluna
    NOT_NULL = "NOT NULL"
    PRIMARY_KEY = "PRIMARY KEY"
    AUTO_INCREMENT = "AUTO_INCREMENT"
    UNIQUE = "UNIQUE"
    DEFAULT = "DEFAULT"

    # Símbolos
    LPAREN = "("
    RPAREN = ")"
    COMMA = ","
    SEMICOLON = ";"
    DOT = "."
    STAR = "*"
    EQ = "="
    NEQ = "<>"
    LT = "<"
    GT = ">"
    LE = "<="
    GE = ">="
    PLUS = "+"
    MINUS = "-"
    DIV = "/"
    MOD = "%"

    # Literais
    IDENTIFIER = "IDENTIFIER"      # nomes de tabelas/colunas/bancos
    STRING = "STRING"              # 'texto'
    NUMBER = "NUMBER"              # 123, 45.67
    BOOLEAN_LITERAL = "BOOLEAN"    # true/false (case insensitive)

    # Fim do arquivo/comando
    EOF = "EOF"
    INVALID = "INVALID"


@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    column: int


class Lexer:
    """Converte string SQL em lista de tokens."""

    # Mapeamento de palavras-chave (case insensitive)
    KEYWORDS = {
        "CREATE": TokenType.CREATE,
        "DATABASE": TokenType.DATABASE,
        "DROP": TokenType.DROP,
        "USE": TokenType.USE,
        "TABLE": TokenType.TABLE,
        "ALTER": TokenType.ALTER,
        "ADD": TokenType.ADD,
        "INSERT": TokenType.INSERT,
        "INTO": TokenType.INTO,
        "VALUES": TokenType.VALUES,
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "UPDATE": TokenType.UPDATE,
        "SET": TokenType.SET,
        "DELETE": TokenType.DELETE,
        "SHOW": TokenType.SHOW,
        "TABLES": TokenType.TABLES,
        "DESCRIBE": TokenType.DESCRIBE,
        "AND": TokenType.AND,
        "OR": TokenType.OR,
        "NOT": TokenType.NOT,
        "LIKE": TokenType.LIKE,
        "NULL": TokenType.NULL,
        "VARCHAR": TokenType.VARCHAR,
        "INTEGER": TokenType.INTEGER,
        "DECIMAL": TokenType.DECIMAL,
        "DATE": TokenType.DATE,
        "BOOLEAN": TokenType.BOOLEAN,
        "TEXT": TokenType.TEXT,
        "NOT NULL": TokenType.NOT_NULL,      # tratado como um token especial
        "PRIMARY KEY": TokenType.PRIMARY_KEY,
        "AUTO_INCREMENT": TokenType.AUTO_INCREMENT,
        "UNIQUE": TokenType.UNIQUE,
        "DEFAULT": TokenType.DEFAULT,
    }

    # Regex para tokens (ordem importa!)
    TOKEN_REGEX = [
        (r'--.*', None),                          # comentário (ignorar)
        (r'\s+', None),                            # whitespace (ignorar)
        (r"'[^']*'", TokenType.STRING),            # strings entre aspas simples
        (r'"[^"]*"', TokenType.STRING),            # strings entre aspas duplas (opcional)
        (r'\d+\.\d+', TokenType.NUMBER),           # números decimais
        (r'\d+', TokenType.NUMBER),                 # inteiros
        (r'[a-zA-Z_][a-zA-Z0-9_]*', TokenType.IDENTIFIER),  # identificadores
        (r'<>', TokenType.NEQ),
        (r'<=', TokenType.LE),
        (r'>=', TokenType.GE),
        (r'=', TokenType.EQ),
        (r'<', TokenType.LT),
        (r'>', TokenType.GT),
        (r'\(', TokenType.LPAREN),
        (r'\)', TokenType.RPAREN),
        (r',', TokenType.COMMA),
        (r';', TokenType.SEMICOLON),
        (r'\.', TokenType.DOT),
        (r'\*', TokenType.STAR),
        (r'\+', TokenType.PLUS),
        (r'-', TokenType.MINUS),
        (r'/', TokenType.DIV),
        (r'%', TokenType.MOD),
    ]

    def __init__(self, text: str):
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []

        # Compila as regex
        self.regexes = [(re.compile(pattern), token_type) for pattern, token_type in self.TOKEN_REGEX]

    def tokenize(self) -> List[Token]:
        """Executa a tokenização e retorna a lista de tokens."""
        while self.pos < len(self.text):
            match = None
            for regex, token_type in self.regexes:
                match = regex.match(self.text, self.pos)
                if match:
                    value = match.group(0)
                    if token_type is not None:  # se não for para ignorar
                        # Verifica se é palavra-chave (IDENTIFIER)
                        if token_type == TokenType.IDENTIFIER:
                            upper = value.upper()
                            # Trata palavras-chave compostas como NOT NULL
                            if upper == "NOT" and self._peek_next() == "NULL":
                                # Consumir também o próximo token NULL
                                self.pos = match.end()
                                self.column += len(value)
                                # Avança para o próximo token (NULL)
                                continue  # o loop vai pegar o NULL na próxima iteração
                            # Palavra-chave simples
                            token_type = self.KEYWORDS.get(upper, TokenType.IDENTIFIER)
                        self.tokens.append(Token(token_type, value, self.line, self.column))
                    # Atualiza posição, linha e coluna
                    self._update_position(value)
                    break
            if not match:
                # Caractere inválido
                raise SyntaxError(f"Caractere inesperado '{self.text[self.pos]}' na linha {self.line}, coluna {self.column}")
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens

    def _peek_next(self) -> str:
        """Olha a próxima palavra (para tratar 'NOT NULL' como um token)."""
        # Pula whitespace
        pos = self.pos
        while pos < len(self.text) and self.text[pos].isspace():
            pos += 1
        # Lê o próximo identificador
        start = pos
        while pos < len(self.text) and (self.text[pos].isalnum() or self.text[pos] == '_'):
            pos += 1
        return self.text[start:pos].upper()

    def _update_position(self, text: str):
        """Atualiza linha e coluna baseado no texto consumido."""
        for ch in text:
            if ch == '\n':
                self.line += 1
                self.column = 1
            else:
                self.column += 1
        self.pos += len(text)