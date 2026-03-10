# sql/parser.py
from typing import List, Optional, Any, Union  # <-- ADICIONE Any AQUI
from .lexer import Token, TokenType, Lexer
from .ast import (
    Command, CreateDatabase, DropDatabase, UseDatabase,
    CreateTable, DropTable, AlterTableAdd, AlterTableDrop,
    Insert, Select, Update, Delete, ShowTables, DescribeTable,
    ColumnDefinition, DataType, Expression, BinaryOp, Literal, ColumnRef
)


class ParseError(Exception):
    pass


class Parser:
    """Parser SQL recursivo descendente."""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0

    def current_token(self) -> Token:
        """Retorna o token atual."""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return self.tokens[-1]  # EOF

    def peek(self) -> TokenType:
        """Tipo do token atual."""
        return self.current_token().type

    def consume(self, expected_type: TokenType = None) -> Token:
        """Consome o token atual e avança. Se expected_type for fornecido, verifica."""
        token = self.current_token()
        if expected_type and token.type != expected_type:
            raise ParseError(f"Esperado {expected_type}, encontrado {token.type} na linha {token.line}")
        self.pos += 1
        return token

    def match(self, *types: TokenType) -> bool:
        """Verifica se o token atual é um dos tipos fornecidos."""
        return self.peek() in types

    def parse(self) -> Command:
        """Ponto de entrada: analisa um comando SQL."""
        if self.match(TokenType.CREATE):
            return self._parse_create()
        elif self.match(TokenType.DROP):
            return self._parse_drop()
        elif self.match(TokenType.USE):
            return self._parse_use()
        elif self.match(TokenType.ALTER):
            return self._parse_alter()
        elif self.match(TokenType.INSERT):
            return self._parse_insert()
        elif self.match(TokenType.SELECT):
            return self._parse_select()
        elif self.match(TokenType.UPDATE):
            return self._parse_update()
        elif self.match(TokenType.DELETE):
            return self._parse_delete()
        elif self.match(TokenType.SHOW):
            return self._parse_show()
        elif self.match(TokenType.DESCRIBE):
            return self._parse_describe()
        else:
            token = self.current_token()
            raise ParseError(f"Comando não reconhecido: {token.value} na linha {token.line}")

    # ========== Comandos ==========

    def _parse_create(self) -> Command:
        self.consume(TokenType.CREATE)
        if self.match(TokenType.DATABASE):
            return self._parse_create_database()
        elif self.match(TokenType.TABLE):
            return self._parse_create_table()
        else:
            raise ParseError("Esperado DATABASE ou TABLE após CREATE")

    def _parse_create_database(self) -> CreateDatabase:
        self.consume(TokenType.DATABASE)
        name_token = self.consume(TokenType.IDENTIFIER)
        self._consume_semicolon()
        return CreateDatabase(database_name=name_token.value)

    def _parse_create_table(self) -> CreateTable:
        self.consume(TokenType.TABLE)
        name_token = self.consume(TokenType.IDENTIFIER)
        self.consume(TokenType.LPAREN)
        columns = []
        while True:
            col = self._parse_column_definition()
            columns.append(col)
            if self.match(TokenType.COMMA):
                self.consume()
                continue
            else:
                break
        self.consume(TokenType.RPAREN)
        self._consume_semicolon()
        return CreateTable(table_name=name_token.value, columns=columns)

    def _parse_column_definition(self) -> ColumnDefinition:
        """Parse: nome tipo [NOT NULL] [PRIMARY KEY] [AUTO_INCREMENT] [UNIQUE] [DEFAULT valor]"""
        name_token = self.consume(TokenType.IDENTIFIER)
        type_token = self.consume()  # VARCHAR, INTEGER, etc.
        try:
            data_type = DataType(type_token.value.upper())
        except ValueError:
            raise ParseError(f"Tipo inválido: {type_token.value}")

        not_null = False
        primary_key = False
        auto_increment = False
        unique = False
        default = None

        while not self.match(TokenType.COMMA, TokenType.RPAREN):
            if self.match(TokenType.NOT_NULL):
                self.consume()
                not_null = True
            elif self.match(TokenType.PRIMARY_KEY):
                self.consume()
                primary_key = True
            elif self.match(TokenType.AUTO_INCREMENT):
                self.consume()
                auto_increment = True
            elif self.match(TokenType.UNIQUE):
                self.consume()
                unique = True
            elif self.match(TokenType.DEFAULT):
                self.consume()
                default = self._parse_literal()
            else:
                # Pode ser uma palavra-chave inesperada
                token = self.current_token()
                raise ParseError(f"Propriedade de coluna inesperada: {token.value}")

        return ColumnDefinition(
            name=name_token.value,
            type=data_type,
            not_null=not_null,
            primary_key=primary_key,
            auto_increment=auto_increment,
            unique=unique,
            default=default
        )

    def _parse_drop(self) -> Command:
        self.consume(TokenType.DROP)
        if self.match(TokenType.DATABASE):
            return self._parse_drop_database()
        elif self.match(TokenType.TABLE):
            return self._parse_drop_table()
        else:
            raise ParseError("Esperado DATABASE ou TABLE após DROP")

    def _parse_drop_database(self) -> DropDatabase:
        self.consume(TokenType.DATABASE)
        name_token = self.consume(TokenType.IDENTIFIER)
        self._consume_semicolon()
        return DropDatabase(database_name=name_token.value)

    def _parse_drop_table(self) -> DropTable:
        self.consume(TokenType.TABLE)
        name_token = self.consume(TokenType.IDENTIFIER)
        self._consume_semicolon()
        return DropTable(table_name=name_token.value)

    def _parse_use(self) -> UseDatabase:
        self.consume(TokenType.USE)
        name_token = self.consume(TokenType.IDENTIFIER)
        self._consume_semicolon()
        return UseDatabase(database_name=name_token.value)

    def _parse_alter(self) -> Command:
        self.consume(TokenType.ALTER)
        self.consume(TokenType.TABLE)
        table_token = self.consume(TokenType.IDENTIFIER)
        if self.match(TokenType.ADD):
            self.consume()
            col = self._parse_column_definition()
            self._consume_semicolon()
            return AlterTableAdd(table_name=table_token.value, column=col)
        elif self.match(TokenType.DROP):
            self.consume()
            col_token = self.consume(TokenType.IDENTIFIER)
            self._consume_semicolon()
            return AlterTableDrop(table_name=table_token.value, column_name=col_token.value)
        else:
            raise ParseError("Esperado ADD ou DROP após ALTER TABLE")

    def _parse_insert(self) -> Insert:
        self.consume(TokenType.INSERT)
        self.consume(TokenType.INTO)
        table_token = self.consume(TokenType.IDENTIFIER)

        columns = None
        if self.match(TokenType.LPAREN):
            self.consume()
            cols = []
            while True:
                col_token = self.consume(TokenType.IDENTIFIER)
                cols.append(col_token.value)
                if self.match(TokenType.COMMA):
                    self.consume()
                    continue
                else:
                    break
            self.consume(TokenType.RPAREN)
            columns = cols

        self.consume(TokenType.VALUES)
        self.consume(TokenType.LPAREN)
        values_list = []
        while True:
            row = self._parse_values_row()
            values_list.append(row)
            if self.match(TokenType.COMMA):
                self.consume()
                self.consume(TokenType.LPAREN)
                continue
            else:
                break
        self.consume(TokenType.RPAREN)  # último fecha
        self._consume_semicolon()
        return Insert(table_name=table_token.value, columns=columns, values=values_list)

    def _parse_values_row(self) -> List[Any]:
        """Lê uma linha de valores dentro de parênteses."""
        values = []
        while not self.match(TokenType.RPAREN):
            if self.match(TokenType.COMMA):
                self.consume()
            lit = self._parse_literal()
            values.append(lit)
        self.consume(TokenType.RPAREN)
        return values

    def _parse_select(self) -> Select:
        self.consume(TokenType.SELECT)
        columns = []
        if self.match(TokenType.STAR):
            self.consume()
            columns = ['*']
        else:
            while True:
                col_token = self.consume(TokenType.IDENTIFIER)
                columns.append(col_token.value)
                if self.match(TokenType.COMMA):
                    self.consume()
                    continue
                else:
                    break
        self.consume(TokenType.FROM)
        table_token = self.consume(TokenType.IDENTIFIER)
        where = None
        if self.match(TokenType.WHERE):
            self.consume()
            where = self._parse_expression()
        self._consume_semicolon()
        return Select(columns=columns, table_name=table_token.value, where=where)

    def _parse_update(self) -> Update:
        self.consume(TokenType.UPDATE)
        table_token = self.consume(TokenType.IDENTIFIER)
        self.consume(TokenType.SET)
        assignments = []
        while True:
            col_token = self.consume(TokenType.IDENTIFIER)
            self.consume(TokenType.EQ)
            val = self._parse_literal()
            assignments.append((col_token.value, val))
            if self.match(TokenType.COMMA):
                self.consume()
                continue
            else:
                break
        where = None
        if self.match(TokenType.WHERE):
            self.consume()
            where = self._parse_expression()
        self._consume_semicolon()
        return Update(table_name=table_token.value, assignments=assignments, where=where)

    def _parse_delete(self) -> Delete:
        self.consume(TokenType.DELETE)
        self.consume(TokenType.FROM)
        table_token = self.consume(TokenType.IDENTIFIER)
        where = None
        if self.match(TokenType.WHERE):
            self.consume()
            where = self._parse_expression()
        self._consume_semicolon()
        return Delete(table_name=table_token.value, where=where)

    def _parse_show(self) -> ShowTables:
        self.consume(TokenType.SHOW)
        if self.match(TokenType.TABLES):
            self.consume()
            self._consume_semicolon()
            return ShowTables()
        else:
            raise ParseError("Esperado TABLES após SHOW")

    def _parse_describe(self) -> DescribeTable:
        self.consume(TokenType.DESCRIBE)
        table_token = self.consume(TokenType.IDENTIFIER)
        self._consume_semicolon()
        return DescribeTable(table_name=table_token.value)

    # ========== Expressões ==========

    def _parse_expression(self) -> Expression:
        """Parse de expressões com precedência simples: AND/OR, depois operadores de comparação."""
        return self._parse_or()

    def _parse_or(self) -> Expression:
        left = self._parse_and()
        while self.match(TokenType.OR):
            op_token = self.consume()
            right = self._parse_and()
            left = BinaryOp(left, op_token.value.upper(), right)
        return left

    def _parse_and(self) -> Expression:
        left = self._parse_comparison()
        while self.match(TokenType.AND):
            op_token = self.consume()
            right = self._parse_comparison()
            left = BinaryOp(left, op_token.value.upper(), right)
        return left

    def _parse_comparison(self) -> Expression:
        left = self._parse_primary()
        if self.match(TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT, TokenType.LE, TokenType.GE, TokenType.LIKE):
            op_token = self.consume()
            right = self._parse_primary()
            return BinaryOp(left, op_token.value.upper(), right)
        return left

    def _parse_primary(self) -> Expression:
        """Literal ou identificador (coluna) ou expressão entre parênteses."""
        if self.match(TokenType.LPAREN):
            self.consume()
            expr = self._parse_expression()
            self.consume(TokenType.RPAREN)
            return expr
        elif self.match(TokenType.STRING, TokenType.NUMBER, TokenType.BOOLEAN_LITERAL, TokenType.NULL):
            return self._parse_literal_expr()
        elif self.match(TokenType.IDENTIFIER):
            token = self.consume()
            return ColumnRef(name=token.value)
        else:
            token = self.current_token()
            raise ParseError(f"Expressão inesperada: {token.value}")

    def _parse_literal_expr(self) -> Literal:
        """Retorna um Literal a partir do token atual."""
        token = self.current_token()
        if token.type == TokenType.STRING:
            self.consume()
            # Remove aspas
            value = token.value[1:-1]
            return Literal(value)
        elif token.type == TokenType.NUMBER:
            self.consume()
            if '.' in token.value:
                return Literal(float(token.value))
            else:
                return Literal(int(token.value))
        elif token.type == TokenType.BOOLEAN_LITERAL:
            self.consume()
            return Literal(token.value.upper() == 'TRUE')
        elif token.type == TokenType.NULL:
            self.consume()
            return Literal(None)
        else:
            raise ParseError(f"Literal esperado, encontrado {token.type}")

    def _parse_literal(self) -> Any:
        """Retorna o valor literal bruto (usado em INSERT e UPDATE)."""
        expr = self._parse_literal_expr()
        return expr.value

    def _consume_semicolon(self):
        """Consome ponto e vírgula opcional no final."""
        if self.match(TokenType.SEMICOLON):
            self.consume()
        # Ignora EOF
        if self.match(TokenType.EOF):
            pass