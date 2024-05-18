from typing import Any, Dict, List, Optional

from .lexer import Lexer, Token, TokenType

class ASTNode:
    pass

class SelectStatement(ASTNode):
    def __init__(self, columns: List[str], table: str, joins: List['JoinClause'], where: Optional['WhereClause'], order_by: Optional['OrderBy']):
        self.columns = columns
        self.table = table
        self.joins = joins  # List of JoinClause
        self.where = where
        self.order_by = order_by

class InsertStatement(ASTNode):
    def __init__(self, table: str, columns: List[str], values: List[Any]):
        self.table = table
        self.columns = columns
        self.values = values

class UpdateStatement(ASTNode):
    def __init__(self, table: str, set_clauses: Dict[str, Any], where: Optional['WhereClause']):
        self.table = table
        self.set_clauses = set_clauses
        self.where = where

class DeleteStatement(ASTNode):
    def __init__(self, table: str, where: Optional['WhereClause']):
        self.table = table
        self.where = where

class CreateTableStatement(ASTNode):
    def __init__(self, table: str, schema: Dict[str, type], primary_key: List[str], unique: List[List[str]], foreign_keys: List['ForeignKey']):
        self.table = table
        self.schema = schema
        self.primary_key = primary_key  # List of columns
        self.unique = unique  # List of lists of columns
        self.foreign_keys = foreign_keys  # List of ForeignKey objects

class CreateIndexStatement(ASTNode):
    def __init__(self, table: str, column: str):
        self.table = table
        self.column = column

class DropTableStatement(ASTNode):
    def __init__(self, table: str):
        self.table = table

class ForeignKey:
    def __init__(self, column: str, ref_table: str, ref_column: str):
        self.column = column
        self.ref_table = ref_table
        self.ref_column = ref_column

class WhereClause(ASTNode):
    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

class OrderBy(ASTNode):
    def __init__(self, column: str, order: str):
        self.column = column
        self.order = order  # 'ASC' or 'DESC'

class JoinClause(ASTNode):
    def __init__(self, join_type: str, table: str, on_left: str, on_right: str):
        self.join_type = join_type  # 'INNER', 'LEFT', etc.
        self.table = table
        self.on_left = on_left
        self.on_right = on_right

class Parser:
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[self.position]

    def parse(self) -> ASTNode:
        if self.current_token.type == TokenType.KEYWORD:
            if self.current_token.value == "SELECT":
                return self._parse_select()
            elif self.current_token.value == "INSERT":
                return self._parse_insert()
            elif self.current_token.value == "UPDATE":
                return self._parse_update()
            elif self.current_token.value == "DELETE":
                return self._parse_delete()
            elif self.current_token.value == "CREATE":
                return self._parse_create()
            elif self.current_token.value == "DROP":
                return self._parse_drop()
        raise ValueError("Unsupported SQL command")

    def _advance(self) -> None:
        self.position += 1
        if self.position < len(self.tokens):
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = Token(TokenType.EOF, '')

    def _expect(self, type_: TokenType, value: Optional[str] = None) -> Token:
        if self.current_token.type != type_:
            raise ValueError(f"Expected token type {type_}, got {self.current_token.type}")
        if value and self.current_token.value != value:
            raise ValueError(f"Expected token value '{value}', got '{self.current_token.value}'")
        token = self.current_token
        self._advance()
        return token

    def _parse_select(self) -> SelectStatement:
        self._expect(TokenType.KEYWORD, "SELECT")
        columns = self._parse_columns()
        self._expect(TokenType.KEYWORD, "FROM")
        table = self._parse_identifier()
        joins = []
        while self.current_token.type == TokenType.KEYWORD and self.current_token.value in {"INNER", "LEFT", "RIGHT", "OUTER"}:
            joins.append(self._parse_join())
        where = None
        order_by = None
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value == "WHERE":
            where = self._parse_where()
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value == "ORDER":
            order_by = self._parse_order_by()
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return SelectStatement(columns, table, joins, where, order_by)

    def _parse_join(self) -> JoinClause:
        join_type = self.current_token.value
        self._advance()
        self._expect(TokenType.KEYWORD, "JOIN")
        table = self._parse_identifier()
        self._expect(TokenType.KEYWORD, "ON")
        on_left = self._parse_identifier()
        self._expect(TokenType.OPERATOR, "=")
        on_right = self._parse_identifier()
        return JoinClause(join_type, table, on_left, on_right)

    def _parse_insert(self) -> InsertStatement:
        self._expect(TokenType.KEYWORD, "INSERT")
        self._expect(TokenType.KEYWORD, "INTO")
        table = self._parse_identifier()
        self._expect(TokenType.SYMBOL, '(')
        columns = self._parse_identifier_list()
        self._expect(TokenType.SYMBOL, ')')
        self._expect(TokenType.KEYWORD, "VALUES")
        self._expect(TokenType.SYMBOL, '(')
        values = self._parse_value_list()
        self._expect(TokenType.SYMBOL, ')')
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return InsertStatement(table, columns, values)

    def _parse_update(self) -> UpdateStatement:
        self._expect(TokenType.KEYWORD, "UPDATE")
        table = self._parse_identifier()
        self._expect(TokenType.KEYWORD, "SET")
        set_clauses = self._parse_set_clauses()
        where = None
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value == "WHERE":
            where = self._parse_where()
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return UpdateStatement(table, set_clauses, where)

    def _parse_delete(self) -> DeleteStatement:
        self._expect(TokenType.KEYWORD, "DELETE")
        self._expect(TokenType.KEYWORD, "FROM")
        table = self._parse_identifier()
        where = None
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value == "WHERE":
            where = self._parse_where()
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return DeleteStatement(table, where)

    def _parse_create(self) -> ASTNode:
        self._expect(TokenType.KEYWORD, "CREATE")
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value == "TABLE":
            return self._parse_create_table()
        elif self.current_token.type == TokenType.KEYWORD and self.current_token.value == "INDEX":
            return self._parse_create_index()
        else:
            raise ValueError("Unsupported CREATE command")

    def _parse_drop(self) -> DropTableStatement:
        self._expect(TokenType.KEYWORD, "DROP")
        self._expect(TokenType.KEYWORD, "TABLE")
        table = self._parse_identifier()
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return DropTableStatement(table)

    def _parse_create_table(self) -> CreateTableStatement:
        self._expect(TokenType.KEYWORD, "TABLE")
        table = self._parse_identifier()
        self._expect(TokenType.SYMBOL, '(')
        schema, primary_key, unique, foreign_keys = self._parse_table_constraints()
        self._expect(TokenType.SYMBOL, ')')
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return CreateTableStatement(table, schema, primary_key, unique, foreign_keys)

    def _parse_table_constraints(self):
        schema = {}
        primary_key = []
        unique = []
        foreign_keys = []
        while True:
            if self.current_token.type == TokenType.KEYWORD and self.current_token.value in {"PRIMARY", "UNIQUE", "FOREIGN"}:
                if self.current_token.value == "PRIMARY":
                    self._advance()
                    self._expect(TokenType.KEYWORD, "KEY")
                    self._expect(TokenType.SYMBOL, '(')
                    pk_columns = self._parse_identifier_list()
                    self._expect(TokenType.SYMBOL, ')')
                    primary_key.extend(pk_columns)
                elif self.current_token.value == "UNIQUE":
                    self._advance()
                    self._expect(TokenType.SYMBOL, '(')
                    unique_columns = self._parse_identifier_list()
                    self._expect(TokenType.SYMBOL, ')')
                    unique.append(unique_columns)
                elif self.current_token.value == "FOREIGN":
                    self._advance()
                    self._expect(TokenType.KEYWORD, "KEY")
                    self._expect(TokenType.SYMBOL, '(')
                    fk_column = self._parse_identifier()
                    self._expect(TokenType.SYMBOL, ')')
                    self._expect(TokenType.KEYWORD, "REFERENCES")
                    ref_table = self._parse_identifier()
                    self._expect(TokenType.SYMBOL, '(')
                    ref_column = self._parse_identifier()
                    self._expect(TokenType.SYMBOL, ')')
                    foreign_keys.append(ForeignKey(fk_column, ref_table, ref_column))
            else:
                # Parse column definition
                column = self._parse_identifier()
                type_token = self.current_token
                if type_token.type == TokenType.KEYWORD or type_token.type == TokenType.IDENTIFIER:
                    type_str = type_token.value.upper()
                    self._advance()
                    type_map = {"INT": int, "INTEGER": int, "TEXT": str, "FLOAT": float}
                    if type_str in type_map:
                        schema[column] = type_map[type_str]
                    else:
                        schema[column] = str  # Default to string if unknown type
                else:
                    raise ValueError(f"Expected type for column '{column}', got {type_token}")
            if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ',':
                self._advance()
                continue
            else:
                break
        return schema, primary_key, unique, foreign_keys

    def _parse_create_index(self) -> CreateIndexStatement:
        self._expect(TokenType.KEYWORD, "INDEX")
        self._expect(TokenType.KEYWORD, "ON")
        table = self._parse_identifier()
        self._expect(TokenType.SYMBOL, '(')
        column = self._parse_identifier()
        self._expect(TokenType.SYMBOL, ')')
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ';':
            self._advance()
        return CreateIndexStatement(table, column)

    def _parse_columns(self) -> List[str]:
        columns = []
        if self.current_token.type == TokenType.SYMBOL and self.current_token.value == '*':
            columns.append('*')
            self._advance()
            return columns
        while True:
            columns.append(self._parse_identifier())
            if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ',':
                self._advance()
                continue
            else:
                break
        return columns

    def _parse_identifier(self) -> str:
        if self.current_token.type in {TokenType.IDENTIFIER, TokenType.KEYWORD}:
            value = self.current_token.value
            self._advance()
            return value
        else:
            raise ValueError(f"Expected identifier, got {self.current_token}")

    def _parse_identifier_list(self) -> List[str]:
        identifiers = []
        while True:
            identifiers.append(self._parse_identifier())
            if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ',':
                self._advance()
                continue
            else:
                break
        return identifiers

    def _parse_value_list(self) -> List[Any]:
        values = []
        while True:
            values.append(self._parse_value())
            if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ',':
                self._advance()
                continue
            else:
                break
        return values

    def _parse_value(self) -> Any:
        if self.current_token.type == TokenType.STRING:
            value = self.current_token.value
            self._advance()
            return value
        elif self.current_token.type == TokenType.NUMBER:
            num_str = self.current_token.value
            self._advance()
            if '.' in num_str:
                return float(num_str)
            else:
                return int(num_str)
        elif self.current_token.type == TokenType.NULL:
            self._advance()
            return None
        else:
            raise ValueError(f"Unexpected token in value list: {self.current_token}")

    def _parse_set_clauses(self) -> Dict[str, Any]:
        set_clauses = {}
        while True:
            column = self._parse_identifier()
            self._expect(TokenType.OPERATOR, '=')
            value = self._parse_value()
            set_clauses[column] = value
            if self.current_token.type == TokenType.SYMBOL and self.current_token.value == ',':
                self._advance()
                continue
            else:
                break
        return set_clauses

    def _parse_where(self) -> WhereClause:
        self._expect(TokenType.KEYWORD, "WHERE")
        column = self._parse_identifier()
        operator = self._parse_operator()
        value = self._parse_value()
        return WhereClause(column, operator, value)

    def _parse_operator(self) -> str:
        if self.current_token.type == TokenType.OPERATOR:
            op = self.current_token.value
            self._advance()
            return op
        else:
            raise ValueError(f"Expected operator, got {self.current_token}")

    def _parse_order_by(self) -> OrderBy:
        self._expect(TokenType.KEYWORD, "ORDER")
        self._expect(TokenType.KEYWORD, "BY")
        column = self._parse_identifier()
        order = 'ASC'  # Default order
        if self.current_token.type == TokenType.KEYWORD and self.current_token.value in {"ASC", "DESC"}:
            order = self.current_token.value
            self._advance()
        return OrderBy(column, order)
