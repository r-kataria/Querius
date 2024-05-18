from enum import Enum, auto
from typing import List

class TokenType(Enum):
    KEYWORD = auto()
    IDENTIFIER = auto()
    SYMBOL = auto()
    STRING = auto()
    NUMBER = auto()
    OPERATOR = auto()
    NULL = auto()
    EOF = auto()

class Token:
    def __init__(self, type_: TokenType, value: str):
        self.type = type_
        self.value = value

    def __repr__(self):
        return f"Token({self.type}, {self.value})"

class Lexer:
    KEYWORDS = {
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INDEX",
        "ON", "ORDER", "BY", "ASC", "DESC",
        "PRIMARY", "KEY", "UNIQUE", "FOREIGN", "REFERENCES",
        "INNER", "JOIN", "LEFT", "RIGHT", "OUTER",
        "DROP", "NULL"  # Added 'DROP' and 'NULL'
    }
    SYMBOLS = {'(', ')', ',', ';', '*', '.'}
    OPERATORS = {'=', '<', '>', '<=', '>=', '!=', '<>'}

    def __init__(self, input_text: str):
        self.text = input_text
        self.position = 0
        self.length = len(input_text)

    def tokenize(self) -> List[Token]:
        tokens = []
        while self.position < self.length:
            current_char = self.text[self.position]

            if current_char.isspace():
                self.position += 1
                continue
            elif current_char == "'" or current_char == '"':
                tokens.append(self._string())
            elif current_char.isdigit():
                tokens.append(self._number())
            elif current_char.isalpha() or current_char == '_':
                tokens.append(self._identifier_or_keyword())
            elif current_char in self.SYMBOLS:
                tokens.append(Token(TokenType.SYMBOL, current_char))
                self.position += 1
            elif current_char in {'<', '>', '!', '='}:
                tokens.append(self._operator())
            else:
                raise ValueError(f"Unknown character: {current_char}")
        tokens.append(Token(TokenType.EOF, ''))
        return tokens

    def _string(self) -> Token:
        quote_char = self.text[self.position]
        self.position += 1
        start = self.position
        while self.position < self.length and self.text[self.position] != quote_char:
            self.position += 1
        if self.position >= self.length:
            raise ValueError("Unterminated string literal")
        value = self.text[start:self.position]
        self.position += 1  # Skip closing quote
        return Token(TokenType.STRING, value)

    def _number(self) -> Token:
        start = self.position
        while self.position < self.length and (self.text[self.position].isdigit() or self.text[self.position] == '.'):
            self.position += 1
        value = self.text[start:self.position]
        return Token(TokenType.NUMBER, value)

    def _identifier_or_keyword(self) -> Token:
        start = self.position
        while self.position < self.length and (self.text[self.position].isalnum() or self.text[self.position] == '_'):
            self.position += 1
        value_upper = self.text[start:self.position].upper()
        value_original = self.text[start:self.position]
        if value_upper in self.KEYWORDS:
            if value_upper == "NULL":
                return Token(TokenType.NULL, value_upper)
            return Token(TokenType.KEYWORD, value_upper)
        else:
            return Token(TokenType.IDENTIFIER, value_original)

    def _operator(self) -> Token:
        start = self.position
        if self.text[self.position:self.position+2] in self.OPERATORS:
            op = self.text[self.position:self.position+2]
            self.position += 2
        else:
            op = self.text[self.position]
            self.position += 1
        return Token(TokenType.OPERATOR, op)
