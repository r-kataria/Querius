import sys

from database.database import Database
from executor.executor import QueryExecutor
from parser.lexer import Lexer
from parser.parser import Parser

def main():
    db = Database()
    executor = QueryExecutor(db)

    print("Welcome to the Enhanced Python SQL Engine!")
    print("Enter your SQL commands. Type 'exit;' to quit.")

    while True:
        try:
            sql = input("sql> ").strip()
            if not sql:
                continue
            if sql.lower() == 'exit;' or sql.lower() == 'exit':
                print("Goodbye!")
                break

            lexer = Lexer(sql)
            tokens = lexer.tokenize()
            parser = Parser(tokens)
            ast = parser.parse()
            result = executor.execute(ast)

            if isinstance(result, list):
                # Pretty-print the result
                if result:
                    columns = result[0].keys()
                    # Print header
                    header = ' | '.join(columns)
                    print(header)
                    print('-' * len(header))
                    # Print rows
                    for row in result:
                        row_str = ' | '.join(str(row[col]) for col in columns)
                        print(row_str)
                else:
                    print("No results found.")
            else:
                print(result)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
