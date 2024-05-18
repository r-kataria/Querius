from typing import Any, Dict, List, Optional

from database.database import Database
from parser.parser import (
    ASTNode, SelectStatement, InsertStatement, UpdateStatement,
    DeleteStatement, CreateTableStatement, CreateIndexStatement,
    DropTableStatement, WhereClause, OrderBy, JoinClause
)

class QueryExecutor:
    def __init__(self, db: Database):
        self.db = db

    def execute(self, ast: ASTNode) -> Any:
        if isinstance(ast, CreateTableStatement):
            return self._execute_create_table(ast)
        elif isinstance(ast, CreateIndexStatement):
            return self._execute_create_index(ast)
        elif isinstance(ast, InsertStatement):
            return self._execute_insert(ast)
        elif isinstance(ast, SelectStatement):
            return self._execute_select(ast)
        elif isinstance(ast, UpdateStatement):
            return self._execute_update(ast)
        elif isinstance(ast, DeleteStatement):
            return self._execute_delete(ast)
        elif isinstance(ast, DropTableStatement):
            return self._execute_drop_table(ast)
        else:
            raise ValueError("Unsupported AST node")

    def _execute_create_table(self, stmt: CreateTableStatement) -> str:
        self.db.create_table(stmt.table, stmt.schema, stmt.primary_key, stmt.unique, stmt.foreign_keys)
        return f"Table '{stmt.table}' created successfully."

    def _execute_create_index(self, stmt: CreateIndexStatement) -> str:
        table = self.db.tables.get(stmt.table)
        if not table:
            raise ValueError(f"Table '{stmt.table}' does not exist.")
        table.create_index(stmt.column)
        return f"Index on '{stmt.column}' created successfully for table '{stmt.table}'."

    def _execute_drop_table(self, stmt: DropTableStatement) -> str:
        self.db.drop_table(stmt.table)
        return f"Table '{stmt.table}' dropped successfully."

    def _execute_insert(self, stmt: InsertStatement) -> str:
        table = self.db.tables.get(stmt.table)
        if not table:
            raise ValueError(f"Table '{stmt.table}' does not exist.")
        if len(stmt.columns) != len(stmt.values):
            raise ValueError("Number of columns and values do not match.")
        data = {}
        for col, val in zip(stmt.columns, stmt.values):
            if col not in table.schema:
                raise ValueError(f"Column '{col}' does not exist in table '{stmt.table}'.")
            expected_type = table.schema[col]
            data[col] = self._cast_value(val, expected_type)
        row_id = table.insert(data)
        return f"Row inserted with ID {row_id}."

    def _execute_select(self, stmt: SelectStatement) -> List[Dict[str, Any]]:
        table = self.db.tables.get(stmt.table)
        if not table:
            raise ValueError(f"Table '{stmt.table}' does not exist.")
        rows = self._filter_rows(table, stmt.where)

        # Handle Joins
        for join in stmt.joins:
            join_table = self.db.tables.get(join.table)
            if not join_table:
                raise ValueError(f"Joined table '{join.table}' does not exist.")
            join_rows = self._filter_rows(join_table, None)  # No WHERE clause for JOIN table
            joined_data = []
            for row in rows:
                left_value = row.get(join.on_left)
                for join_row in join_rows:
                    if join_row.get(join.on_right) == left_value:
                        # Merge dictionaries; prefix with table names to avoid collisions
                        merged_row = {f"{stmt.table}.{k}": v for k, v in row.items()}
                        merged_row.update({f"{join.table}.{k}": v for k, v in join_row.items()})
                        joined_data.append(merged_row)
            rows = joined_data

        if stmt.columns != ["*"]:
            selected_rows = []
            for row in rows:
                selected_row = {}
                for col in stmt.columns:
                    if '.' in col:
                        # Column specified with table prefix
                        selected_row[col] = row.get(col)
                    else:
                        # Column without table prefix
                        # Attempt to find the column in row
                        matching_keys = [k for k in row if (k == ".{col}" or k == col)]
                        if len(matching_keys) == 1:
                            selected_row[col] = row.get(matching_keys[0])
                        elif len(matching_keys) > 1:
                            raise ValueError(f"Ambiguous column '{col}' in SELECT statement.")
                        else:
                            print(col, table.schema)
                            raise ValueError(f"Column '{col}' does not exist in result set.")
                selected_rows.append(selected_row)
        else:
            selected_rows = rows

        if stmt.order_by:
            selected_rows.sort(key=lambda x: x.get(stmt.order_by.column), reverse=(stmt.order_by.order == "DESC"))
        return selected_rows

    def _execute_update(self, stmt: UpdateStatement) -> str:
        table = self.db.tables.get(stmt.table)
        if not table:
            raise ValueError(f"Table '{stmt.table}' does not exist.")
        rows = self._filter_rows(table, stmt.where)
        count = 0
        for row in rows:
            row_id = self._get_row_id(table, row)
            if not row_id:
                continue
            set_data = {}
            for col, val in stmt.set_clauses.items():
                if col not in table.schema:
                    raise ValueError(f"Column '{col}' does not exist in table '{stmt.table}'.")
                expected_type = table.schema[col]
                set_data[col] = self._cast_value(val, expected_type)
            table.update(row_id, set_data)
            count += 1
        return f"{count} row(s) updated."

    def _execute_delete(self, stmt: DeleteStatement) -> str:
        table = self.db.tables.get(stmt.table)
        if not table:
            raise ValueError(f"Table '{stmt.table}' does not exist.")
        rows = self._filter_rows(table, stmt.where)
        count = 0
        for row in rows:
            row_id = self._get_row_id(table, row)
            if not row_id:
                continue
            table.delete(row_id)
            count += 1
        return f"{count} row(s) deleted."

    def _filter_rows(self, table: Any, where: Optional[WhereClause]) -> List[Dict[str, Any]]:
        if where:
            column = where.column
            operator = where.operator
            value = self._cast_value(where.value, table.schema.get(column, str))
            if column in table.indexes and operator == '=':
                row_ids = table.indexes[column].get(value, [])
                rows = [table.kv_store.get(rid) for rid in row_ids]
                return [row for row in rows if row is not None]
            else:
                # Full table scan for non-indexed columns or non-equality operators
                return [
                    row for row in table.kv_store.all().values()
                    if self._evaluate(row.get(column), operator, value)
                ]
        else:
            return list(table.kv_store.all().values())

    def _evaluate(self, a: Any, operator: str, b: Any) -> bool:
        if operator == '=':
            return a == b
        elif operator in {'!=', '<>'}:
            return a != b
        elif operator == '<':
            return a < b
        elif operator == '<=':
            return a <= b
        elif operator == '>':
            return a > b
        elif operator == '>=':
            return a >= b
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

    def _cast_value(self, value: Any, to_type: type) -> Any:
        if value is None:
            return None
        if to_type == int:
            return int(value)
        elif to_type == float:
            return float(value)
        elif to_type == str:
            return str(value)
        else:
            return value

    def _get_row_id(self, table: Any, target_row: Dict[str, Any]) -> Optional[str]:
        # Attempt to find the row ID based on primary key
        if table.primary_key:
            pk_tuple = tuple(target_row[col] for col in table.primary_key)
            return table.primary_key_index.get(pk_tuple)
        else:
            # Fallback to searching by content (inefficient)
            for row_id, row in table.kv_store.all().items():
                if row == target_row:
                    return row_id
            return None
