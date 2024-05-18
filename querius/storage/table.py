import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional

from .kv_store import KeyValueStore

class ForeignKey:
    def __init__(self, column: str, ref_table: str, ref_column: str):
        self.column = column
        self.ref_table = ref_table
        self.ref_column = ref_column

class Table:
    def __init__(self, name: str, schema: Dict[str, type],
                 primary_key: List[str],
                 unique: List[List[str]],
                 foreign_keys: List[ForeignKey],
                 database):
        self.name = name
        self.schema = schema  # e.g., {"id": int, "name": str}
        self.primary_key = primary_key  # List of columns
        self.unique = unique  # List of lists of columns
        self.foreign_keys = foreign_keys  # List of ForeignKey objects
        self.kv_store = KeyValueStore()
        self.indexes: Dict[str, Dict[Any, List[str]]] = defaultdict(dict)  # column -> value -> list of row_ids
        self.unique_indexes: List[Dict[tuple, str]] = [dict() for _ in self.unique]  # For unique constraints
        self.primary_key_index: Dict[tuple, str] = dict()  # For primary key
        self.database = database  # Reference to the database for foreign key checks

        # Automatically create index for primary key
        if self.primary_key:
            for pk in self.primary_key:
                self.create_index(pk)

    def insert(self, data: Dict[str, Any]) -> str:
        # Schema enforcement
        for column, col_type in self.schema.items():
            if column not in data:
                raise ValueError(f"Missing value for column '{column}'")
            if data[column] is not None and not isinstance(data[column], col_type):
                raise TypeError(f"Incorrect type for column '{column}': Expected {col_type.__name__}")

        # Enforce primary key uniqueness
        if self.primary_key:
            pk_tuple = tuple(data[col] for col in self.primary_key)
            if pk_tuple in self.primary_key_index:
                raise ValueError(f"Duplicate primary key {pk_tuple} for table '{self.name}'")

        # Enforce unique constraints
        for idx, unique_cols in enumerate(self.unique):
            unique_tuple = tuple(data[col] for col in unique_cols)
            if unique_tuple in self.unique_indexes[idx]:
                raise ValueError(f"Duplicate unique key {unique_tuple} for table '{self.name}'")

        # Enforce foreign key constraints
        for fk in self.foreign_keys:
            ref_table = self.database.tables.get(fk.ref_table)
            if not ref_table:
                raise ValueError(f"Referenced table '{fk.ref_table}' does not exist for foreign key in '{self.name}'")
            ref_value = data.get(fk.column)
            if ref_value is not None:
                # Assuming foreign key references a single column primary key
                if fk.ref_column in ref_table.indexes:
                    if ref_value not in ref_table.indexes[fk.ref_column]:
                        raise ValueError(f"Foreign key constraint failed: '{fk.ref_table}.{fk.ref_column}' does not contain '{ref_value}'")
                else:
                    # If referenced column is not indexed, perform a full scan
                    exists = any(row.get(fk.ref_column) == ref_value for row in ref_table.kv_store.all().values())
                    if not exists:
                        raise ValueError(f"Foreign key constraint failed: '{fk.ref_table}.{fk.ref_column}' does not contain '{ref_value}'")

        row_id = str(uuid.uuid4())
        self.kv_store.set(row_id, data)

        # Update primary key index
        if self.primary_key:
            self.primary_key_index[pk_tuple] = row_id

        # Update unique indexes
        for idx, unique_cols in enumerate(self.unique):
            unique_tuple = tuple(data[col] for col in unique_cols)
            self.unique_indexes[idx][unique_tuple] = row_id

        # Update indexes
        for column in self.indexes:
            value = data.get(column)
            if value in self.indexes[column]:
                self.indexes[column][value].append(row_id)
            else:
                self.indexes[column][value] = [row_id]

        return row_id

    def create_index(self, column: str) -> None:
        if column not in self.schema:
            raise ValueError(f"Column '{column}' does not exist in table '{self.name}'")
        index = defaultdict(list)
        for row_id, data in self.kv_store.all().items():
            value = data.get(column)
            index[value].append(row_id)
        self.indexes[column] = index

    def update(self, row_id: str, new_data: Dict[str, Any]) -> None:
        row = self.kv_store.get(row_id)
        if not row:
            raise ValueError(f"Row ID '{row_id}' does not exist in table '{self.name}'")

        updated_row = row.copy()
        updated_row.update(new_data)

        # Enforce schema
        for column, col_type in self.schema.items():
            if column not in updated_row:
                raise ValueError(f"Missing value for column '{column}'")
            if updated_row[column] is not None and not isinstance(updated_row[column], col_type):
                raise TypeError(f"Incorrect type for column '{column}': Expected {col_type.__name__}")

        # Enforce primary key uniqueness
        if self.primary_key:
            old_pk = tuple(row[col] for col in self.primary_key)
            new_pk = tuple(updated_row[col] for col in self.primary_key)
            if old_pk != new_pk:
                if new_pk in self.primary_key_index:
                    raise ValueError(f"Duplicate primary key {new_pk} for table '{self.name}'")
                # Update primary key index
                del self.primary_key_index[old_pk]
                self.primary_key_index[new_pk] = row_id

        # Enforce unique constraints
        for idx, unique_cols in enumerate(self.unique):
            old_unique = tuple(row[col] for col in unique_cols)
            new_unique = tuple(updated_row[col] for col in unique_cols)
            if old_unique != new_unique:
                if new_unique in self.unique_indexes[idx]:
                    raise ValueError(f"Duplicate unique key {new_unique} for table '{self.name}'")
                # Update unique index
                del self.unique_indexes[idx][old_unique]
                self.unique_indexes[idx][new_unique] = row_id

        # Enforce foreign key constraints
        for fk in self.foreign_keys:
            old_fk = row.get(fk.column)
            new_fk = updated_row.get(fk.column)
            if old_fk != new_fk:
                ref_table = self.database.tables.get(fk.ref_table)
                if not ref_table:
                    raise ValueError(f"Referenced table '{fk.ref_table}' does not exist for foreign key in '{self.name}'")
                if new_fk is not None:
                    if fk.ref_column in ref_table.indexes:
                        if new_fk not in ref_table.indexes[fk.ref_column]:
                            raise ValueError(f"Foreign key constraint failed: '{fk.ref_table}.{fk.ref_column}' does not contain '{new_fk}'")
                    else:
                        # If referenced column is not indexed, perform a full scan
                        exists = any(row.get(fk.ref_column) == new_fk for row in ref_table.kv_store.all().values())
                        if not exists:
                            raise ValueError(f"Foreign key constraint failed: '{fk.ref_table}.{fk.ref_column}' does not contain '{new_fk}'")

        # Update unique indexes
        for idx, unique_cols in enumerate(self.unique):
            new_unique = tuple(updated_row[col] for col in unique_cols)
            self.unique_indexes[idx][new_unique] = row_id

        # Update indexes
        for column in self.indexes:
            old_value = row.get(column)
            new_value = updated_row.get(column)
            if old_value != new_value:
                # Remove old index
                if old_value in self.indexes[column]:
                    self.indexes[column][old_value].remove(row_id)
                    if not self.indexes[column][old_value]:
                        del self.indexes[column][old_value]
                # Add new index
                if new_value in self.indexes[column]:
                    self.indexes[column][new_value].append(row_id)
                else:
                    self.indexes[column][new_value] = [row_id]

        # Update the row in KV store
        self.kv_store.set(row_id, updated_row)

    def delete(self, row_id: str) -> None:
        row = self.kv_store.get(row_id)
        if not row:
            raise ValueError(f"Row ID '{row_id}' does not exist in table '{self.name}'")

        # Enforce referential integrity (prevent deletion if referenced)
        for table in self.database.tables.values():
            for fk in table.foreign_keys:
                if fk.ref_table == self.name:
                    ref_value = row.get(fk.ref_column)
                    if ref_value is not None:
                        referencing_rows = table.indexes.get(fk.column, {}).get(ref_value, [])
                        if referencing_rows:
                            raise ValueError(f"Cannot delete row; it is referenced by table '{table.name}' via foreign key '{fk.column}'")

        # Remove from primary key index
        if self.primary_key:
            pk_tuple = tuple(row[col] for col in self.primary_key)
            if pk_tuple in self.primary_key_index:
                del self.primary_key_index[pk_tuple]

        # Remove from unique indexes
        for idx, unique_cols in enumerate(self.unique):
            unique_tuple = tuple(row[col] for col in unique_cols)
            if unique_tuple in self.unique_indexes[idx]:
                del self.unique_indexes[idx][unique_tuple]

        # Remove from indexes
        for column in self.indexes:
            value = row.get(column)
            if value in self.indexes[column]:
                if row_id in self.indexes[column][value]:
                    self.indexes[column][value].remove(row_id)
                    if not self.indexes[column][value]:
                        del self.indexes[column][value]

        # Delete from KV store
        self.kv_store.delete(row_id)
