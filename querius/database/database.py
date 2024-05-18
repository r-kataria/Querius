from typing import Dict, List

from storage.table import Table, ForeignKey

class Database:
    def __init__(self):
        self.tables: Dict[str, Table] = {}

    def create_table(self, name: str, schema: Dict[str, type],
                    primary_key: List[str],
                    unique: List[List[str]],
                    foreign_keys: List[ForeignKey]) -> None:
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists.")
        self.tables[name] = Table(name, schema, primary_key, unique, foreign_keys, self)

    def drop_table(self, name: str) -> None:
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist.")
        # Check if other tables have foreign keys referencing this table
        for table in self.tables.values():
            for fk in table.foreign_keys:
                if fk.ref_table == name:
                    raise ValueError(f"Cannot drop table '{name}' because it is referenced by table '{table.name}' via foreign key '{fk.column}'")
        del self.tables[name]
