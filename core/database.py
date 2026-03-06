import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
from .table import Table
from .errors import DatabaseNotFoundError, TableNotFoundError, TableAlreadyExistsError

class Database:
    """Represents a database (a directory with metadata and tables)."""
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.metadata_path = self.db_path / "metadata.json"
        self.name = self.db_path.name
        self._tables_cache: Dict[str, Table] = {}

    @classmethod
    def create(cls, name: str, base_dir: Path) -> 'Database':
        """Create a new database directory."""
        db_path = base_dir / name
        if db_path.exists():
            raise FileExistsError(f"Database '{name}' already exists")
        db_path.mkdir(parents=True)
        metadata = {
            "name": name,
            "created_at": datetime.now().isoformat(),
            "tables": []
        }
        with open(db_path / "metadata.json", "w", encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        return cls(db_path)

    @classmethod
    def connect(cls, name: str, base_dir: Path) -> 'Database':
        """Connect to an existing database."""
        db_path = base_dir / name
        if not db_path.exists() or not (db_path / "metadata.json").exists():
            raise DatabaseNotFoundError(f"Database '{name}' not found")
        return cls(db_path)

    def exists(self) -> bool:
        return self.db_path.exists() and self.metadata_path.exists()

    def drop(self):
        """Delete the entire database directory."""
        if self.db_path.exists():
            shutil.rmtree(self.db_path)

    def list_tables(self) -> List[str]:
        """Return list of table names from metadata."""
        with open(self.metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        return metadata.get("tables", [])

    def table(self, name: str) -> Table:
        """Get a Table object for the given table name."""
        if name in self._tables_cache:
            return self._tables_cache[name]
        # Verify table exists
        table_path = self.db_path / name
        if not table_path.exists() or not (table_path / "schema.json").exists():
            raise TableNotFoundError(f"Table '{name}' not found in database '{self.name}'")
        table = Table(self.db_path, name)
        self._tables_cache[name] = table
        return table

    def create_table(self, name: str, columns: List[Dict[str, Any]]) -> Table:
        """
        Create a new table with given columns definition.
        columns: list of dicts with keys: name, type, not_null, primary_key, auto_increment, unique, default.
        """
        if name in self.list_tables():
            raise TableAlreadyExistsError(f"Table '{name}' already exists")

        table_path = self.db_path / name
        table_path.mkdir()
        (table_path / "data").mkdir()

        # Build schema using schema.py (but we need to import Schema here)
        # To avoid circular import, we'll import locally
        from .schema import Schema, Column
        col_objs = []
        for col_def in columns:
            col = Column(
                name=col_def["name"],
                col_type=col_def["type"],
                not_null=col_def.get("not_null", False),
                primary_key=col_def.get("primary_key", False),
                auto_increment=col_def.get("auto_increment", False),
                unique=col_def.get("unique", False),
                default=col_def.get("default")
            )
            col_objs.append(col)

        schema = Schema(name=name, columns=col_objs)

        # Write schema
        with open(table_path / "schema.json", "w", encoding='utf-8') as f:
            json.dump(schema.to_dict(), f, indent=2, ensure_ascii=False)

        # Update metadata
        with open(self.metadata_path, 'r+', encoding='utf-8') as f:
            metadata = json.load(f)
            metadata["tables"].append(name)
            f.seek(0)
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            f.truncate()

        table = Table(self.db_path, name)
        self._tables_cache[name] = table
        return table

    def drop_table(self, name: str):
        """Delete a table and all its data."""
        if name not in self.list_tables():
            raise TableNotFoundError(f"Table '{name}' not found")
        table_path = self.db_path / name
        shutil.rmtree(table_path)
        # Remove from metadata
        with open(self.metadata_path, 'r+', encoding='utf-8') as f:
            metadata = json.load(f)
            metadata["tables"].remove(name)
            f.seek(0)
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            f.truncate()
        if name in self._tables_cache:
            del self._tables_cache[name]