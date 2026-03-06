import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterator, Callable
from .schema import Schema
from .errors import TableNotFoundError, InvalidDataError

class Table:
    """Represents a table with its data and schema."""
    def __init__(self, db_path: Path, table_name: str):
        self.db_path = db_path
        self.name = table_name
        self.table_path = db_path / table_name
        self.data_path = self.table_path / "data"
        self.schema_path = self.table_path / "schema.json"
        self._schema: Optional[Schema] = None

    def _load_schema(self) -> Schema:
        """Load schema from disk (cached)."""
        if self._schema is None:
            if not self.schema_path.exists():
                raise TableNotFoundError(f"Table '{self.name}' schema not found")
            with open(self.schema_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self._schema = Schema.from_dict(data)
        return self._schema

    def _save_schema(self):
        """Save schema to disk atomically."""
        if self._schema is None:
            return
        temp_path = self.schema_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(self._schema.to_dict(), f, indent=2, ensure_ascii=False)
        shutil.move(str(temp_path), str(self.schema_path))

    def get_schema(self) -> Schema:
        """Return the schema (public method)."""
        return self._load_schema()

    def insert(self, record: Dict[str, Any]) -> int:
        """
        Insert a single record. Returns the new record ID.
        """
        schema = self._load_schema()
        # Validate and clean data
        cleaned = schema.validate_record(record)

        # Generate ID if auto-increment
        new_id = None
        for col in schema.columns:
            if col.auto_increment:
                new_id = schema.increment_id()
                break
        if new_id is None:
            # If no auto-increment, maybe ID is provided? For simplicity, we assume ID is auto or not needed.
            # We'll require an 'id' column? Not necessarily. We'll use a sequential id anyway.
            # But for simplicity, we'll generate a simple sequential id from last_id.
            new_id = schema.last_id + 1
            schema.last_id = new_id

        record_data = {
            "id": new_id,
            "data": cleaned,
            "created_at": datetime.now().isoformat()
        }

        # Write record file
        record_path = self.data_path / f"{new_id}.json"
        # Atomic write
        temp_path = record_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(record_data, f, indent=2, ensure_ascii=False)
        shutil.move(str(temp_path), str(record_path))

        # Update schema counters and save
        schema.record_count += 1
        self._save_schema()

        return new_id

    def insert_many(self, records: List[Dict[str, Any]]) -> List[int]:
        """
        Insert multiple records efficiently. Returns list of new IDs.
        """
        ids = []
        for record in records:
            ids.append(self.insert(record))
        return ids

    def select(self, where: Optional[Callable[[Dict], bool]] = None,
               columns: Optional[List[str]] = None) -> List[Dict]:
        """
        Select records. If where is None, returns all.
        where is a predicate function that takes record data (dict) and returns bool.
        columns filters which fields to return.
        Returns list of dicts with keys: id, created_at, and data fields.
        """
        schema = self._load_schema()
        records = []
        for rec_file in sorted(self.data_path.glob("*.json")):
            with open(rec_file, 'r', encoding='utf-8') as f:
                record = json.load(f)
            # Apply where filter
            if where is not None and not where(record['data']):
                continue
            # Build result
            result = {"id": record["id"], "created_at": record["created_at"]}
            if columns is None:
                result.update(record["data"])
            else:
                for col in columns:
                    if col in record["data"]:
                        result[col] = record["data"][col]
            records.append(result)
        return records

    def select_one(self, record_id: int) -> Optional[Dict]:
        """Select a single record by ID."""
        record_path = self.data_path / f"{record_id}.json"
        if not record_path.exists():
            return None
        with open(record_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def update(self, where: Callable[[Dict], bool], updates: Dict[str, Any]) -> int:
        """
        Update records matching where condition. Returns number of updated records.
        updates is a dict of column->new value.
        """
        schema = self._load_schema()
        count = 0
        for rec_file in self.data_path.glob("*.json"):
            # Read record
            with open(rec_file, 'r', encoding='utf-8') as f:
                record = json.load(f)
            if not where(record['data']):
                continue
            # Apply updates (validate each)
            new_data = record['data'].copy()
            for col, val in updates.items():
                col_def = schema.get_column(col)
                if col_def is None:
                    raise InvalidDataError(f"Column '{col}' does not exist")
                new_data[col] = col_def.validate_value(val)
            # Validate entire record (optional, but ensures constraints)
            # We can skip full validation if we trust individual column validation
            # But for not_null, we might need to check if any required field missing after updates.
            # Simpler: re-validate using schema (but that would require all fields present)
            # We'll assume updates don't remove required fields; if they do, it's an error.
            # For safety, we can validate with schema but must provide all fields.
            # We'll just trust column validation for now.
            record['data'] = new_data
            # Write back atomically
            temp_path = rec_file.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            shutil.move(str(temp_path), str(rec_file))
            count += 1
        return count

    def delete(self, where: Callable[[Dict], bool]) -> int:
        """Delete records matching condition. Returns number deleted."""
        count = 0
        for rec_file in self.data_path.glob("*.json"):
            with open(rec_file, 'r', encoding='utf-8') as f:
                record = json.load(f)
            if where(record['data']):
                rec_file.unlink()
                count += 1
        if count > 0:
            # Update schema count
            schema = self._load_schema()
            schema.record_count -= count
            self._save_schema()
        return count

    def delete_by_id(self, record_id: int) -> bool:
        """Delete a record by ID. Returns True if existed."""
        record_path = self.data_path / f"{record_id}.json"
        if record_path.exists():
            record_path.unlink()
            schema = self._load_schema()
            schema.record_count -= 1
            self._save_schema()
            return True
        return False

    def count(self) -> int:
        """Return number of records."""
        schema = self._load_schema()
        return schema.record_count

    # Future: index support, bulk operations, etc.