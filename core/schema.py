import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from .errors import InvalidSchemaError, InvalidDataError

class Column:
    """Represents a column definition."""
    def __init__(self, name: str, col_type: str, not_null: bool = False,
                 primary_key: bool = False, auto_increment: bool = False,
                 unique: bool = False, default: Any = None):
        self.name = name
        self.type = col_type.upper()
        self.not_null = not_null
        self.primary_key = primary_key
        self.auto_increment = auto_increment
        self.unique = unique
        self.default = default

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "type": self.type,
            "not_null": self.not_null,
            "primary_key": self.primary_key,
            "auto_increment": self.auto_increment,
            "unique": self.unique,
            "default": self.default
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Column':
        return cls(
            name=data["name"],
            col_type=data["type"],
            not_null=data.get("not_null", False),
            primary_key=data.get("primary_key", False),
            auto_increment=data.get("auto_increment", False),
            unique=data.get("unique", False),
            default=data.get("default")
        )

    def validate_value(self, value: Any) -> Any:
        """Validate and convert value according to column type."""
        if value is None:
            if self.not_null:
                raise InvalidDataError(f"Column '{self.name}' cannot be null")
            return None

        try:
            if self.type == "INTEGER":
                return int(value)
            elif self.type == "DECIMAL":
                return float(value)
            elif self.type == "BOOLEAN":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.upper() == 'S' or value.upper() == 'TRUE'
                return bool(value)
            elif self.type in ("VARCHAR", "TEXT"):
                return str(value)
            elif self.type == "DATE":
                # Aceita string ISO ou timestamp
                if isinstance(value, str):
                    return datetime.fromisoformat(value).date().isoformat()
                elif isinstance(value, datetime):
                    return value.date().isoformat()
                else:
                    raise ValueError
            else:
                return value
        except (ValueError, TypeError) as e:
            raise InvalidDataError(
                f"Invalid value '{value}' for column '{self.name}' of type {self.type}"
            ) from e


class Schema:
    """Represents a table schema."""
    def __init__(self, name: str, columns: List[Column], created_at: Optional[str] = None,
                 last_id: int = 0, record_count: int = 0):
        self.name = name
        self.columns = columns
        self.created_at = created_at or datetime.now().isoformat()
        self.last_id = last_id
        self.record_count = record_count
        self._column_map = {col.name: col for col in columns}
        self._validate()

    def _validate(self):
        """Validate schema consistency."""
        if not self.columns:
            raise InvalidSchemaError("Table must have at least one column")
        # Check for duplicate column names
        names = [col.name for col in self.columns]
        if len(names) != len(set(names)):
            raise InvalidSchemaError("Duplicate column names")
        # Check primary key: only one allowed and must be auto_increment?
        pk_count = sum(1 for col in self.columns if col.primary_key)
        if pk_count > 1:
            raise InvalidSchemaError("Only one primary key allowed")
        # Auto increment must be primary key and integer
        for col in self.columns:
            if col.auto_increment:
                if not col.primary_key:
                    raise InvalidSchemaError("AUTO_INCREMENT column must be PRIMARY KEY")
                if col.type != "INTEGER":
                    raise InvalidSchemaError("AUTO_INCREMENT column must be INTEGER")

    def get_column(self, name: str) -> Optional[Column]:
        return self._column_map.get(name)

    def validate_record(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and convert a record according to schema. Returns cleaned data."""
        cleaned = {}
        for col in self.columns:
            if col.auto_increment:
                continue  # auto-generated, not provided
            value = data.get(col.name)
            if value is None and col.default is not None:
                value = col.default
            cleaned[col.name] = col.validate_value(value)
        # Check for extra columns
        extra = set(data.keys()) - set(self._column_map.keys())
        if extra:
            # Optionally ignore extra columns, but we'll raise error for strictness
            raise InvalidDataError(f"Extra columns not allowed: {extra}")
        return cleaned

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "created_at": self.created_at,
            "columns": [col.to_dict() for col in self.columns],
            "last_id": self.last_id,
            "record_count": self.record_count
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Schema':
        columns = [Column.from_dict(col) for col in data["columns"]]
        return cls(
            name=data["name"],
            columns=columns,
            created_at=data.get("created_at"),
            last_id=data.get("last_id", 0),
            record_count=data.get("record_count", 0)
        )

    def increment_id(self) -> int:
        """Generate next auto-increment id."""
        self.last_id += 1
        return self.last_id