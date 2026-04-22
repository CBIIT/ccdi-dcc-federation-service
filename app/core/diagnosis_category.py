import json
from pathlib import Path

_ENUM_PATH = Path(__file__).parent.parent / "config_data" / "diagnosis_enum.json"

with _ENUM_PATH.open() as _f:
    _data = json.load(_f)

HARMONIZED_DIAGNOSIS_CATEGORIES: frozenset[str] = frozenset(_data["diagnosis_category"])
