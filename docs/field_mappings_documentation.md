# Field Mappings Documentation

**Date**: Feb 2026
**Version**: v1.2.0  

## Overview

The `field_mappings.json` file defines value transformations between database (Memgraph) and API representations. This centralized configuration ensures consistent data transformation across the CCDI Federation Service API.

**Location**: `app/config_data/field_mappings.json`

**Implementation**: `app/core/field_mappings.py`

---

## High-Level Data Flow

The following diagram illustrates how field mappings are applied across the request lifecycle:

DB → (map_field_value) → API response  

API filter → (reverse_map_field_value) → DB Cypher query  

Aggregation query → (build_case_mapping_statement) → Cypher CASE mapping

---

## Mapping Configuration Overview

- `field_mappings.json` is the central configuration for DB↔API value normalization.
- Mappings support forward, reverse, and null-handling behaviors.
- Participant logic is intentionally split: mapped fields (`sex`, `race`) plus derived field (`ethnicity` from `race`).
- One-to-many reverse mappings are supported and require list-aware query predicates (e.g., `IN`).

The JSON file is organized by **node type** (entity type), then by **field name**:

```json
{
  "node_type": {
    "field_name": {
      "description": "Human-readable description",
      "source_property": "database_property_name",
      "mappings": {
        "db_value": "api_value"
      },
      "null_mappings": ["value_to_treat_as_null"],
      "reverse_mappings": {
        "api_value": "db_value" | ["db_value1", "db_value2"]
      }
    }
  }
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `description` | string | Yes | Human-readable description of the mapping |
| `source_property` | string | Yes | Database property name (may differ from API field name) |
| `mappings` | object | No | Forward mappings: Database value → API value |
| `null_mappings` | array | No | Database values that should be treated as `null` in API |
| `reverse_mappings` | object | No | Reverse mappings: API value → Database value(s) |

---

## Current Field Mappings

### 1. Sequencing File Fields

#### `library_selection_method`
- **Source Property**: `library_selection`
- **Mappings**:
  - `"PolyA"` → `"Poly-A Enriched Genomic Library"`
  - `"Not Applicable"` → `"Not applicable"`
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"Poly-A Enriched Genomic Library"` → `"PolyA"`
  - `"Not applicable"` → `"Not Applicable"`

#### `library_source_material`
- **Source Property**: `library_source_material`
- **Mappings**: None (pass-through)
- **Null Mappings**: `["Other"]` (treated as null)
- **Reverse Mappings**: None

#### `specimen_molecular_analyte_type`
- **Source Property**: `library_source_molecule`
- **Mappings**:
  - `"Transcriptomic"` → `"RNA"`
  - `"Genomic"` → `"DNA"`
  - `"Viral RNA"` → `"RNA"`
- **Null Mappings**: `["Not Reported"]`
- **Reverse Mappings**:
  - `"RNA"` → `["Transcriptomic", "Viral RNA"]` (multiple DB values map to one API value)
  - `"DNA"` → `"Genomic"`

#### `library_strategy`
- **Source Property**: `library_strategy`
- **Mappings**:
  - `"Archer Fusion"` → `"Other"`
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"Other"` → `"Archer Fusion"`

### 2. Diagnosis Fields

#### `disease_phase`
- **Source Property**: `disease_phase`
- **Mappings**:
  - `"Recurrent Disease"` → `"Relapse"`
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"Relapse"` → `["Recurrent Disease", "Relapse"]` (handles both DB values)

#### `tumor_classification`
- **Source Property**: `tumor_classification`
- **Mappings**: None
- **Null Mappings**: `["non-malignant"]`
- **Reverse Mappings**: None

### 3. Subject Fields

#### `vital_status`
- **Source Property**: `last_known_survival_status`
- **Mappings**:
  - `"Not Reported"` → `"Not reported"` (case normalization)
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"Not reported"` → `"Not Reported"`

#### `sex`
- **Source Property**: `sex_at_birth`
- **Mappings**:
  - `"Male"` → `"M"`
  - `"Female"` → `"F"`
  - `"Not Reported"` → `"U"`
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"M"` → `"Male"`
  - `"F"` → `"Female"`
  - `"U"` → `"Not Reported"`
- **Note**: Canonical `sex` mapping values are documented in `field_mappings.json`, while current runtime normalization primarily uses `Settings.sex_value_mappings` (forward) and subject repository logic for reverse/filter handling.

#### `race`
- **Source Property**: `race`
- **Mappings**:
  - `"Not Allowed to Collect"` → `"Not allowed to collect"`
- **Null Mappings**: None
- **Reverse Mappings**:
  - `"Not allowed to collect"` → `"Not Allowed to Collect"`
- **Note**:
  - Race may be stored as semicolon-separated values in DB.
  - `"Hispanic or Latino"` is removed from returned race list and used to derive ethnicity.
  - If race contains only `"Hispanic or Latino"`, returned race is `"Not Reported"`.

#### `ethnicity` (Derived, not stored mapping)
- **Source Logic**: Derived from `participant.race` in query/conversion logic
- **Rule**:
  - If race contains `"Hispanic or Latino"` → `"Hispanic or Latino"`
  - Else → `"Not reported"`
- **Note**: `ethnicity` / `ethnicity_value` is not a direct field mapping entry in `field_mappings.json`.

---

## Data Transformation Workflows

### Participant Mapping Clarification

Participant mapping currently uses a hybrid approach:
- `race`: configured in `field_mappings.json` and applied in subject processing.
- `sex`: mapping values are documented in `field_mappings.json`, with runtime normalization handled primarily via `Settings.sex_value_mappings` and subject repository logic.
- `ethnicity`: derived from `race` in repository query/conversion logic, not a direct DB property mapping.


### Workflow 1: Database → API (Response Transformation)

**Purpose**: Transform database values to API-friendly values when returning data to clients.

**Process**:
1. Query executes and returns database records
2. For each field with mappings, apply `map_field_value(field_name, db_value)`
3. Check `null_mappings` first - if value matches, return `None`
4. Check `mappings` - if value matches, return mapped API value
5. Otherwise, return original value

**Example**:
```python
# Database returns: library_selection = "PolyA"
# API response: library_selection_method = "Poly-A Enriched Genomic Library"

from app.core.field_mappings import map_field_value

db_value = "PolyA"
api_value = map_field_value("library_selection_method", db_value)
# Returns: "Poly-A Enriched Genomic Library"
```

**Usage Locations (primary)**:
- `app/repositories/sample_converters.py` - `_record_to_sample()` method
- `app/repositories/subject.py` - Subject model conversion
- Additional repository conversion paths that call mapping helpers

### Workflow 2: API → Database (Filter Transformation)

**Purpose**: Transform API filter values to database values when building queries.

**Process**:
1. API receives filter parameter (e.g., `library_selection_method=Poly-A Enriched Genomic Library`)
2. Apply `reverse_map_field_value(field_name, api_value)`
3. Check `reverse_mappings` - if value matches, return mapped DB value(s)
4. If reverse mapping returns a list, use `IN` clause in Cypher query
5. If reverse mapping returns a single value, use `=` clause
6. If no mapping found, use API value as-is

**Example**:
```python
# API filter: specimen_molecular_analyte_type = "RNA"
# Database query: library_source_molecule IN ["Transcriptomic", "Viral RNA"]

from app.core.field_mappings import reverse_map_field_value

api_value = "RNA"
db_values = reverse_map_field_value("specimen_molecular_analyte_type", api_value)
# Returns: ["Transcriptomic", "Viral RNA"]
# Used in Cypher: WHERE sf.library_source_molecule IN $param
```

**Usage Locations**:
- `app/repositories/sample.py` - `_get_samples_by_sequencing_file_filters()`
- `app/repositories/sample_count.py` - Count query filter building
- `app/repositories/subject.py` - Subject filter building
- All repository filter processing methods

### Workflow 3: Cypher Query-Level Mapping

**Purpose**: Apply mappings directly in Cypher queries for performance (avoids Python-side processing).

**Process**:
1. Use `build_case_mapping_statement(field_name, variable_name)` to generate Cypher CASE statement
2. Embed CASE statement in Cypher query
3. Database performs mapping during query execution

**Example**:
```cypher
// Generated CASE statement:
CASE 
  WHEN molecule_value = 'Transcriptomic' THEN 'RNA'
  WHEN molecule_value = 'Genomic' THEN 'DNA'
  WHEN molecule_value = 'Viral RNA' THEN 'RNA'
  ELSE molecule_value
END AS mapped_value

// Used in query:
MATCH (sa:sample)
OPTIONAL MATCH (sa)<-[:of_sequencing_file]-(sf:sequencing_file)
WITH sa, collect(DISTINCT sf.library_source_molecule) AS molecules
UNWIND molecules AS molecule_value
WITH sa, CASE 
  WHEN molecule_value = 'Transcriptomic' THEN 'RNA'
  WHEN molecule_value = 'Genomic' THEN 'DNA'
  WHEN molecule_value = 'Viral RNA' THEN 'RNA'
  ELSE molecule_value
END AS mapped_value
RETURN mapped_value, count(*) AS count
```

**Usage Locations**:
- `app/repositories/sample_count.py` - Count queries for `specimen_molecular_analyte_type`
- Aggregation queries where mapping is needed in Cypher

### Workflow 4: Invalid Value Filtering

**Purpose**: Filter out invalid/null values in queries based on `null_mappings`.

**Process**:
1. Use `build_invalid_value_filter(node_field, field_name)` to generate WHERE clause
2. Use `is_null_mapped_value(field_name, value)` to check individual values
3. Filter out values that should be treated as null

**Example**:
```python
# For library_source_material field with null_mappings: ["Other"]
# Generated filter:
# sf.library_source_material IS NOT NULL 
# AND sf.library_source_material <> '' 
# AND sf.library_source_material <> '-999'
# AND sf.library_source_material <> 'Other'

from app.core.field_mappings import build_invalid_value_filter

filter_clause = build_invalid_value_filter("sf.library_source_material", "library_source_material")
```

**Usage Locations**:
- Query WHERE clause construction
- Count query filtering
- Summary query filtering

---

## Key Functions Reference

### Core Transformation Functions

#### `map_field_value(field_name: str, db_value: Any) -> Optional[str]`
Maps database value to API value.
- Returns `None` if value is in `null_mappings`
- Returns mapped value if found in `mappings`
- Returns original value if no mapping exists

#### `reverse_map_field_value(field_name: str, api_value: Any) -> Optional[str | List[str]]`
Maps API value to database value(s).
- Returns single value or list of values from `reverse_mappings`
- Returns original value if no reverse mapping exists
- **Note**: Can return a list for one-to-many mappings (e.g., "RNA" → ["Transcriptomic", "Viral RNA"])

### Validation Functions

#### `is_null_mapped_value(field_name: str, value: Any) -> bool`
Checks if a value should be treated as null.

#### `is_database_only_value(field_name: str, value: Any) -> bool`
Checks if a value exists in forward mappings but not in reverse mappings (database-only, not valid for API filters).

### Query Building Functions

#### `build_case_mapping_statement(field_name: str, variable_name: str = "value") -> str`
Builds Cypher CASE statement for query-level mapping.

#### `build_invalid_value_filter(node_field: str, field_name: str) -> str`
Builds WHERE clause conditions to filter invalid values.

#### `build_invalid_value_list_filter(field_name: str) -> str`
Builds list comprehension filter conditions.

#### `get_mapped_db_values(field_name: str) -> List[str]`
Returns list of database values that have mappings (for building IN clauses).

#### `get_null_mappings(field_name: str) -> List[str]`
Returns list of values that should be treated as null.

---

## Implementation Details

### Caching

Field mappings are loaded once and cached in memory:
```python
_field_mappings_cache: Optional[Dict[str, Any]] = None

def _get_field_mappings() -> Dict[str, Any]:
    global _field_mappings_cache
    if _field_mappings_cache is None:
        _field_mappings_cache = _load_field_mappings()
    return _field_mappings_cache
```

### Field Lookup

The system searches across all node types to find field configuration:
```python
def _find_field_config(field_name: str) -> Optional[tuple[str, Dict[str, Any]]]:
    field_mappings = _get_field_mappings()
    for node_type, node_fields in field_mappings.items():
        if field_name in node_fields:
            return (node_type, node_fields[field_name])
    return None
```

### One-to-Many Reverse Mappings

Some API values map to multiple database values. The system handles this by:
1. Returning a list from `reverse_map_field_value()`
2. Using `IN` clause in Cypher queries when list is returned
3. Example: `"RNA"` → `["Transcriptomic", "Viral RNA"]` uses `WHERE field IN $param`

---

## Usage Examples

### Example 1: Sample Response Transformation

```python
# In app/repositories/sample_converters.py
def _record_to_sample(self, sa, p, st, sf, pf, diagnoses, base_url=None):
    # ... other code ...
    
    # Transform library_selection_method
    if sf:
        library_selection_method = map_field_value(
            "library_selection_method", 
            sf.get("library_selection")
        )
        # Returns "Poly-A Enriched Genomic Library" if DB has "PolyA"
    
    # Transform specimen_molecular_analyte_type
    if sf:
        molecule = map_field_value(
            "specimen_molecular_analyte_type",
            sf.get("library_source_molecule")
        )
        # Returns "RNA" if DB has "Transcriptomic" or "Viral RNA"
```

### Example 2: Filter Query Building

```python
# In app/repositories/sample.py
async def _get_samples_by_sequencing_file_filters(self, filters, ...):
    # Process library_selection_method filter
    if "library_selection_method" in filters:
        api_value = filters["library_selection_method"]
        db_value = reverse_map_field_value("library_selection_method", api_value)
        # Returns "PolyA" if API sends "Poly-A Enriched Genomic Library"
        
        where_conditions.append(f"sf.library_selection = ${param_name}")
        params[param_name] = db_value
    
    # Process specimen_molecular_analyte_type filter (one-to-many)
    if "specimen_molecular_analyte_type" in filters:
        api_value = filters["specimen_molecular_analyte_type"]
        db_values = reverse_map_field_value("specimen_molecular_analyte_type", api_value)
        # Returns ["Transcriptomic", "Viral RNA"] if API sends "RNA"
        
        if isinstance(db_values, list):
            where_conditions.append(f"sf.library_source_molecule IN ${param_name}")
            params[param_name] = db_values
        else:
            where_conditions.append(f"sf.library_source_molecule = ${param_name}")
            params[param_name] = db_values
```

### Example 3: Count Query with Cypher Mapping

```python
# In app/repositories/sample_count.py
async def count_samples_by_field(self, field, filters, ...):
    if field == "specimen_molecular_analyte_type":
        # Get mapped DB values for IN clause
        mapped_db_values = get_mapped_db_values(field)
        # Returns: ["Transcriptomic", "Genomic", "Viral RNA"]
        
        # Build CASE statement for mapping
        case_statement = build_case_mapping_statement(field, "molecule_value")
        # Returns: "CASE WHEN molecule_value = 'Transcriptomic' THEN 'RNA' ... END"
        
        cypher = f"""
        MATCH (sa:sample)
        OPTIONAL MATCH (sa)<-[:of_sequencing_file]-(sf:sequencing_file)
        WHERE sf.library_source_molecule IN $mapped_db_values
        WITH sa, collect(DISTINCT sf.library_source_molecule) AS molecules
        UNWIND molecules AS molecule_value
        WITH sa, {case_statement} AS mapped_value
        RETURN mapped_value, count(*) AS count
        """
```

### Example 4: Invalid Value Filtering

```python
# In app/repositories/sample_count.py
async def count_samples_by_field(self, field, filters, ...):
    if field == "library_source_material":
        # Build filter to exclude null_mappings values
        invalid_filter = build_invalid_value_filter(
            "sf.library_source_material",
            "library_source_material"
        )
        # Returns: "sf.library_source_material IS NOT NULL AND ... AND sf.library_source_material <> 'Other'"
        
        cypher = f"""
        MATCH (sa:sample)
        OPTIONAL MATCH (sa)<-[:of_sequencing_file]-(sf:sequencing_file)
        WHERE {invalid_filter}
        RETURN sf.library_source_material, count(*) AS count
        """
```

---

## Best Practices

### 1. Adding New Field Mappings

When adding a new field mapping:

1. **Add to `field_mappings.json`**:
   ```json
   {
     "sequencing_file": {
       "new_field": {
         "description": "Description of mapping",
         "source_property": "db_property_name",
         "mappings": {
           "db_value": "api_value"
         },
         "null_mappings": ["invalid_value"],
         "reverse_mappings": {
           "api_value": "db_value"
         }
       }
     }
   }
   ```

2. **Update converter methods** to use `map_field_value()`:
   ```python
   new_field_value = map_field_value("new_field", db_record.get("db_property_name"))
   ```

3. **Update filter methods** to use `reverse_map_field_value()`:
   ```python
   if "new_field" in filters:
       db_value = reverse_map_field_value("new_field", filters["new_field"])
       where_conditions.append(f"node.property = ${param_name}")
   ```

4. **Add tests** in `tests/unit/test_field_mappings.py`

### 2. One-to-Many Mappings

When one API value maps to multiple database values:
- Use a list in `reverse_mappings`: `"api_value": ["db1", "db2"]`
- Always check if result is a list and use `IN` clause:
  ```python
  db_values = reverse_map_field_value(field_name, api_value)
  if isinstance(db_values, list):
      where_clause = f"field IN ${param_name}"
  else:
      where_clause = f"field = ${param_name}"
  ```

### 3. Null Mappings

Values in `null_mappings` are:
- Converted to `None` in API responses
- Filtered out in queries (unless explicitly needed)
- Not accepted as valid filter values

### 4. Database-Only Values

Values that exist in `mappings` but NOT in `reverse_mappings` are database-only:
- They appear in database but are transformed before API response
- They should NOT be accepted as filter values (use `is_database_only_value()` to validate)

### 5. Performance Considerations

- **Use Cypher CASE statements** for count/aggregation queries (avoids Python-side processing)
- **Cache mappings** - already implemented via `_field_mappings_cache`
- **Filter early** - apply invalid value filters in Cypher WHERE clauses

---

## Testing

Field mappings are tested in:
- `tests/unit/test_field_mappings.py` - Core function tests
- `tests/unit/test_field_mappings_coverage.py` - Coverage tests
- `tests/unit/test_field_mappings_edge_cases.py` - Edge case tests
- `tests/unit/test_field_mappings_enhanced.py` - Enhanced tests
- `tests/test_field_mappings.py` - Integration tests

**Test Coverage**: 99.41% (as of Feb 2026 v.1.2.0.0 coverage report)

---

## Related Files

- **Configuration**: `app/config_data/field_mappings.json`
- **Implementation**: `app/core/field_mappings.py`
- **Usage**: 
  - `app/repositories/sample_converters.py`
  - `app/repositories/sample.py`
  - `app/repositories/sample_count.py`
  - `app/repositories/subject.py`
  - `app/repositories/sample_helpers.py`
- **Tests**: `tests/unit/test_field_mappings*.py`

---

## Migration Notes

### Adding New Mappings

1. Update `field_mappings.json`
2. No code changes needed if using existing functions
3. Test with existing test suite
4. Verify API responses and filter behavior

### Changing Existing Mappings

1. Update `field_mappings.json`
2. Clear cache (restart service) or wait for cache refresh
3. Verify backward compatibility if needed
4. Update tests if behavior changes

### Removing Mappings

1. Remove from `field_mappings.json`
2. Update code that depends on the mapping
3. Remove related tests
4. Update documentation

---

## Troubleshooting

### Issue: Mapping not applied
- **Check**: Field name matches exactly (case-sensitive)
- **Check**: Value matches exactly (including whitespace)
- **Check**: Cache was refreshed (restart service)

### Issue: Filter not working
- **Check**: `reverse_mappings` exists for the API value
- **Check**: One-to-many mappings use `IN` clause, not `=`
- **Check**: Value is not in `null_mappings` or `database_only_values`

### Issue: Null values appearing
- **Check**: Value is in `null_mappings` list
- **Check**: `map_field_value()` is being called correctly
- **Check**: Empty strings are handled (they return `None`)

---
