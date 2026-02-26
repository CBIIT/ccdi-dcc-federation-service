# CCDI-DCC Field Mappings Review Summary

**Date**: Feb 2026
**Version**: v1.2.0  
**File Reviewed**: `app/config_data/field_mappings.json`  

---

## Overview

This document records the February 2026 review of `app/config_data/field_mappings.json` and summarizes how mappings are used across the CCDI-DCC service. The review confirms that the configuration is structurally valid, internally consistent, and aligned with current repository behavior for value normalization (DB → API), reverse filtering (API → DB), and null-value handling.

At a glance:
- Mappings are organized by node type (`sequencing_file`, `diagnosis`, `subject`) and support both forward and reverse transformations where needed.
- Participant-related behavior is intentionally split between direct mappings (`sex`, `race`) and derived logic (`ethnicity` derived from `race`).
- One-to-many reverse mapping patterns (for example, normalized API values mapping to multiple DB values) are handled consistently via list-aware query conditions.
- Current content is suitable as a team reference for maintenance, onboarding, and future mapping extensions.

---

## Review Snapshot

- Configuration structure is valid and complete for current mapped fields.
- Forward/reverse/null mapping rules are coherent across configured node types.
- Subject-related mappings now include `race`, with participant derivation behavior documented for `ethnicity`.
- No critical configuration defects identified in this review cycle.

--- 

## Statistics

- **Total Node Types**: 3
  - `sequencing_file`
  - `diagnosis`
  - `subject`

- **Total Fields**: 9
  - Sequencing File: 4 fields
  - Diagnosis: 2 fields (`disease_phase`, `tumor_classification`)
  - Subject: 3 fields (`vital_status`, `sex`, `race`)

- **Total Forward Mappings**: 12 (database → API)
- **Total Reverse Mappings**: 11 (API → database)
- **Total Null Mappings**: 3 (values treated as null)

---

## Field Breakdown

### Sequencing File Fields (4)

1. **`library_selection_method`**
   - Source: `library_selection`
   - Forward mappings: 2
   - Reverse mappings: 2
   - Null mappings: 0
   - Fully bidirectional

2. **`library_source_material`**
   - Source: `library_source_material`
   - Forward mappings: 0 (pass-through)
   - Reverse mappings: 0
   - Null mappings: 1 (`["Other"]`)
   - Null filtering only

3. **`specimen_molecular_analyte_type`**
   - Source: `library_source_molecule`
   - Forward mappings: 3
   - Reverse mappings: 2 (one-to-many: "RNA" → ["Transcriptomic", "Viral RNA"])
   - Null mappings: 1 (`["Not Reported"]`)
   - Complex one-to-many mapping handled correctly

4. **`library_strategy`**
   - Source: `library_strategy`
   - Forward mappings: 1
   - Reverse mappings: 1
   - Null mappings: 0
   - Fully bidirectional

### Diagnosis Fields (2)

1. **`disease_phase`**
   - Source: `disease_phase`
   - Forward mappings: 1 (`"Recurrent Disease"` → `"Relapse"`)
   - Reverse mappings: 1 (`"Relapse"` → `["Recurrent Disease", "Relapse"]`)
   - Null mappings: 0
   - ⚠️ **Note**: Reverse mapping includes "Relapse" which is not in forward mappings
     - **Intentional**: "Relapse" can exist directly in database
     - **Behavior**: Both "Recurrent Disease" and "Relapse" map to "Relapse" in API
     - **Status**: Acceptable (handles both database values)

2. **`tumor_classification`**
   - Source: `tumor_classification`
   - Forward mappings: 0 (pass-through)
   - Reverse mappings: 0
   - Null mappings: 1 (`["non-malignant"]`)
   - Null filtering only

### Subject Fields (3)

1. **`vital_status`**
   - Source: `last_known_survival_status`
   - Forward mappings: 1 (`"Not Reported"` → `"Not reported"`)
   - Reverse mappings: 1 (`"Not reported"` → `"Not Reported"`)
   - Null mappings: 0
   - Fully bidirectional (case normalization)

2. **`sex`**
   - Source: `sex_at_birth`
   - Forward mappings: 3 (`"Male"` → `"M"`, `"Female"` → `"F"`, `"Not Reported"` → `"U"`)
   - Reverse mappings: 3 (`"M"` → `"Male"`, `"F"` → `"Female"`, `"U"` → `"Not Reported"`)
   - Null mappings: 0
   - Fully bidirectional
   - ⚠️ **Note**: Reference only - Currently implemented via Settings.sex_value_mappings (forward) and hardcoded reverse mapping in subject.py. Code implementation unchanged for performance reasons.

3. **`race`**
   - Source: `race`
   - Forward mappings: 1 (`"Not Allowed to Collect"` → `"Not allowed to collect"`)
   - Reverse mappings: 1 (`"Not allowed to collect"` → `"Not Allowed to Collect"`)
   - Null mappings: 0
   - Bidirectional case-normalization mapping
   - ⚠️ `"Hispanic or Latino"` handling is part of subject logic (used for ethnicity derivation), not a direct race mapping entry.

---

## Participant DB Mapping (Implementation Notes)

- **sex**
  - DB field: `participant.sex_at_birth`
  - API output: normalized via `sex_value_mappings` (`Male/Female/Not Reported` → `M/F/U`)
  - Reverse filtering: `M/F/U` → `Male/Female/Not Reported`

- **race**
  - DB field: `participant.race` (can be semicolon-separated)
  - API output: split into values; remove `"Hispanic or Latino"` from race list
  - Special case: if race is only `"Hispanic or Latino"`, return race as `"Not Reported"`
  - Mapping example: `"Not Allowed to Collect"` ↔ `"Not allowed to collect"`

- **ethnicity / ethnicity_value**
  - Not a direct mapped DB property in `field_mappings.json`
  - Derived from race:
    - contains `"Hispanic or Latino"` → `"Hispanic or Latino"`
    - otherwise → `"Not reported"`

---

## Mapping Patterns

### Pattern 1: Simple Bidirectional Mapping
- **Example**: `library_selection_method`
- **Characteristics**: One-to-one mapping, both directions defined
- **Usage**: Standard value transformation

### Pattern 2: One-to-Many Mapping
- **Example**: `specimen_molecular_analyte_type` ("RNA" → ["Transcriptomic", "Viral RNA"])
- **Characteristics**: Multiple database values map to one API value
- **Usage**: Normalization of similar values
- **Implementation**: Returns list from `reverse_map_field_value()`, uses `IN` clause in queries

### Pattern 3: Null Filtering Only
- **Example**: `library_source_material`, `tumor_classification`
- **Characteristics**: No value transformation, only filters invalid values
- **Usage**: Data cleaning without transformation

### Pattern 4: Case Normalization
- **Example**: `vital_status` ("Not Reported" → "Not reported")
- **Characteristics**: Minor text normalization
- **Usage**: Consistent API presentation

### Pattern 5: Database Value Handling
- **Example**: `disease_phase` (handles both "Recurrent Disease" and "Relapse" in DB)
- **Characteristics**: Reverse mapping includes values not in forward mappings
- **Usage**: Handles legacy or variant database values

---

## Usage Patterns

### DB → API Value Normalization
- Primary function: `map_field_value()`
- Typical usage: repository record-to-model conversion paths (for example in sample and subject conversion logic)
- Purpose: present normalized API values while preserving raw DB storage conventions

### API → DB Filter Translation
- Primary function: `reverse_map_field_value()`
- Typical usage: filter preprocessing in repository query builders
- Purpose: convert API-facing filter values into DB-compatible query values
- Note: supports one-to-many reverse mappings, which are applied using `IN` semantics in Cypher

### Null/Invalid Value Handling
- Primary functions: `is_null_mapped_value()`, `build_invalid_value_filter()`
- Typical usage: WHERE-clause construction for list/count/summary queries
- Purpose: keep response/count semantics consistent when source values should be treated as null-equivalent

### Query-Level Mapping for Aggregations
- Primary function: `build_case_mapping_statement()`
- Typical usage: count/aggregation queries where mapping is performed directly in Cypher
- Purpose: improve consistency and reduce post-processing overhead for grouped results

### Participant-Specific Pattern
- `sex` and `race`: represented in mapping configuration and applied in subject-processing paths
- `ethnicity`: derived from `race` in repository logic (not a direct mapping entry in `field_mappings.json`)
- Purpose: keep business semantics explicit where derivation rules are domain-specific

---

## Related Documentation

- **Full Documentation**: `docs/field_mappings_documentation.md`
- **Implementation**: `app/core/field_mappings.py`
- **Tests**: `tests/unit/test_field_mappings*.py`
