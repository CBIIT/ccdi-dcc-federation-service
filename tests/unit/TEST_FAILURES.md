# Test Failures Summary - test_deps.py

## Overview
**Total Tests:** 51  
**Passing:** 35  
**Failing:** 16  

## Root Cause
The failing tests are all in `TestGetSubjectFilters` class. The issue is that when calling FastAPI dependency functions directly (not through FastAPI's dependency injection system), the `Query()` default parameters are passed as `FieldInfo` objects instead of actual values.

When FastAPI calls these functions, it extracts the actual values from the request. But when we call them directly in tests, we get the Query objects themselves, which the validation logic treats as invalid values.

## Failing Tests (16 total)

All failures are in `TestGetSubjectFilters`:

1. `test_no_filters` - Query objects are being validated as invalid ethnicity
2. `test_valid_sex_filter` - Query object passed instead of "M"
3. `test_invalid_sex_filter` - Query object passed instead of "INVALID"
4. `test_valid_race_single_value` - Query object passed instead of "White"
5. `test_valid_race_multiple_values` - Query object passed instead of "White||Asian"
6. `test_race_url_encoded_delimiter` - Query object passed instead of "White%7C%7CAsian"
7. `test_invalid_race_filter` - Query object passed instead of "Invalid Race"
8. `test_race_with_mixed_valid_invalid` - Query object passed instead of "White||InvalidRace"
9. `test_valid_vital_status_filter` - Query object passed instead of "Alive"
10. `test_invalid_vital_status_filter` - Query object passed instead of "Invalid Status"
11. `test_valid_age_at_vital_status` - Query object passed instead of "3650"
12. `test_invalid_age_at_vital_status_non_integer` - Query object passed instead of "not_a_number"
13. `test_invalid_age_at_vital_status_out_of_range` - Query object passed instead of "100000"
14. `test_negative_age_at_vital_status` - Query object passed instead of "-100"
15. `test_identifiers_single_value` - Query object passed instead of "id123"
16. `test_identifiers_multiple_values` - Query object passed instead of "id1||id2||id3"

## Solution Options

### Option 1: Explicitly Pass None for Unused Parameters
Modify tests to explicitly pass `None` for all parameters not being tested:

```python
def test_valid_sex_filter(self, mock_request):
    result = get_subject_filters(
        sex="M",
        race=None,
        ethnicity=None,
        identifiers=None,
        vital_status=None,
        age_at_vital_status=None,
        depositions=None,
        request=mock_request
    )
    assert result["sex"] == "M"
```

### Option 2: Use FastAPI TestClient
Test through FastAPI's TestClient which properly handles dependency injection:

```python
def test_valid_sex_filter(client):
    response = client.get("/api/v1/subjects?sex=M")
    # Verify the endpoint uses the filter correctly
```

### Option 3: Mock the Query Objects
Create a helper function that extracts values from Query objects or mocks them properly.

## Recommendation
**Option 1** is the quickest fix - explicitly pass `None` for all unused parameters in the failing tests. This ensures we're testing the actual validation logic rather than dealing with FastAPI's Query objects.

## Passing Tests (35 total)
✅ All pagination tests (5)  
✅ All sample filter tests (8)  
✅ All file filter tests (6)  
✅ All diagnosis search filter tests (4)  
✅ All core dependency tests (2)  
✅ Some subject filter tests (10) - those that explicitly pass None or test error cases

