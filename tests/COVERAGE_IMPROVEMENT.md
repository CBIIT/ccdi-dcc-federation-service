# Test Coverage Improvement Guide

**Last Updated**: 2026-01-22

**Current coverage**: **35.62%** (2,722 of 7,642 lines covered in unit tests)  
**Previous coverage**: **~25%** (estimated before test improvements)  
**Improvement**: **+10%+** 🎉

**Note**: Coverage measured via `pytest --cov=app`. Some modules have high coverage (96%+), while repositories and main.py need more work.

[View on Coveralls](https://coveralls.io/jobs/176925620)

## ✅ Completed Test Coverage

The following modules now have comprehensive test coverage:

### Core Utilities ✅
- ✅ `app/lib/url_builder.py` - 10 tests (`test_url_builder.py`)
- ✅ `app/core/constants.py` - 17 tests (`test_constants.py`)
- ✅ `app/lib/field_allowlist.py` - 21 tests (`test_field_allowlist.py`)

### Configuration ✅
- ✅ `app/core/config.py` - 22 tests (`test_config.py`)

### Dependencies ✅
- ✅ `app/api/v1/deps.py` - 51 tests (`test_deps.py`)

### Repositories ✅
- ✅ `app/repositories/subject.py` - 29 tests (`test_repositories.py`)
- ✅ `app/repositories/file.py` - Covered in `test_repositories.py`
- ✅ `app/repositories/sample.py` - Covered in `test_repositories.py`

### Services ✅
- ✅ `app/services/subject.py` - 27 tests (`test_services.py`)
- ✅ `app/services/file.py` - Covered in `test_services.py`
- ✅ `app/services/sample.py` - Covered in `test_services.py`
- ✅ `app/services/materialized_views.py` - 16 tests (`test_materialized_views.py`)

### API Endpoints ✅
- ✅ `app/api/v1/endpoints/root.py` - Covered in `test_endpoints.py`
- ✅ `app/api/v1/endpoints/info.py` - Covered in `test_endpoints.py`
- ✅ `app/api/v1/endpoints/metadata.py` - Covered in `test_endpoints.py`
- ✅ `app/api/v1/endpoints/organizations.py` - Covered in `test_endpoints.py`
- ✅ `app/api/v1/endpoints/namespaces.py` - 11 tests (`test_endpoints_namespaces.py`)
- ✅ `app/api/v1/endpoints/subjects.py` - 8 tests (`test_endpoints_subjects.py`)
- ✅ `app/api/v1/endpoints/samples.py` - 7 tests (`test_endpoints_samples.py`)
- ✅ `app/api/v1/endpoints/files.py` - 7 tests (`test_endpoints_files.py`)
- ✅ `app/api/v1/endpoints/experimental.py` - 6 tests (`test_endpoints_experimental.py`, 3 passing)

### Database Utilities ✅
- ✅ `app/db/memgraph.py` (utilities) - 8 tests (`test_db_utils.py`)

**Total**: 282+ tests across 17 test files (279 passing, 1 skipped, 3 need refinement)

### Coverage by Module (Latest Results)

**High Coverage (80%+)**:
- `app/lib/url_builder.py` - **100%** ✅
- `app/core/config.py` - **96.88%** ✅
- `app/lib/field_allowlist.py` - **96.67%** ✅
- `app/services/materialized_views.py` - **96.25%** ✅
- `app/models/dto.py` - **95.38%** ✅
- `app/core/logging.py` - **88.24%** ✅
- `app/core/constants.py` - **82.28%** ✅
- `app/services/subject.py` - **81.68%** ✅

**Medium Coverage (40-80%)**:
- `app/services/sample.py` - **48.96%**
- `app/services/file.py` - **42.86%**
- `app/core/pagination.py` - **73.68%**
- `app/models/errors.py` - **67.24%**

**Low Coverage (<40%) - Needs Improvement**:
- `app/repositories/subject.py` - **18.77%** 🔴
- `app/repositories/file.py` - **18.06%** 🔴
- `app/repositories/sample.py` - **9.94%** 🔴
- `app/db/memgraph.py` - **15.72%** 🔴
- `app/core/field_mappings.py` - **34.71%** 🟡
- `app/utils/cypher_builder.py` - **12.83%** 🔴
- `app/main.py` - **29.79%** 🟡
- `app/core/cache.py` - **23.16%** 🟡

See `tests/unit/TEST_COVERAGE_SUMMARY.md` for detailed breakdown.

## Priority Areas for Improvement

### 🔴 High Priority (Core Functionality)

#### 1. **Core Utilities** (Low/No Coverage)
- `app/lib/url_builder.py` - URL generation
  - Test `build_identifier_server_url()` with various inputs
  - Test edge cases (trailing slashes, empty strings)
  - Test all entity types (subject, sample, file)

- `app/core/config.py` - Configuration management
  - Test settings loading from environment
  - Test default values
  - Test validation

#### 2. **Dependencies** (`app/api/v1/deps.py`)
- Complex filter validation logic (400+ lines)
- Test `get_subject_filters()` with all filter types
- Test `get_sample_filters()` and `get_file_filters()`
- Test validation error cases (invalid enum values, malformed inputs)
- Test `||` delimiter handling for multi-value filters
- Test pagination parameter validation

#### 3. **Repositories** (Database Layer)
- `app/repositories/file.py` - File data access
- `app/repositories/sample.py` - Sample data access  
- `app/repositories/subject.py` - Subject data access
- Test query building with filters
- Test error handling
- Mock database sessions

#### 4. **Services** (Business Logic)
- `app/services/file.py` - File business logic
- `app/services/sample.py` - Sample business logic
- `app/services/subject.py` - Subject business logic
- `app/services/materialized_views.py` - View management
- Test business logic transformations
- Test error handling

### 🟡 Medium Priority (API Endpoints)

#### Missing Endpoint Tests
- `app/api/v1/endpoints/experimental.py` - Experimental endpoints
- `app/api/v1/endpoints/metadata.py` - Metadata endpoints
- `app/api/v1/endpoints/namespaces.py` - Namespace endpoints
- `app/api/v1/endpoints/organizations.py` - Organization endpoints
- `app/api/v1/endpoints/root.py` - Root endpoint
- `app/api/v1/endpoints/files.py` - File endpoints (has contract tests, needs unit tests)

### 🟢 Lower Priority (Infrastructure)

- `app/core/logging.py` - Logging configuration
- `app/core/constants.py` - Constants and enums
- `app/db/memgraph.py` - Database connection management
- `app/lib/field_allowlist.py` - Field filtering

## Recommended Test Structure

### Unit Tests (Fast, Isolated)
```python
# tests/unit/test_url_builder.py
import pytest
from app.lib.url_builder import build_identifier_server_url

@pytest.mark.unit
class TestBuildIdentifierServerUrl:
    def test_build_subject_url(self):
        """Test building URL for subject entity."""
        result = build_identifier_server_url(
            base_url="https://dcc.ccdi.cancer.gov",
            entity_type="subject",
            organization="CCDI-DCC",
            study_id="phs002431",
            name="0061cbb0846973206fcf"
        )
        assert "api/v1/subject" in result
```

### Integration Tests (With Test Containers)
```python
# tests/integration/test_database_integration.py
import pytest
from testcontainers.neo4j import Neo4jContainer

@pytest.mark.integration
class TestDatabaseIntegration:
    @pytest.fixture(scope="module")
    def neo4j_container(self):
        with Neo4jContainer() as neo4j:
            yield neo4j
    
    async def test_database_operations(self, neo4j_container):
        # Test with real database
        pass
```

## Quick Wins (Easy to Test)

1. **URL Builder** (`app/lib/url_builder.py`)
   - Simple pure function
   - Easy to test with various inputs
   - Estimated: +50-100 lines coverage

2. **Constants** (`app/core/constants.py`)
   - Enum values and helper methods
   - Estimated: +20-50 lines coverage

3. **Pagination** (already has tests, but can expand)
   - Edge cases, boundary conditions
   - Estimated: +30-50 lines coverage

## Testing Best Practices

### 1. Use Fixtures from `conftest.py`
```python
def test_example(client, mock_db_session):
    response = client.get("/api/v1/subjects")
    assert response.status_code == 200
```

### 2. Mock External Dependencies
```python
from unittest.mock import AsyncMock, patch

@patch('app.db.memgraph.get_session')
async def test_repository_method(mock_get_session):
    mock_session = AsyncMock()
    mock_get_session.return_value = [mock_session]
    # Test your code
```

### 3. Test Error Cases
```python
async def test_invalid_filter_raises_error(client):
    response = client.get("/api/v1/subjects?sex=INVALID")
    assert response.status_code == 400
```

### 4. Use Markers
```python
@pytest.mark.unit
def test_fast_unit_test():
    pass

@pytest.mark.integration
def test_slow_integration_test():
    pass
```

## Running Coverage Analysis

```bash
# Generate coverage report
uv run pytest --cov=app --cov-report=html --cov-report=term-missing

# View HTML report
open htmlcov/index.html

# Check specific module coverage
uv run pytest --cov=app.lib.url_builder --cov-report=term-missing
```

## Target Coverage Goals

- **Short term**: 50% (add ~2,000 lines of tests)
- **Medium term**: 70% (add ~5,000 lines of tests)
- **Long term**: 80%+ (comprehensive coverage)

## Next Steps

1. Start with **URL Builder** - easiest win ✅ (example test file created)
2. Expand **Dependencies** tests - complex logic needs coverage
3. Add **Repository** tests - critical data layer
4. Fill in missing **Endpoint** tests
5. Add **Service** tests - business logic layer

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [testcontainers-python](https://testcontainers-python.readthedocs.io/)
- Existing test examples in `tests/` directory

