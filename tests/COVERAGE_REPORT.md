# Test Coverage Report

**Generated**: 2026-01-22  
**Test Suite**: Unit Tests Only  
**Command**: `pytest tests/unit/ --cov=app`

## Overall Coverage

**Total Coverage**: **35.62%** (2,722 of 7,642 lines)

## API Endpoints Coverage

| Endpoint Module | Coverage | Status |
|----------------|----------|--------|
| `app/api/v1/endpoints/root.py` | **100%** | ✅ Complete |
| `app/api/v1/endpoints/info.py` | **100%** | ✅ Complete |
| `app/api/v1/endpoints/metadata.py` | **100%** | ✅ Complete |
| `app/api/v1/endpoints/namespaces.py` | **87.50%** | ✅ Good |
| `app/api/v1/endpoints/organizations.py` | **77.48%** | ✅ Good |
| `app/api/v1/endpoints/experimental.py` | **75.00%** | ✅ Good |
| `app/api/v1/endpoints/deps.py` | **69.23%** | ✅ Good |
| `app/api/v1/endpoints/samples.py` | **64.93%** | 🟡 Moderate |
| `app/api/v1/endpoints/files.py` | **63.77%** | 🟡 Moderate |
| `app/api/v1/endpoints/subjects.py` | **56.67%** | 🟡 Moderate |
| `app/api/v1/endpoints/errors.py` | **36.36%** | 🟡 Needs Work |

## Core Modules Coverage

| Module | Coverage | Status |
|--------|----------|--------|
| `app/lib/url_builder.py` | **100%** | ✅ Complete |
| `app/core/config.py` | **96.88%** | ✅ Excellent |
| `app/lib/field_allowlist.py` | **96.67%** | ✅ Excellent |
| `app/services/materialized_views.py` | **96.25%** | ✅ Excellent |
| `app/models/dto.py` | **95.38%** | ✅ Excellent |
| `app/core/logging.py` | **88.24%** | ✅ Good |
| `app/core/constants.py` | **82.28%** | ✅ Good |
| `app/services/subject.py` | **81.68%** | ✅ Good |
| `app/core/pagination.py` | **73.68%** | ✅ Good |
| `app/services/sample.py` | **48.96%** | 🟡 Moderate |
| `app/services/file.py` | **42.86%** | 🟡 Moderate |
| `app/models/errors.py` | **67.24%** | ✅ Good |
| `app/core/field_mappings.py` | **34.71%** | 🟡 Needs Work |
| `app/core/cache.py` | **23.16%** | 🔴 Low |

## Repository Layer Coverage

| Module | Coverage | Status |
|--------|----------|--------|
| `app/repositories/subject.py` | **18.77%** | 🔴 Low |
| `app/repositories/file.py` | **18.06%** | 🔴 Low |
| `app/repositories/sample.py` | **9.94%** | 🔴 Very Low |

## Infrastructure Coverage

| Module | Coverage | Status |
|--------|----------|--------|
| `app/db/memgraph.py` | **15.72%** | 🔴 Low |
| `app/utils/cypher_builder.py` | **12.83%** | 🔴 Low |
| `app/main.py` | **29.79%** | 🟡 Needs Work |

## Test Statistics

- **Total Tests**: 282+ tests
- **Passing**: 279 tests
- **Skipped**: 1 test (known bug)
- **Failing**: 3 tests (experimental endpoints - complex mocking)

## Priority Areas for Improvement

### 🔴 High Priority (Low Coverage)
1. **Repositories** (9-18% coverage)
   - `app/repositories/sample.py` - 9.94%
   - `app/repositories/file.py` - 18.06%
   - `app/repositories/subject.py` - 18.77%
   - These are critical data access layers

2. **Database Layer** (15% coverage)
   - `app/db/memgraph.py` - 15.72%
   - Connection management and retry logic

3. **Utilities** (12% coverage)
   - `app/utils/cypher_builder.py` - 12.83%
   - Query building logic

### 🟡 Medium Priority
1. **Main Application** (29% coverage)
   - `app/main.py` - 29.79%
   - Application setup and middleware

2. **Field Mappings** (34% coverage)
   - `app/core/field_mappings.py` - 34.71%

3. **Cache Service** (23% coverage)
   - `app/core/cache.py` - 23.16%

## Recommendations

1. **Focus on Repository Tests**: Add integration-style tests for repository methods with real database queries (using testcontainers)
2. **Database Connection Tests**: Add more tests for connection retry logic and error handling
3. **Cypher Builder Tests**: Test query building logic
4. **Main Application Tests**: Test application initialization and middleware setup
5. **Error Handler Tests**: Improve coverage for `app/api/v1/endpoints/errors.py`

## Notes

- Coverage measured via `pytest --cov=app` on unit tests only
- Integration tests may provide additional coverage not shown here
- Some modules (like `main.py`) are harder to test in isolation
- Repository tests may benefit from using testcontainers for more realistic testing

