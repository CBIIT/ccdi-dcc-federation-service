# Test Documentation

This document provides comprehensive information about the test suites for the CCDI Federation Service.

## Test Structure

```
tests/
├── unit/                      # Unit tests
│   ├── test_endpoints_*.py   # API endpoint tests
│   ├── test_repositories_*.py # Repository layer tests
│   ├── test_services_*.py   # Service layer tests
│   └── test_*.py             # Utility and helper tests
└── integration/              # Integration tests
```

## Running Tests

### All Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=app --cov-report=html

# Run specific test file
uv run pytest tests/unit/test_services.py -v

# Run specific test
uv run pytest tests/unit/test_services.py::TestSubjectService::test_get_subjects_success -v
```

### Unit Tests Only

```bash
# Run all unit tests
uv run pytest tests/unit/ -v

# Run with coverage
uv run pytest tests/unit/ --cov=app --cov-report=term-missing
```

### Integration Tests

```bash
# Run integration tests
uv run pytest tests/integration/ -v
```

## Test Coverage

See [TEST_COVERAGE_SUMMARY.md](TEST_COVERAGE_SUMMARY.md) for detailed coverage information.

**Current Overall Coverage: ~80%**

### High Coverage Modules (>90%)

- `app/api/v1/deps.py`: 98.88%
- `app/db/memgraph.py`: 94.32%
- `app/models/dto.py`: 93.54%
- `app/core/config.py`: 92.97%
- `app/utils/cypher_builder.py`: 99.47%

### Medium Coverage Modules (70-90%)

- `app/api/v1/endpoints/subjects.py`: 88.99%
- `app/api/v1/endpoints/samples.py`: 95.14%
- `app/api/v1/endpoints/files.py`: 86.49%
- `app/repositories/subject.py`: 68.04%
- `app/repositories/sample.py`: 65.27%

## Test Categories

### Unit Tests

Unit tests are located in `tests/unit/` and test individual components in isolation:

- **Endpoint Tests**: Test API endpoints with mocked dependencies
- **Repository Tests**: Test database query logic with mocked sessions
- **Service Tests**: Test business logic with mocked repositories
- **Utility Tests**: Test helper functions and utilities

### Integration Tests (Not ready)

Integration tests are located in `tests/integration/` and test component interactions:

- Database integration tests
- End-to-end API tests
- Cross-layer integration tests

### Endpoint Tests

The `test_all_endpoints.py` script in the root directory provides comprehensive endpoint testing:

```bash
# Run endpoint tests
python test_all_endpoints.py
```

## Test Patterns

### Mocking Database Sessions

```python
@pytest.fixture
def mock_session(self):
    return AsyncMock(spec=AsyncSession)
```

### Async Test Handling

```python
async def test_async_method(self, mock_session):
    result = await service.method()
    assert result is not None
```

### Error Handling Tests

```python
def test_error_handling(self):
    with pytest.raises(HTTPException) as exc_info:
        endpoint_function()
    assert exc_info.value.status_code == 404
```

## Test Files

### Core Test Files

- `test_deps.py` - FastAPI dependencies and filter parsing
- `test_repositories.py` - Repository layer tests
- `test_services.py` - Service layer tests
- `test_endpoints.py` - API endpoint tests
- `test_db_utils.py` - Database utility tests
- `test_config.py` - Configuration tests
- `test_constants.py` - Constants and enums tests

### Enhanced Test Files

- `test_endpoints_subjects_enhanced.py` - Comprehensive subject endpoint tests
- `test_endpoints_samples_enhanced.py` - Comprehensive sample endpoint tests
- `test_endpoints_files_enhanced.py` - Comprehensive file endpoint tests
- `test_repositories_comprehensive.py` - Comprehensive repository tests
- `test_db_memgraph_enhanced.py` - Enhanced database connection tests
- `test_db_memgraph_coverage.py` - Additional database coverage tests

## Continuous Improvement

- Tests are updated when corresponding code changes
- New features include corresponding tests
- Test failures are investigated immediately
- Coverage reports are reviewed regularly

