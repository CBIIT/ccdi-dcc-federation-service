# CCDI - DCC Tests

This directory contains the test suite for the CCDI-DCC Federation Service.

## Structure

```
tests/
├── __init__.py
├── conftest.py                    # Shared fixtures and configuration
├── pytest.ini                     # Pytest configuration
├── README.md                       # This file
├── test_contract_*.py              # Contract tests (API contract validation)
├── test_error_responses.py         # Error response tests
├── test_subjects_endpoint.py       # Subjects endpoint integration tests
├── unit/                           # Unit tests (comprehensive coverage)
│   ├── __init__.py
│   ├── TEST_COVERAGE_SUMMARY.md    # Detailed test coverage summary
│   ├── test_*.py                   # 40+ unit test files
│   └── ...
└── integration/                    # Integration tests
    └── __init__.py
```

**Total Test Files**: 47  
**Total Tests**: 700+  
**Coverage**: ~78% (see `tests/TEST_RESULTS_REVIEW.md` for details)

## Running Tests

### Prerequisites

Ensure test dependencies are installed:
```bash
# Using uv (recommended)
uv sync --all-groups

# Or using pip
uv pip install -r requirements.txt
```

### Run all tests
```bash
# Using uv (recommended)
uv run pytest

# Or directly
pytest
```

### Run with coverage
```bash
# Using uv (recommended)
uv run pytest --cov=app --cov-report=html

# View coverage report
open htmlcov/index.html  # macOS
```

### Run specific test file
```bash
uv run pytest tests/unit/test_services.py
```

### Run specific test function
```bash
uv run pytest tests/unit/test_services.py::TestSubjectService::test_get_subjects_success
```

### Run tests by marker
```bash
# Run only unit tests
uv run pytest -m unit

# Run only integration tests
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

### Run tests with verbose output
```bash
uv run pytest -v
```

### Run tests and show print statements
```bash
uv run pytest -s
```

### Run contract tests
```bash
# Contract tests verify API contract compliance
uv run pytest tests/test_contract_*.py

# These tests verify:
# - Response structure matches OpenAPI schema
# - Mathematical consistency: sum(values) + missing = total
# - Error handling follows expected patterns
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)
- Fast, isolated tests
- Mock external dependencies
- Test individual functions and classes
- Located in `tests/unit/`
- **47 test files** covering:
  - Core utilities (constants, config, logging, URL builder)
  - API dependencies (deps, filters, pagination)
  - Repositories (subject, sample, file)
  - Services (subject, sample, file, materialized views)
  - Endpoints (subjects, samples, files, experimental)
  - Database utilities
  - Field mappings and validations

### Contract Tests
- Verify API contract compliance
- Test response structure matches OpenAPI schema
- **Critical**: Verify mathematical consistency: `sum(values) + missing = total`
- Located in `tests/test_contract_*.py`
- Files:
  - `test_contract_subjects.py` - Subject endpoint contracts
  - `test_contract_samples.py` - Sample endpoint contracts
  - `test_contract_files.py` - File endpoint contracts

### Integration Tests (`@pytest.mark.integration`)
- Test interactions with external services
- May use test containers (Docker)
- Slower execution time
- Located in `tests/integration/`

## Writing Tests

### Test Naming Convention
- Test files: `test_*.py`
- Test functions: `test_*`
- Test classes: `Test*`

### Using Fixtures
Common fixtures are defined in `conftest.py`:
- `client`: Synchronous test client
- `async_client`: Async test client
- `mock_db_session`: Mocked database session
- `test_settings`: Test configuration
- `sample_subject_data`: Sample test data

Example:
```python
def test_example(client, sample_subject_data):
    response = client.get("/api/v1/subjects")
    assert response.status_code == 200
```

## Coverage Reports

After running tests with coverage, view the HTML report:
```bash
open htmlcov/index.html  # macOS
```

## CI/CD Integration

Tests are automatically run in CI/CD pipelines. Ensure all tests pass before submitting pull requests.

## Dependencies

Install all dependencies (including testing):
```bash
# Using uv (recommended)
uv sync --all-groups

# Or using pip
uv pip install -r requirements.txt
```

**Note**: Test dependencies (like `pytest-cov`) are in the `test` dependency group. Use `uv sync --all-groups` to ensure they're installed.

## Test Coverage

See `tests/TEST_RESULTS_REVIEW.md` for detailed coverage information:
- **Total Tests**: 700+
- **Coverage**: ~78%
- **Test Files**: 47
- **Status**: All tests passing (some skipped for expected reasons)

## Related Documentation

- `tests/unit/TEST_COVERAGE_SUMMARY.md` - Detailed test coverage summary
- `pytest.ini` - Pytest configuration
- `requirements.txt` - Test dependencies
- `pyproject.toml` - Project dependencies including test group
