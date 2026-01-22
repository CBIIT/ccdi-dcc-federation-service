# CCDI Federation Service Tests

This directory contains the test suite for the CCDI Federation Service.

## Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures and configuration
├── test_main.py             # Main application tests
├── test_info_endpoint.py    # Info endpoint tests
├── test_subjects_endpoint.py # Subjects endpoint tests
├── unit/                    # Unit tests
│   ├── __init__.py
│   ├── test_models.py       # Data model tests
│   └── test_pagination.py   # Pagination utility tests
└── integration/             # Integration tests
    └── __init__.py
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=app --cov-report=html
```

### Run specific test file
```bash
pytest tests/test_info_endpoint.py
```

### Run specific test function
```bash
pytest tests/test_info_endpoint.py::test_get_info_success
```

### Run tests by marker
```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

### Run tests with verbose output
```bash
pytest -v
```

### Run tests and show print statements
```bash
pytest -s
```

## Test Categories

### Unit Tests (`@pytest.mark.unit`)
- Fast, isolated tests
- Mock external dependencies
- Test individual functions and classes
- Located in `tests/unit/`

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
pip install -r requirements.txt
```

Or with poetry:
```bash
poetry install --with dev --with test
```
