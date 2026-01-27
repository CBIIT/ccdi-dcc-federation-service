# Test Coverage Summary

## Overview

This document provides a comprehensive summary of test coverage for the CCDI Federation Service codebase.

**Related Documentation:**
- [TEST_README.md](TEST_README.md) - Test documentation and running instructions

## Overall Coverage

**Current Overall Coverage: 80.25%** (6272 of 7816 lines covered). 

Coverage reports are available via:
- **Local HTML report**: Run `uv run pytest --cov=app --cov-report=html` then open `htmlcov/index.html`
- **GitHub Actions**: Coverage reports are generated in the [coverage workflow](.github/workflows/coverage.yml)
- **Coveralls**: https://coveralls.io/jobs/177046266 (reference) 

Coverage is measured using `pytest-cov` and includes:
- Unit tests for API endpoints
- Unit tests for repositories
- Unit tests for services
- Unit tests for utilities and helpers
- Integration tests for database operations

## Coverage by Module

### High Coverage Modules (>90%)

- **app/api/v1/deps.py**: 98.88% - Filter parsing and dependency injection
- **app/db/memgraph.py**: 94.32% - Database connection and session management
- **app/models/dto.py**: 93.54% - Data transfer objects
- **app/core/config.py**: 92.97% - Configuration management
- **app/core/field_mappings.py**: 99.41% - Field mapping utilities
- **app/core/constants.py**: 98.73% - Constants and enums
- **app/utils/cypher_builder.py**: 99.47% - Cypher query building utilities
- **app/utils/cypher_validator.py**: 98.82% - Cypher query validation

### Medium Coverage Modules (70-90%)

- **app/api/v1/endpoints/subjects.py**: 88.99% - Subject endpoints
- **app/api/v1/endpoints/samples.py**: 95.14% - Sample endpoints
- **app/api/v1/endpoints/files.py**: 86.49% - File endpoints
- **app/api/v1/endpoints/namespaces.py**: 87.50% - Namespace endpoints
- **app/api/v1/endpoints/organizations.py**: 92.79% - Organization endpoints
- **app/core/pagination.py**: 90.79% - Pagination utilities
- **app/core/cache.py**: 87.37% - Caching utilities
- **app/lib/field_allowlist.py**: 96.67% - Field allowlist management

### Lower Coverage Modules (<70%)

- **app/repositories/subject.py**: 68.04% - Subject repository (complex query logic)
- **app/repositories/sample.py**: 65.27% - Sample repository (complex query logic)
- **app/repositories/file.py**: 75.05% - File repository
- **app/services/subject.py**: 98.44% - Subject service
- **app/services/sample.py**: 96.88% - Sample service
- **app/services/file.py**: 95.80% - File service

### Areas for Future Improvement

1. **Repository Layer** (65-75% coverage):
   - Many uncovered lines are alternative query paths for different filter combinations
   - Complex Cypher query building logic with many conditional branches
   - Edge cases in query optimization paths

2. **Error Handling**:
   - Some error paths in endpoints are not fully covered
   - Database connection error scenarios
   - Edge cases in validation logic

## Test Structure

### Unit Tests

Located in `tests/unit/`:
- `test_endpoints_*.py`: Tests for API endpoints
- `test_repositories_*.py`: Tests for repository layer
- `test_services_*.py`: Tests for service layer
- `test_*.py`: Tests for utilities and helpers

### Integration Tests

Located in `tests/integration/`:
- Database integration tests
- End-to-end API tests

### Endpoint Tests

Located in root:
- `test_all_endpoints.py`: Comprehensive endpoint testing script

## Running Coverage Reports

### Generate Coverage Report

```bash
# Run all tests with coverage
uv run pytest --cov=app --cov-report=term-missing

# Generate HTML report
uv run pytest --cov=app --cov-report=html

# Generate XML report (for CI/CD)
uv run pytest --cov=app --cov-report=xml
```

### View Coverage Report

```bash
# Open HTML report in browser
open htmlcov/index.html
```

For more information on running tests, see [TEST_README.md](TEST_README.md).

## Coverage Goals

### Current Goals

- **Overall Coverage**: Maintain >80%
- **Critical Modules**: >90% (API endpoints, services, utilities)
- **Repository Layer**: >70% (complex query logic makes 100% difficult)

### Quality Metrics

- **Line Coverage**: Measures which lines are executed
- **Branch Coverage**: Measures which code branches are taken
- **Function Coverage**: Measures which functions are called

## Notes

- Coverage percentages are approximate and may vary slightly between runs
- Some uncovered lines are intentional (e.g., debug code, unreachable code)
- Complex query logic in repositories has many conditional paths that are hard to test exhaustively
- Focus is on testing critical paths and error handling rather than achieving 100% coverage


**Related Documentation:**
- [TEST_README.md](TEST_README.md) - Test documentation and running instructions

