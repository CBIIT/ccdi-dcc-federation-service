# Integration Tests vs Unit Tests Strategy

**Date**: 2026-01-22  
**Context**: App uses Memgraph DB (shares Neo4j driver)

## Current Situation

### Coverage Status
- **Overall**: 35.62% (2,722 of 7,642 lines)
- **Repositories** (low coverage):
  - `app/repositories/subject.py` - **18.77%**
  - `app/repositories/file.py` - **18.06%**
  - `app/repositories/sample.py` - **9.94%**

### Integration Tests
- Using Neo4j container (compatible with Memgraph)
- Complex setup (Docker, event loops, fixtures)
- Slow execution (container startup, network)
- CI/CD complexity (Docker in GitHub Actions)

## Recommendation: **Focus on Unit Tests**

### Why Unit Tests Are Better Here

1. **Faster Feedback Loop**
   - Unit tests run in seconds vs minutes for integration tests
   - No Docker container startup overhead
   - No network dependencies

2. **Better Coverage ROI**
   - Can test edge cases, error conditions, and boundary conditions easily
   - Mock database responses to test all code paths
   - Current repository coverage (9-18%) can be improved to 70-80%+ with unit tests

3. **CI/CD Simplicity**
   - No Docker required in GitHub Actions
   - Faster CI runs = faster feedback
   - Lower infrastructure costs

4. **Neo4j ≠ Memgraph**
   - Integration tests use Neo4j, but production uses Memgraph
   - Subtle differences in Cypher query behavior
   - False sense of security if tests pass but production fails

5. **Test What Matters**
   - Business logic (filtering, pagination, data transformation)
   - Error handling
   - Query building logic
   - These can all be tested with mocks

### When Integration Tests Add Value

Integration tests are valuable for:
- ✅ **End-to-end API tests** (FastAPI routes → services → repositories)
- ✅ **Database schema validation** (if schema changes frequently)
- ✅ **Performance testing** (query optimization)
- ✅ **Migration testing** (database migrations)

But NOT for:
- ❌ **Repository query logic** (can be mocked)
- ❌ **Filter building** (pure logic, no DB needed)
- ❌ **Data transformation** (pure functions)

## Recommended Strategy

### Phase 1: Improve Unit Test Coverage (Priority)

**Goal**: Increase repository coverage from 9-18% to 70-80%

**Approach**:
1. **Better Mocking Strategy**
   ```python
   # Mock AsyncSession.run() to return realistic data
   # Test query building logic
   # Test filter application
   # Test pagination
   # Test error handling
   ```

2. **Focus Areas**:
   - Query building (`_build_where_clause`, `_build_cypher_query`)
   - Filter validation and application
   - Pagination logic
   - Data transformation (`_record_to_subject`, etc.)
   - Error handling (UnsupportedFieldError, etc.)

3. **Expected Outcome**:
   - Repository coverage: 9-18% → 70-80%
   - Overall coverage: 35.62% → 50-60%
   - Faster test execution
   - Better CI/CD experience

### Phase 2: Selective Integration Tests (Optional)

If needed later, add minimal integration tests for:
- Critical end-to-end flows (1-2 tests per repository)
- Smoke tests for major features
- Run only in specific environments (not every CI run)

### Phase 3: Remove or Skip Integration Tests

**Option A**: Remove integration tests entirely
- Focus on unit tests
- Use contract tests for API validation
- Test against real Memgraph in staging environment

**Option B**: Keep but skip in CI
- Mark with `@pytest.mark.integration`
- Run locally only: `pytest -m "not integration"`
- CI runs: `pytest -m "not integration"`

## Action Plan

### Immediate Actions

1. **Remove/Disable Integration Tests in CI**
   ```yaml
   # .github/workflows/coverage.yml
   - name: Run tests with coverage
     run: |
       uv run pytest -m "not integration" --cov-report=lcov
   ```

2. **Improve Repository Unit Tests**
   - Add more comprehensive mocks for `AsyncSession`
   - Test all filter combinations
   - Test error paths
   - Test edge cases (empty results, large datasets, etc.)

3. **Update Coverage Goals**
   - Target: 50-60% overall coverage
   - Repository coverage: 70-80%
   - Focus on business logic, not database queries

### Long-term

- Consider contract testing for API endpoints
- Use staging environment for real database testing
- Monitor production metrics instead of integration tests

## Conclusion

**Recommendation**: **Focus on unit tests, skip integration tests**

**Reasons**:
1. Better ROI (faster, simpler, more coverage)
2. Neo4j ≠ Memgraph (false confidence)
3. Current low coverage (9-18%) can be improved significantly
4. CI/CD simplicity (no Docker needed)

**Integration tests are nice-to-have but not essential** for this codebase. The time spent fixing integration test fixtures would be better spent writing comprehensive unit tests that actually improve coverage.

