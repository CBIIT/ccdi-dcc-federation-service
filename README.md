# CCDI Federation Service

**Version: 1.2.0**

A REST API service for querying the CCDI (Childhood Cancer Data Initiative) graph database. This service provides endpoints for retrieving subjects, samples, files, and metadata from a Memgraph graph database.

## Features

- **REST API**: FastAPI-based service with automatic OpenAPI documentation
- **Graph Database**: Memgraph integration with Cypher query support  
- **Pagination**: RFC 5988 compliant pagination with Link headers
- **Field Validation**: Allowlist-based field filtering for security
- **Error Handling**: Comprehensive error handling matching OpenAPI specification
- **Logging**: Structured logging with correlation IDs
- **Docker Support**: Full Docker Compose setup for development
- **Type Safety**: Full Pydantic models and type hints

## Architecture

```
├── app/
│   ├── api/v1/               # API layer
│   │   ├── deps.py          # FastAPI dependencies (auth, pagination, filters)
│   │   └── endpoints/       # Route handlers
│   │       ├── subjects.py  # Subject endpoints + diagnosis search
│   │       ├── samples.py   # Sample endpoints + diagnosis search  
│   │       ├── files.py     # File endpoints
│   │       ├── metadata.py  # Metadata field discovery
│   │       └── namespaces.py # Namespace registry
│   ├── core/                # Core utilities
│   │   ├── config.py        # Comprehensive configuration management
│   │   ├── logging.py       # Structured logging with correlation IDs
│   │   └── pagination.py    # RFC 5988 compliant pagination
│   ├── db/                  # Database layer
│   │   └── memgraph.py      # Memgraph connection with lifecycle management
│   ├── lib/                 # Shared libraries
│   │   └── field_allowlist.py # Field validation and security
│   ├── models/              # Data models
│   │   ├── dto.py           # Pydantic request/response models
│   │   └── errors.py        # Custom exception classes with HTTP mapping
│   ├── repositories/        # Data access layer (Subject, Sample, File)
│   ├── services/            # Business logic layer (with caching integration)
│   └── main.py              # Application entry point with lifespan management
```

### Key Architectural Features
- **Layered Architecture**: Clean separation between API, Service, and Repository layers
- **Dependency Injection**: Extensive use of FastAPI dependencies for shared concerns  
- **Async Support**: Full async/await implementation
- **Error Handling**: Custom exception hierarchy with automatic HTTP status mapping and security-first design
- **Configuration Management**: Nested settings with environment-specific overrides
- **Query Optimization**: Combined Cypher queries for performance (e.g., single-query count operations)
- **Code Reusability**: Extracted validation logic into reusable helper methods

## API Endpoints

#### Subjects
- `GET /api/v1/subject` - List subjects with pagination and filtering
- `GET /api/v1/subject/{organization}/{namespace}/{name}` - Get specific subject by identifier
- `GET /api/v1/subject/by/{field}/count` - Count subjects by field value
- `GET /api/v1/subject/summary` - Get subject summary statistics (total count only, no parameters accepted)

#### Samples
- `GET /api/v1/sample` - List samples with pagination and filtering
- `GET /api/v1/sample/{organization}/{namespace}/{name}` - Get specific sample by identifier
- `GET /api/v1/sample/by/{field}/count` - Count samples by field value
- `GET /api/v1/sample/summary` - Get sample summary statistics (total count only, no parameters accepted)

#### Files
- `GET /api/v1/file` - List files with pagination and filtering
- `GET /api/v1/file/{organization}/{namespace}/{name}` - Get specific file by identifier
- `GET /api/v1/file/by/{field}/count` - Count files by field value
- `GET /api/v1/file/summary` - Get file summary statistics (total count only, no parameters accepted)

#### Metadata
- `GET /api/v1/metadata/fields/subject` - Get filterable subject fields
- `GET /api/v1/metadata/fields/sample` - Get filterable sample fields
- `GET /api/v1/metadata/fields/file` - Get filterable file fields

#### Namespaces
- `GET /api/v1/namespace` - List available namespaces
- `GET /api/v1/namespace/{organization}/{namespace}` - Get specific namespace info

#### Organizations
- `GET /api/v1/organization` - List organizations
- `GET /api/v1/organization/{name}` - Get specific organization

#### Server Info
- `GET /api/v1/info` - Server information and capabilities

#### Error Examples
- `GET /api/v1/errors/examples` - View error response examples for all error types

#### Experimental Endpoints
- `GET /api/v1/sample-diagnosis` - Standalone sample diagnosis search
- `GET /api/v1/subject-diagnosis` - Standalone subject diagnosis search

### Health & System
- `GET /health` - Service health check
- `GET /api/v1` - Service information

## Quick Start

### Using Docker Compose (Recommended)

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd ccdi-dcc-federation-service
   ```

2. **Start all services**:
   ```bash
   docker-compose up -d
   ```

3. **Access the services**:
   - API: http://localhost:8000
   - API Documentation: http://localhost:8000/docs
   - Memgraph Lab: http://localhost:3000

### Manual Setup

#### Option 1: Using Poetry (Recommended)

 **Install dependencies**:
   ```bash
   pip install poetry
   poetry install
   ```
 **Set up environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```


 **Run the application**:
   ```bash
   poetry run uvicorn app.main:app --reload
   ```

#### Option 2: Using Virtual Environment

1. **Create and activate virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Run the application**:
   ```bash
   uvicorn app.main:app --reload
   ```

5. **Deactivate virtual environment when done**:
   ```bash
   deactivate
   ```

## Configuration

The service uses environment variables for configuration. See `.env.example` for all available options.

### Key Configuration Sections

#### Application
```bash
APP_NAME="CCDI Federation Service"
APP_VERSION="v1.2.0" 
DEBUG=false
HOST=0.0.0.0
PORT=8000
```

#### Database (Memgraph)
```bash
MEMGRAPH_URI=bolt://localhost:7687
MEMGRAPH_USER=
MEMGRAPH_PASSWORD=  
MEMGRAPH_DATABASE=memgraph
MEMGRAPH_MAX_CONNECTION_LIFETIME=3600
MEMGRAPH_MAX_CONNECTION_POOL_SIZE=50
```

#### CORS
```bash
CORS_ENABLED=true
CORS_ORIGINS=["*"]
CORS_CREDENTIALS=true
CORS_METHODS=["GET","POST","PUT","DELETE","OPTIONS"]  
CORS_HEADERS=["*"]
```

#### Pagination
```bash
DEFAULT_PAGE_SIZE=50
MAX_PAGE_SIZE=1000
PAGINATION_DEFAULT_PER_PAGE=20
```

#### Rate Limiting
```bash
RATE_LIMIT_ENABLED=true
RATE_LIMIT_REQUESTS_PER_MINUTE=60
```

#### Logging
```bash
LOG_LEVEL=INFO
LOG_FORMAT=json                     # json or text
```

## Development

### Project Structure

The service follows a layered architecture:

1. **API Layer** (`app/api/`): Route handlers and dependencies
2. **Service Layer** (`app/services/`): Business logic and caching
3. **Repository Layer** (`app/repositories/`): Data access with Cypher queries
4. **Database Layer** (`app/db/`): Connection management

### Adding New Endpoints

1. **Create repository** in `app/repositories/` with Cypher queries
2. **Create service** in `app/services/` with business logic and caching
3. **Add routes** in `app/api/v1/endpoints/` with dependency injection
4. **Update models** in `app/models/dto.py` for request/response schemas
5. **Include router** in `app/main.py` setup_routers() function
6. **Add dependencies** in `app/api/v1/deps.py` if needed

**Example of Organization endpoint implementation:**
```python
# 1. app/repositories/organization.py
# 2. app/services/organization.py  
# 3. app/api/v1/endpoints/organizations.py
# 4. Update app/main.py to include organization router
```

### Code Quality

Planned tooling setup:
```bash
# Format code
poetry run black app/

# Lint code  
poetry run ruff check app/

# Type check
poetry run mypy app/

# Run tests
poetry run pytest
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html

# Run specific test file
uv run pytest tests/unit/test_services.py

# Run error response tests
uv run pytest tests/test_error_responses.py

# Run endpoint integration tests
python test_all_endpoints.py
```

**Test Structure:**
```
tests/
├── __init__.py
├── test_error_responses.py    # Error handling validation tests
├── test_contract_subjects.py  # Subject endpoint contract tests
├── test_contract_files.py     # File endpoint contract tests
├── unit/                      # Unit tests
└── integration/                # Integration tests
```

**Test Documentation:**
- [TEST_README.md](TEST_README.md) - Comprehensive test documentation
- [TEST_COVERAGE_SUMMARY.md](TEST_COVERAGE_SUMMARY.md) - Detailed coverage information


**Test Coverage:**
- Error response validation (no 500 errors, sanitized messages)
- Endpoint contract compliance
- Summary count accuracy
- Pagination Link header correctness
- Field validation and allowlist enforcement
- **Current Overall Coverage: ~80%**

## Data Model

The service works with the following entities:

### Subject
```python
{
  "id": "string",
  "identifiers": ["organization.namespace.name"],
  "sex": "string",
  "race": "string", 
  "ethnicity": "string",
  "vital_status": "string",
  "age_at_vital_status": "string",
  "depositions": ["string"],
  "metadata": {...}
}
```

### Sample
```python
{
  "id": "string",
  "identifiers": ["string"],
  "disease_phase": "string",
  "anatomical_sites": ["string"],
  "tissue_type": "string",
  # ... additional fields
}
```

### File
```python
{
  "id": "string", 
  "identifiers": ["string"],
  "type": "string",
  "size": "integer",
  "checksums": {...},
  "description": "string"
}
```

## Filtering

All list endpoints support filtering through query parameters:

```bash
# Filter subjects by sex
GET /api/v1/subject?sex=Male

# Filter with multiple parameters
GET /api/v1/subject?sex=Male&race=White

# Filter with unharmonized metadata
GET /api/v1/subject?metadata.unharmonized.custom_field=value
```

## Pagination

All list endpoints support pagination:

```bash
# Get second page with 50 items per page
GET /api/v1/subject?page=2&per_page=50
```

Response includes RFC 5988 compliant Link header with `first`, `last`, `next`, and `prev` relations:
```
Link: <http://localhost:8000/api/v1/subject?page=1&per_page=50>; rel="first",
      <http://localhost:8000/api/v1/subject?page=10&per_page=50>; rel="last",
      <http://localhost:8000/api/v1/subject?page=1&per_page=50>; rel="prev",
      <http://localhost:8000/api/v1/subject?page=3&per_page=50>; rel="next"
```

The `last` link always points to the final page based on total item count, ensuring accurate pagination navigation.

## Error Handling

The API returns structured error responses matching the OpenAPI specification. All error responses follow strict security guidelines: **no internal error messages or user inputs are exposed** in responses. These details are logged for debugging but never returned to clients.

### Error Response Format

All errors follow this structure:
```json
{
  "errors": [
    {
      "kind": "ErrorKind",
      "message": "User-friendly error message",
      // Additional fields vary by error type
    }
  ]
}
```

### Error Types

#### InvalidRoute (404)
Returned when requesting a non-existent route:
```json
{
  "errors": [
    {
      "kind": "InvalidRoute",
      "method": "GET",
      "route": "Invalid route requested."
    }
  ]
}
```

#### InvalidParameters (400)
Returned when query or path parameters are invalid:
```json
{
  "errors": [
    {
      "kind": "InvalidParameters",
      "parameters": [],
      "message": "Invalid query parameter(s) provided.",
      "reason": "Unknown query parameter(s)"
    }
  ]
}
```
**Note:** The `parameters` array is always empty to avoid exposing user input.

#### NotFound (404)
Returned when a requested resource is not found:
```json
{
  "errors": [
    {
      "kind": "NotFound",
      "entity": "Subjects",
      "message": "Unable to find data for your request.",
      "reason": "No data found"
    }
  ]
}
```

#### MessageOnly (404)
Simplified error format with only a message:
```json
{
  "errors": [
    {
      "message": "Unable to find data for your request."
    }
  ]
}
```

#### UnsupportedField (400)
Returned when a field is not supported for filtering or counting:
```json
{
  "errors": [
    {
      "kind": "UnsupportedField",
      "field": "wrong field",
      "reason": "This field is not present for subjects.",
      "message": "Field is not supported for subjects."
    }
  ]
}
```
**Note:** The `field` value is always `"wrong field"` to avoid exposing user input.

#### UnshareableData (404)
Returned when data cannot be shared due to agreements:
```json
{
  "errors": [
    {
      "kind": "UnshareableData",
      "entity": "Sample",
      "message": "Our agreement with data providers prohibits us from sharing line-level data.",
      "reason": "Data sharing is restricted by agreement with data providers."
    }
  ]
}
```

### Empty Data Responses (200)

For list endpoints, when no data is found or errors occur, the API returns **200 OK with empty data** instead of 404:
```json
{
  "data": [],
  "summary": {
    "counts": {
      "all": 0,
      "current": 0
    }
  }
}
```

This follows the `NoDataFoundResponse-200.json` specification pattern.

### Error Handling Principles

1. **No 500 Errors**: All server errors are converted to 404 NotFound or 200 empty data responses
2. **No Internal Details**: Internal error messages, stack traces, and database errors are never exposed
3. **No User Input Exposure**: User-provided values (field names, parameter values) are sanitized in responses
4. **Consistent Format**: All errors follow the structured `ErrorsResponse` format
5. **Logging**: Full error details (including user inputs) are logged for debugging but not returned

### Custom Exception Classes

- `CCDIException` - Base exception with HTTP mapping
- `InvalidRouteError` - Invalid API route (404)
- `InvalidParametersError` - Parameter validation failures (400)
- `UnsupportedFieldError` - Field allowlist violations (400)
- `NotFoundError` - Resource not found (404)
- `UnshareableDataError` - Data sharing policy violations (404)

## Monitoring

### Health Checks

```bash
# Basic health check
GET /health
# Returns: {"status": "healthy", "service": "ccdi-federation-service"}

# Service information
GET /
# Returns: {
#   "service": "CCDI Federation Service", 
#   "version": "1.0.0",
#   "status": "running", 
#   "docs": "/docs"
# }
```

### Logging

The service provides structured logging with configurable format:

**JSON Format:**
```json
{
  "timestamp": "2025-09-20T10:30:00Z",
  "level": "INFO", 
  "message": "List subjects request",
  "filters": {"sex": "Male"},
  "page": 1,
  "per_page": 20,
  "path": "/api/v1/subject"
}
```

**Log Features:**
- Structured logging with correlation
- Request/response logging
- Error logging with stack traces  
- Configurable log levels (DEBUG, INFO, WARNING, ERROR)
- JSON or text format options



## Deployment

### Production Environment

1. **Build Docker image**:
   ```bash
   docker build -t ccdi-federation-service .
   ```

2. **Run with production settings**:
   ```bash
   docker run -d \
     -p 8000:8000 \
     -e DEBUG=false \
     -e MEMGRAPH_URI=bolt://your-memgraph:7687 \
     ccdi-federation-service
   ```
