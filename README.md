# CCDI Federation Service

A REST API service for querying the CCDI (Childhood Cancer Data Initiative) graph database. This service provides endpoints for retrieving subjects, samples, files, and metadata from a Memgraph graph database.

## Features

- **REST API**: FastAPI-based service with automatic OpenAPI documentation
- **Graph Database**: Memgraph integration with Cypher query support  
- **Caching**: Redis-based caching for count and summary endpoints
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
│   │   ├── pagination.py    # RFC 5988 compliant pagination 
│   │   └── cache.py         # Async Redis caching service
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
- **Async Support**: Full async/await implementation with async Redis
- **Error Handling**: Custom exception hierarchy with automatic HTTP status mapping
- **Caching Strategy**: Redis-based caching with configurable TTLs per endpoint type
- **Configuration Management**: Nested settings with environment-specific overrides

## API Endpoints

#### Subjects
- `GET /api/v1/subject` - List subjects with pagination and filtering
- `GET /api/v1/subject/{organization}/{namespace}/{name}` - Get specific subject by identifier
- `GET /api/v1/subject/by/{field}/count` - Count subjects by field value
- `GET /api/v1/subject/summary` - Get subject summary statistics
- `GET /api/v1/subject/diagnosis/search` - Search subjects by diagnosis
- `GET /api/v1/subject/diagnosis/by/{field}/count` - Count subjects by field with diagnosis
- `GET /api/v1/subject/diagnosis/summary` - Subject summary with diagnosis filtering

#### Samples
- `GET /api/v1/sample` - List samples with pagination and filtering
- `GET /api/v1/sample/{organization}/{namespace}/{name}` - Get specific sample by identifier
- `GET /api/v1/sample/by/{field}/count` - Count samples by field value
- `GET /api/v1/sample/summary` - Get sample summary statistics
- `GET /api/v1/sample/diagnosis/*` - Sample diagnosis endpoints (similar to subjects)

#### Files
- `GET /api/v1/file` - List files with pagination and filtering
- `GET /api/v1/file/{organization}/{namespace}/{name}` - Get specific file by identifier
- `GET /api/v1/file/by/{field}/count` - Count files by field value
- `GET /api/v1/file/summary` - Get file summary statistics

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

#### Standalone Diagnosis
- `GET /api/v1/sample-diagnosis` - Standalone sample diagnosis search
- `GET /api/v1/subject-diagnosis` - Standalone subject diagnosis search

### Health & System
- `GET /health` - Service health check
- `GET /` - Service information

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
   - Redis: localhost:6379

### Manual Setup

#### Option 1: Using Poetry (Recommended)

1. **Install dependencies**:
   ```bash
   pip install poetry
   poetry install
   ```

2. **Set up environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start external services**:
   ```bash
   # Start Memgraph
   docker run -p 7687:7687 -p 7444:7444 memgraph/memgraph:2.11.1

   # Start Redis (optional, for caching)
   docker run -p 6379:6379 redis:7.2-alpine
   ```

4. **Run the application**:
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

4. **Start external services**:
   ```bash
   # Start Memgraph
   docker run -p 7687:7687 -p 7444:7444 memgraph/memgraph:2.11.1

   # Start Redis (optional, for caching)
   docker run -p 6379:6379 redis:7.2-alpine
   ```

5. **Run the application**:
   ```bash
   uvicorn app.main:app --reload
   ```

6. **Deactivate virtual environment when done**:
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

#### Cache (Redis)
```bash
CACHE_ENABLED=true
CACHE_REDIS_HOST=localhost
CACHE_REDIS_PORT=6379
CACHE_REDIS_DB=0
CACHE_REDIS_PASSWORD=
CACHE_TTL_COUNT_ENDPOINTS=1800      # 30 minutes
CACHE_TTL_SUMMARY_ENDPOINTS=900     # 15 minutes  
CACHE_TTL_LIST_ENDPOINTS=300        # 5 minutes
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
DEFAULT_PAGE_SIZE=100
MAX_PAGE_SIZE=1000
PAGINATION_DEFAULT_PER_PAGE=20
PAGINATION_MAX_PER_PAGE=100
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
poetry run pytest

# Run with coverage
poetry run pytest --cov=app --cov-report=html

# Run specific test file
poetry run pytest tests/test_subjects.py
```

**Test Structure:**
```
tests/
├── __init__.py
├── unit/
└── integration/
```

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

Response includes RFC 5988 compliant Link header:
```
Link: <http://localhost:8000/api/v1/subject?page=1&per_page=20>; rel="prev",
      <http://localhost:8000/api/v1/subject?page=3&per_page=20>; rel="next"
```

## Error Handling

The API returns structured error responses matching the OpenAPI specification:

```json
{
  "errors": [
    {
      "kind": "InvalidParameters",
      "message": "Invalid value for parameter 'page': Must be a positive integer",  
      "parameters": ["page"],
      "reason": "Unable to calculate offset"
    }
  ]
}
```

**Error Types:**
- `InvalidParameters` (422) - Invalid query/path parameters
- `UnsupportedField` (422) - Field not available for filtering/counting  
- `NotFound` (404) - Entity not found by identifier
- `UnshareableData` (404) - Data sharing restrictions
- `InternalServerError` (500) - Server-side errors

**Custom Exception Classes:**
- `CCDIException` - Base exception with HTTP mapping
- `InvalidParametersError` - Parameter validation failures
- `UnsupportedFieldError` - Field allowlist violations
- `NotFoundError` - Resource not found  
- `UnshareableDataError` - Data sharing policy violations

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

### Caching Metrics

Cache operations are logged for monitoring:
- Cache hits/misses
- Cache set/delete operations  
- Redis connection health
- TTL expiration tracking

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
     -e CACHE_REDIS_HOST=your-redis \
     ccdi-federation-service
   ```

### Docker Compose (Development)

The included `docker-compose.yml` sets up:
- FastAPI application
- Memgraph database  
- Redis cache
- Development environment configuration

### Kubernetes

Example deployment configuration available in `k8s/` directory (if present).

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions and support:

- Create an issue in the repository
- Contact the CCDI team
- Check the API documentation at `/docs`
