# CCDI Federation Service API Documentation

This directory contains the API documentation for the CCDI Federation Service.

## 📖 Live Documentation

The interactive API documentation is available at:
- **GitHub Pages**: `https://cbiit.github.io/ccdi-federation-service/`
- **Local Development**: `http://localhost:8000/docs` (when running the service)

## 🚀 Quick Start

### Single Subject Search
```bash
curl "https://your-server.com/api/v1/subject/CCDI-DCC/CCDI-DCC/TARGET-10-PAKKMW"
```

### Multiple Subjects Search
```bash
curl "https://your-server.com/api/v1/subject/CCDI-DCC/CCDI-DCC/TARGET-10-PAKKMW,TARGET-20-PAWUUE"
```

### Advanced Search with Pagination
```bash
curl "https://your-server.com/api/v1/subject/search/CCDI-DCC/CCDI-DCC/TARGET-10-PAKKMW?page=1&per_page=10"
```

## 📋 API Endpoints

### Subject Endpoints
- `GET /api/v1/subject` - List all subjects with pagination
- `GET /api/v1/subject/{organization}/{namespace}/{name}` - Get subject by identifier
- `GET /api/v1/subject/search/{organization}/{namespace}/{name}` - Search subjects by participant IDs
- `GET /api/v1/subject/summary` - Get subject summary statistics
- `GET /api/v1/subject/by/{field}/count` - Count subjects by field

### Sample Endpoints
- `GET /api/v1/sample` - List all samples
- `GET /api/v1/sample/{organization}/{namespace}/{name}` - Get sample by identifier
- `GET /api/v1/sample/summary` - Get sample summary statistics

### File Endpoints
- `GET /api/v1/file` - List all files
- `GET /api/v1/file/{organization}/{namespace}/{name}` - Get file by identifier
- `GET /api/v1/file/summary` - Get file summary statistics

## 🔧 Configuration

### Environment Variables
- `BASE_URL`: Base URL for the API (default: `http://localhost:8000`)
- `NAMESPACE_PREFIX`: Prefix for namespace names (default: `dbGaP_`)

### Authentication
Currently, the API does not require authentication. Rate limiting is applied to prevent abuse.

## 📊 Response Formats

### Single Subject Response
```json
{
  "id": {
    "namespace": {
      "organization": "CCDI-DCC",
      "name": "dbGaP_phs000465"
    },
    "name": "TARGET-10-PAKKMW"
  },
  "kind": "Participant",
  "metadata": {
    "sex": {"value": "Female", "ancestors": null},
    "race": [{"value": "White", "ancestors": null}],
    "ethnicity": {"value": "Not reported", "ancestors": null},
    "identifiers": [...],
    "associated_diagnoses": [...],
    "vital_status": {"value": "Alive", "ancestors": null},
    "age_at_vital_status": {"value": 15, "ancestors": null},
    "depositions": ["db_gap"]
  },
  "gateways": []
}
```

### Multiple Subjects Response
```json
{
  "source": "CCDI-DCC",
  "summary": {
    "counts": {
      "all": 104496,
      "current": 2
    }
  },
  "data": [
    { /* Subject 1 */ },
    { /* Subject 2 */ }
  ],
  "pagination": {
    "page": 1,
    "per_page": 100,
    "total_pages": 1045,
    "total_items": 104496,
    "has_next": true,
    "has_prev": false
  }
}
```

## 🛠️ Development

### Local Development
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Start the server: `uvicorn app.main:app --reload`
4. Access documentation: `http://localhost:8000/docs`

### Generate Documentation
```bash
# Generate OpenAPI spec
python -c "from app.main import app; import json; print(json.dumps(app.openapi(), indent=2))" > docs/openapi.json
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Update documentation if needed
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Support

For questions or support, please:
1. Check the [Issues](https://github.com/your-username/ccdi-federation-service/issues) page
2. Create a new issue if your question isn't answered
3. Contact the development team

## 🔗 Links

- **Repository**: https://github.com/your-username/ccdi-federation-service
- **Live API**: https://your-server.com
- **Documentation**: https://cbiit.github.io/ccdi-federation-service/
