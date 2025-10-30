---
layout: default
title: CCDI Federation Service API
description: REST API for querying CCDI-DCC  graph database
---

# Welcome to CCDI Federation Service API

The CCDI Federation Service provides a REST API for querying the Childhood Cancer Data Initiative (CCDI) graph database. This API allows you to search and retrieve information about subjects, samples, and files.

## 🚀 Getting Started

### Base URL
```
https://your-server.com/api/v1
```

### Authentication
Currently, the API does not require authentication. Rate limiting is applied to prevent abuse.

## 📋 Key Features

- **Subject Search**: Find participants by ID with support for multiple IDs
- **Sample Management**: Access sample data and metadata
- **File Operations**: Retrieve file information and metadata
- **Pagination**: Efficient handling of large datasets
- **Filtering**: Advanced filtering capabilities
- **Summary Statistics**: Get counts and summaries of data

## 🔧 API Endpoints

### Subject Endpoints
- `GET /subject` - List all subjects with pagination
- `GET /subject/{org}/{ns}/{name}` - Get subject by identifier
- `GET /subject/search/{org}/{ns}/{name}` - Search subjects by participant IDs
- `GET /subject/summary` - Get subject summary statistics

### Sample Endpoints
- `GET /sample` - List all samples
- `GET /sample/{org}/{ns}/{name}` - Get sample by identifier
- `GET /sample/summary` - Get sample summary statistics

### File Endpoints
- `GET /file` - List all files
- `GET /file/{org}/{ns}/{name}` - Get file by identifier
- `GET /file/summary` - Get file summary statistics

## 📖 Documentation

- **[Interactive API Documentation](index.html)** - Explore the API with Swagger UI
- **[OpenAPI Specification (YAML)](swagger.yml)** - Complete API specification
- **[OpenAPI Specification (JSON)](openapi.json)** - Complete API specification in JSON format

## 🛠️ Development

### Local Development
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Start the server: `uvicorn app.main:app --reload`
4. Access documentation: `http://localhost:8000/docs`

### Generate Documentation
```bash
python generate_docs.py
```

## 📄 Response Format

The API returns data in a structured JSON format with the following key components:

- **Single Subject**: Returns a `Subject` object with complete metadata
- **Multiple Subjects**: Returns a `SubjectResponse` with pagination and summary information
- **Error Responses**: Standard HTTP status codes with descriptive error messages

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Update documentation if needed
5. Submit a pull request

## 📞 Support

For questions or support, please:
1. Check the [Issues](https://github.com/CBIIT/ccdi-dcc-federation-service/issues) page
2. Create a new issue if your question isn't answered
3. Contact the development team

## 🔗 Links

- **Repository**: https://github.com/CBIIT/ccdi-dcc-federation-service
- **Live API**: https://your-server.com
- **Documentation**: https://cbiit.github.io/ccdi-dcc-federation-service/
